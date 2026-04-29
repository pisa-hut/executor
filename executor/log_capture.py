"""In-memory capture of everything the executor prints during a task run.

Both loguru (what the executor itself logs) and stdlib `logging` (what
simcore + grpc + third-party libs log) tee into the same bounded buffer.
When the task finishes, ``snapshot()`` returns the last `max_bytes` of
combined output so we can PUT it onto the task_run row and render it in
the web UI.
"""

from __future__ import annotations

import io
import logging
import threading
from loguru import logger as loguru_logger


class LogCapture:
    """Rotating in-memory buffer PLUS an unsent-chunk queue for streaming.

    `snapshot()` returns a trimmed tail for the final PUT on task end.
    `drain_queued()` returns everything that was written since the last
    drain, so the streamer can forward only new bytes to the manager."""

    def __init__(self, max_bytes: int = 512 * 1024):
        self._buf = io.StringIO()
        self._max = max_bytes
        self._queue: list[str] = []
        self._lock = threading.Lock()

    def write(self, msg: str) -> None:
        with self._lock:
            self._buf.write(msg)
            self._queue.append(msg)
            if self._buf.tell() > self._max * 2:
                text = self._buf.getvalue()[-self._max :]
                self._buf = io.StringIO()
                self._buf.write(text)

    def snapshot(self) -> str:
        with self._lock:
            text = self._buf.getvalue()
        if len(text) <= self._max:
            return text
        return f"... (truncated, keeping last {self._max} bytes)\n{text[-self._max :]}"

    def drain_queued(self) -> str:
        with self._lock:
            if not self._queue:
                return ""
            text = "".join(self._queue)
            self._queue.clear()
            return text


class _StdlibToCapture(logging.Handler):
    def __init__(self, capture: LogCapture):
        super().__init__(level=logging.DEBUG)
        self._capture = capture
        self.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s: %(message)s",
                datefmt="%H:%M:%S",
            )
        )

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._capture.write(self.format(record) + "\n")
        except Exception:
            pass


# Libraries whose DEBUG output is too noisy to capture. urllib3 in
# particular floods for every HTTP call; since the log streamer itself
# makes HTTP calls, leaving it at DEBUG creates a compounding mess in
# the captured log (chunks containing logs of the chunk-upload requests).
_NOISY_LIBS = (
    "urllib3",
    "urllib3.connectionpool",
    "requests",
    "charset_normalizer",
    "asyncio",
)


def install(capture: LogCapture) -> None:
    """Tee loguru + stdlib logging into `capture`. Stdlib root logger is
    left at whatever level the caller configured; we only append a handler.
    Loguru sink is added at DEBUG so we capture everything.

    Noisy low-level libs are pinned to WARNING so they don't dominate the
    captured stream — their DEBUG output isn't useful for task debugging
    and would consume most of the buffer."""

    loguru_logger.add(
        capture.write,
        level="DEBUG",
        format="{time:HH:mm:ss} | {level: <8} | {name}:{line} - {message}\n",
    )

    root = logging.getLogger()
    # Ensure the root logger lets records through to our handler even if
    # no one configured a level yet.
    if root.level > logging.DEBUG or root.level == logging.NOTSET:
        root.setLevel(logging.DEBUG)
    root.addHandler(_StdlibToCapture(capture))

    for name in _NOISY_LIBS:
        logging.getLogger(name).setLevel(logging.WARNING)

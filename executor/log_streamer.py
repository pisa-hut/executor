"""Background thread that drains a `LogCapture` queue and PUTs each batch
to `POST /task_run/{id}/log/append` on the manager. The manager appends
to `task_run.log` and broadcasts a `log` SSE envelope so the web UI can
stream chunks into the Log Drawer in real time.

Failures are swallowed (logged at debug level) — the next tick retries;
if streaming is permanently broken, the final `snapshot()` still lands
via the lifecycle call on task completion as a safety net."""

from __future__ import annotations

import threading

import requests
from loguru import logger

from executor.log_capture import LogCapture


class LogStreamer:
    def __init__(
        self,
        capture: LogCapture,
        manager_url: str,
        task_run_id: int,
        interval_s: float = 1.0,
        timeout_s: int = 10,
    ) -> None:
        self._capture = capture
        self._url = f"{manager_url}/task_run/{task_run_id}/log/append"
        self._interval = interval_s
        self._timeout = timeout_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="log-streamer", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
        # Final flush so anything produced between the last tick and stop()
        # still reaches the manager.
        self._flush_once()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._flush_once()
            self._stop.wait(self._interval)

    def _flush_once(self) -> None:
        chunk = self._capture.drain_queued()
        if not chunk:
            return
        try:
            r = requests.post(
                self._url,
                data=chunk.encode("utf-8"),
                headers={"Content-Type": "application/octet-stream"},
                timeout=self._timeout,
            )
            r.raise_for_status()
        except Exception as exc:
            # Keep going — next tick will try again with accumulated output.
            logger.debug(f"log stream flush to {self._url} failed: {exc}")

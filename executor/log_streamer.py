"""Background thread that drains a `LogCapture` queue and POSTs each batch
to `POST /task_run/{id}/log/append` on the manager. The manager appends
to `task_run.log` and broadcasts a `log` SSE envelope so the web UI can
stream chunks into the Log Drawer in real time.

Transient failures are swallowed (logged at debug level) — the next tick
retries. A 410 Gone response means the task_run has been finalised on
the manager (e.g. the user hit Stop in the web UI); we self-SIGTERM so
the main thread's shutdown handler aborts cleanly.  This SIGTERM is only
raised from the background thread; the final flush in ``stop()`` treats
410 as a no-op to avoid duplicate shutdown signals."""

from __future__ import annotations

import os
import signal
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
        # Set once we've raised the abort signal so we don't keep flushing
        # (and re-tripping the 410 path) during the brief window before the
        # main thread exits.
        self._aborted = False

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
        # still reaches the manager.  Pass allow_sigterm=False so a 410
        # response here (shutdown already in progress) doesn't trigger a
        # second SIGTERM and duplicate lifecycle reports.
        self._flush_once(allow_sigterm=False)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._flush_once()
            self._stop.wait(self._interval)

    def _flush_once(self, *, allow_sigterm: bool = True) -> None:
        if self._aborted:
            return
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
            if r.status_code == 410:
                self._aborted = True
                self._stop.set()
                if allow_sigterm:
                    logger.warning(
                        "Manager rejected log append (410 Gone) — task was stopped; "
                        "raising SIGTERM so the executor exits cleanly."
                    )
                    # Python signal handlers run on the main thread; sending
                    # ourselves SIGTERM lets the existing shutdown handler do the
                    # `task_aborted` round-trip, stop containers, and sys.exit.
                    os.kill(os.getpid(), signal.SIGTERM)
                else:
                    logger.debug(
                        "Manager returned 410 Gone during final log flush; "
                        "shutdown already in progress, skipping SIGTERM."
                    )
                return
            r.raise_for_status()
        except Exception as exc:
            # Keep going — next tick will try again with accumulated output.
            logger.debug(f"log stream flush to {self._url} failed: {exc}")

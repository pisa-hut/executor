import subprocess
import sys
import threading
from typing import Any, Optional

from loguru import logger

from executor.apptainer_utils.apptainer_config import ApptainerServiceConfig
from executor.service_manager import ServiceManager, WrapperLogBuffer


class ApptainerServiceManager(ServiceManager):
    """Start/stop Apptainer services for simulator and AV.

    Uses foreground `apptainer run` so the container is a child of
    this executor process, which itself sits inside the SLURM job
    step's cgroup. Cleanup is whatever the OS / SLURM does to the
    cgroup plus a `terminate → wait → kill` on the apptainer parent
    (apptainer forwards signals to the container's PID 1 and
    destroys the user namespace on exit, so the container processes
    go down with it). No session/process-group plumbing needed.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Track the live subprocess for each service so we can stop
        # it later. Reader threads drain the merged stdout/stderr
        # pipe so the kernel buffer can't fill and stall the
        # container; they exit naturally when the pipe hits EOF on
        # process death.
        self._processes: dict[str, subprocess.Popen[str]] = {}
        self._readers: dict[str, threading.Thread] = {}

    def _start_backend_service(
        self,
        component_kind: str,
        component_name: str,
        component_spec: dict[str, Any],
        runtime_envs: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        config = ApptainerServiceConfig.from_component_spec(component_spec)
        if config is None:
            logger.error(f"Invalid task spec for {component_kind}: {component_name}")
            return None

        start_envs: dict[str, Any] = dict(config.extra_envs)
        start_envs.update(runtime_envs)

        allocated_port = int(runtime_envs["PORT"])
        service_name = f"{component_name}-{self.id}-{allocated_port}"

        try:
            command = config.get_run_command(start_envs)
            logger.debug(f"Spawning: {' '.join(command)}")
            # encoding="utf-8", errors="replace" keep the reader
            # thread robust against non-UTF8 bytes some simulators
            # emit; default decoder behaviour would crash the
            # thread, leave the pipe undrained, and eventually
            # stall the container once the kernel buffer fills.
            proc = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            self._processes[service_name] = proc
            reader = threading.Thread(
                target=self._stream_output,
                args=(service_name, proc, self.wrapper_logs),
                daemon=True,
            )
            reader.start()
            self._readers[service_name] = reader

            if not self._wait_for_service_start(allocated_port):
                rc = proc.poll()
                if rc is not None:
                    logger.error(
                        f"Container exited before service was ready "
                        f"(rc={rc}): {service_name}"
                    )
                else:
                    logger.error(
                        f"Service did not become ready in time: {service_name}"
                    )
                self._stop_backend_service(service_name)
                return None

            service_url = f"localhost:{allocated_port}"
            logger.info(f"{component_name} service available at: {service_url}")

            self._register_started_service(
                service_name=service_name,
                runtime_envs=runtime_envs,
            )

            return {
                "url": service_url,
                "service_name": service_name,
            }
        except Exception as exc:
            logger.exception(f"Failed to start Apptainer service: {exc}")
            self._stop_backend_service(service_name)
            return None

    def _stop_backend_service(self, service_name: str) -> None:
        # Look up but DO NOT pop yet — if the process survives
        # SIGKILL we want the handle to stay around so a follow-up
        # cleanup attempt can retry instead of silently no-op'ing.
        proc = self._processes.get(service_name)
        if proc is None:
            return

        logger.info(f"Stopping Apptainer container: {service_name} (pid {proc.pid})")
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                logger.warning(
                    f"Container {service_name} did not exit on SIGTERM; killing"
                )
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.error(
                        f"Container {service_name} survived SIGKILL; leaking — "
                        f"keeping handle for retry"
                    )
                    return

        # Confirmed exit — drop tracking and join the reader (its
        # pipe hits EOF as the process is reaped).
        self._processes.pop(service_name, None)
        reader = self._readers.pop(service_name, None)
        if reader is not None:
            reader.join(timeout=5)

    @staticmethod
    def _stream_output(
        service_name: str,
        proc: subprocess.Popen[str],
        wrapper_logs: "WrapperLogBuffer",
    ) -> None:
        # Write container output straight to stdout with just a
        # `[<service>]` prefix — the container (simcore wrapper)
        # already prints its own timestamp/level, and routing
        # through loguru.info would prepend a second one. The
        # executor's loguru default sink also writes to stdout
        # (main.py: logger.add(sink=sys.stdout, ...)), so
        # container lines and executor-native lines interleave on
        # the same local stream in chronological order.
        #
        # Bypassing loguru means container output does NOT enter
        # LogCapture and therefore does NOT stream live to the
        # manager / Log Drawer. We tee each line into a bounded
        # `wrapper_logs` buffer so the executor can attach a tail
        # to the final lifecycle POST — gives the web UI the "what
        # did the wrapper say when it crashed?" view without the
        # firehose noise the user explicitly rejected.
        stdout = proc.stdout
        if stdout is None:
            return
        prefix = f"[{service_name}] "
        try:
            for line in stdout:
                sys.stdout.write(
                    prefix + line if line.endswith("\n") else prefix + line + "\n"
                )
                sys.stdout.flush()
                wrapper_logs.append(service_name, line)
        except Exception as exc:
            logger.warning(f"Output reader for {service_name} stopped: {exc}")

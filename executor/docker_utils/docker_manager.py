import subprocess
from typing import Any, Optional

from loguru import logger

from executor.docker_utils.docker_config import DockerServiceConfig
from executor.service_manager import ServiceManager


# Lines of `docker logs --tail` to keep per service. Same order of
# magnitude as the apptainer reader's ring (300) so the snapshot looks
# similar across backends.
_DOCKER_LOG_TAIL_LINES = 300


class DockerServiceManager(ServiceManager):
    """Start/stop Docker services for simulator and AV.

    Containers run detached + `--rm`, so the executor has no live
    stdout handle and the container is removed the moment it stops.
    To preserve a wrapper-output tail for the final lifecycle POST,
    we shell out to `docker logs --tail N <name>` inside
    `_stop_backend_service` BEFORE issuing `docker stop`, feed the
    lines into the shared `wrapper_logs` buffer, and let the base
    class's `snapshot_wrapper_outputs()` do the rest. Doing this at
    snapshot time wouldn't work — by then the container is gone.
    """

    def _capture_container_logs(self, service_name: str) -> None:
        try:
            out = subprocess.run(
                ["docker", "logs", "--tail", str(_DOCKER_LOG_TAIL_LINES), service_name],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except Exception as exc:
            logger.warning(f"docker logs for {service_name} failed: {exc}")
            return
        # docker writes container stdout to stdout and stderr to
        # stderr; merge into the buffer in arrival order isn't
        # possible without --details, so append stdout block first
        # then stderr block — same order users see on `docker logs`.
        for stream in (out.stdout, out.stderr):
            if not stream:
                continue
            for line in stream.splitlines():
                self.wrapper_logs.append(service_name, line)

    def _start_backend_service(
        self,
        component_kind: str,
        component_name: str,
        component_spec: dict[str, Any],
        runtime_envs: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        config = DockerServiceConfig.from_component_spec(component_spec)
        if config is None:
            logger.error(f"Invalid task spec for {component_kind}: {component_name}")
            return None

        start_envs: dict[str, Any] = dict(config.extra_envs)
        start_envs.update(runtime_envs)

        allocated_port = int(runtime_envs["PORT"])
        service_name = f"{component_name}-{self.id}-{allocated_port}"

        try:
            command = config.get_start_command(service_name, start_envs, allocated_port)
            logger.debug(f"Running command: {' '.join(command)}")
            proc = self._run_command(command)
            if proc.returncode != 0:
                logger.error(f"Failed to start Docker container: {proc.stderr}")
                return None

            if not self._wait_for_service_start(allocated_port):
                logger.error(f"Service failed to start: {service_name}")
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
            logger.exception(f"Failed to start Docker service: {exc}")
            self._stop_backend_service(service_name)
            return None

    def _stop_backend_service(self, service_name: str) -> None:
        # Grab logs BEFORE issuing `docker stop` — the container was
        # started with `--rm`, so it gets removed on stop and a later
        # `docker logs` would 404.
        self._capture_container_logs(service_name)
        command = DockerServiceConfig.get_stop_command(service_name)
        logger.info(f"Stopping Docker container: {service_name}")
        try:
            proc = self._run_command(command)
            if proc.returncode != 0:
                logger.error(f"Failed to stop Docker container: {proc.stderr}")
        except Exception as exc:
            logger.error(f"Failed to stop Docker container {service_name}: {exc}")

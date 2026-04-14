from typing import Any, Optional

from loguru import logger

from executor.docker_utils.docker_config import DockerServiceConfig
from executor.service_manager import ServiceManager


class DockerServiceManager(ServiceManager):
    """Start/stop Docker services for simulator and AV."""

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
                component_kind=component_kind,
                component_name=component_name,
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
        command = DockerServiceConfig.get_stop_command(service_name)
        logger.info(f"Stopping Docker container: {service_name}")
        try:
            proc = self._run_command(command)
            if proc.returncode != 0:
                logger.error(f"Failed to stop Docker container: {proc.stderr}")
        except Exception as exc:
            logger.error(f"Failed to stop Docker container {service_name}: {exc}")

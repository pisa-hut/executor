from typing import Any, Optional

from loguru import logger

from executor.apptainer_utils.apptainer_config import ApptainerServiceConfig
from executor.service_manager import ServiceManager


class ApptainerServiceManager(ServiceManager):
    """Start/stop Apptainer services for simulator and AV."""

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
            command = config.get_start_command(service_name, start_envs)
            logger.debug(f"Running command: {' '.join(command)}")
            proc = self._run_command(command)
            if proc.returncode != 0:
                logger.error(f"Failed to start Apptainer instance: {proc.stderr}")
                return None

            if not self._wait_for_service_start(allocated_port):
                logger.error(f"Service failed to start: {service_name}")
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
            logger.exception(f"Failed to start Apptainer service: {exc}")
            return None

    def _stop_backend_service(self, service_name: str) -> None:
        command = ApptainerServiceConfig.get_stop_command(service_name)
        logger.info(f"Stopping Apptainer instance: {service_name}")
        try:
            proc = self._run_command(command)
            if proc.returncode != 0:
                logger.error(f"Failed to stop Apptainer instance: {proc.stderr}")
        except Exception as exc:
            logger.error(f"Failed to stop Apptainer instance {service_name}: {exc}")

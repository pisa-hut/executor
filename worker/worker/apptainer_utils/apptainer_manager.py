import socket
import subprocess
import time
import logging
from pathlib import Path
from typing import Any, Optional

from worker.apptainer_utils.apptainer_config import ApptainerServiceConfig

logger = logging.getLogger(__name__)


def find_free_port(start_port: int = 8000, max_attempts: int = 100) -> Optional[int]:
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.bind(("", port))
                return port
        except OSError:
            continue
    return None


class ApptainerServiceManager:
    """Start/stop Apptainer services for simulator and av."""

    MAP_CONTAINER_PATHS = {
        "xodr_path": "/mnt/map/xodr",
        "osm_path": "/mnt/map/osm",
    }
    SCENARIO_CONTAINER_PATH = "/mnt/scenario"
    OUTPUT_CONTAINER_PATH = "/mnt/output"

    def __init__(self):
        self.running_instances: dict[str, dict[str, int]] = {}
        self.component_to_instance: dict[str, str] = {}

    @staticmethod
    def _run_command(
        command: list[str], timeout: int = 10
    ) -> subprocess.CompletedProcess:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def _allocate_ports(
        self, config: ApptainerServiceConfig
    ) -> Optional[dict[str, int]]:
        allocated_port = find_free_port(start_port=config.preferred_port)
        if allocated_port is None:
            return None

        allocated_ports = {"PORT": allocated_port}
        for env_var, preferred_start in config.extra_ports.items():
            extra_port = find_free_port(start_port=preferred_start)
            if extra_port is None:
                logger.error("Failed to find a free port for %s", env_var)
                return None
            allocated_ports[env_var] = extra_port
            logger.info("Allocated %s: %s", env_var, extra_port)

        return allocated_ports

    @staticmethod
    def _require_existing_path_from_spec(
        spec: dict[str, Any],
        key: str,
    ) -> str:
        raw_path: Any = spec.get(key)

        if raw_path is None:
            available_keys = ", ".join(sorted(spec.keys())) or "<none>"
            raise ValueError(
                "Missing required host path. "
                f"Expected key '{key}'. Available keys: {available_keys}"
            )

        resolved_path = Path(str(raw_path)).resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(
                f"Host path for key '{key}' does not exist: {resolved_path}"
            )
        if not resolved_path.is_dir():
            logger.warning(
                "Host path for key '%s' is not a directory: %s", key, resolved_path
            )

        return str(resolved_path)

    def _start_one_service(
        self,
        component_kind: str,
        component_spec: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        component_name = str(component_spec.get("name") or component_kind)
        config = ApptainerServiceConfig.from_component_spec(component_spec)
        if config is None:
            logger.error("Invalid task spec for %s: %s", component_kind, component_name)
            return None

        allocated_ports = self._allocate_ports(config)
        if allocated_ports is None:
            logger.error(
                "Failed to find a free port for %s: %s",
                component_kind,
                component_name,
            )
            return None

        allocated_port = allocated_ports["PORT"]
        service_name = f"{component_name}-{allocated_port}"

        try:
            command = config.get_start_command(service_name, allocated_ports)
            logger.info("Running command: %s", " ".join(command))
            proc = self._run_command(command)
            if proc.returncode != 0:
                logger.error("Failed to start Apptainer instance: %s", proc.stderr)
                return None

            time.sleep(config.startup_wait)

            service_url = f"http://localhost:{allocated_port}"
            logger.info("%s service available at: %s", component_kind, service_url)

            self.running_instances[service_name] = allocated_ports
            self.component_to_instance[f"{component_kind}:{component_name}"] = (
                service_name
            )

            return {
                "url": service_url,
                "service_name": service_name,
            }
        except Exception as exc:
            logger.exception("Failed to start Apptainer service: %s", exc)
            return None

    def start(
        self,
        services_spec: dict[str, Any],
        output_dir: str,
    ) -> dict[str, dict[str, Any]]:
        simulator_spec = dict(services_spec.get("simulator", {}))
        av_spec = dict(services_spec.get("av", {}))
        map_spec = dict(services_spec.get("map", {}))
        scenario_spec = dict(services_spec.get("scenario", {}))

        xodr_host = self._require_existing_path_from_spec(
            spec=map_spec,
            key="xodr_path",
        )

        osm_host = self._require_existing_path_from_spec(
            spec=map_spec,
            key="osm_path",
        )

        scenario_host = self._require_existing_path_from_spec(
            spec=scenario_spec,
            key="scenairo_path",
        )

        output_host = str(Path(output_dir).resolve())
        Path(output_host).mkdir(parents=True, exist_ok=True)

        shared_bind_mounts: list[tuple[str, str]] = [
            (xodr_host, self.MAP_CONTAINER_PATHS["xodr_path"]),
            (osm_host, self.MAP_CONTAINER_PATHS["osm_path"]),
            (scenario_host, self.SCENARIO_CONTAINER_PATH),
            (output_host, self.OUTPUT_CONTAINER_PATH),
        ]

        simulator_bind_mounts = (
            list(simulator_spec.get("bind_mounts", [])) + shared_bind_mounts
        )
        av_bind_mounts = list(av_spec.get("bind_mounts", [])) + shared_bind_mounts

        simulator_service_config = dict(simulator_spec)
        simulator_service_config["bind_mounts"] = simulator_bind_mounts
        av_service_config = dict(av_spec)
        av_service_config["bind_mounts"] = av_bind_mounts

        simulator_service_info = self._start_one_service(
            "simulator", simulator_service_config
        )
        av_service_info = self._start_one_service("av", av_service_config)

        base_started_spec = {
            "map": {
                "xodr_path": self.MAP_CONTAINER_PATHS["xodr_path"],
                "osm_path": self.MAP_CONTAINER_PATHS["osm_path"],
            },
            "scenario_path": self.SCENARIO_CONTAINER_PATH,
            "output_path": self.OUTPUT_CONTAINER_PATH,
        }

        started_specs: dict[str, dict[str, Any]] = {
            "simulator": dict(base_started_spec),
            "av": dict(base_started_spec),
        }

        if simulator_service_info is not None:
            started_specs["simulator"]["service_info"] = {
                "url": simulator_service_info["url"],
            }

        if av_service_info is not None:
            started_specs["av"]["service_info"] = {
                "url": av_service_info["url"],
            }

        return started_specs

    def stop_all_services(self):
        for service_name in list(self.running_instances.keys()):
            command = ApptainerServiceConfig.get_stop_command(service_name)
            logger.info("Stopping Apptainer instance: %s", service_name)
            try:
                proc = self._run_command(command)
                if proc.returncode != 0:
                    logger.error("Failed to stop Apptainer instance: %s", proc.stderr)
            except Exception as exc:
                logger.exception(
                    "Failed to stop Apptainer instance %s: %s", service_name, exc
                )

        self.running_instances.clear()
        self.component_to_instance.clear()

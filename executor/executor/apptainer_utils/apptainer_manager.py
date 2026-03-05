import logging
from pathlib import Path
import random
import socket
import subprocess
import time
from typing import Any, Optional

from executor.utils import resolve_host_path
from executor.apptainer_utils.apptainer_config import ApptainerServiceConfig

logger = logging.getLogger(__name__)


def find_free_port(start_port: int = 8000, max_attempts: int = 100) -> Optional[int]:
    for _ in range(max_attempts):
        port = random.randint(start_port, start_port + 2000)
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

    def __init__(self, id: str):
        self.id = id
        self.running_instances: dict[str, dict[str, int]] = {}
        self.component_to_instance: dict[str, str] = {}

    def _resolve_ros_domain_id(self) -> int:
        try:
            return int(self.id[-2:])
        except ValueError:
            return abs(hash(self.id)) % 232

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

    def _allocate_runtime_envs(
        self, component_spec: dict[str, Any]
    ) -> Optional[dict[str, int]]:
        service_port = find_free_port()
        if service_port is None:
            return None

        runtime_envs: dict[str, int] = {"PORT": service_port}
        if bool(component_spec.get("carla_runtime", False)):
            carla_port = None
            for _ in range(5):
                candidate_port = find_free_port()
                if candidate_port is not None and candidate_port != service_port:
                    carla_port = candidate_port
                    break
            if carla_port is None:
                return None
            runtime_envs["CARLA_PORT"] = carla_port

        if bool(component_spec.get("ros_runtime", False)):
            runtime_envs["ROS_DOMAIN_ID"] = self._resolve_ros_domain_id()

        return runtime_envs

    @staticmethod
    def _require_existing_path_from_spec(
        spec: dict[str, Any],
        key: str,
    ) -> str:
        if key not in spec:
            raise KeyError(f"Missing required key '{key}' in component spec")

        resolved_path = Path(resolve_host_path(spec.get(key)))
        if not resolved_path.exists():
            raise FileNotFoundError(
                f"Host path for key '{key}' does not exist: {resolved_path}"
            )
        if not resolved_path.is_dir():
            logger.warning(
                "Host path for key '%s' is not a directory: %s", key, resolved_path
            )

        return str(resolved_path)

    def _wait_for_service_start(self, port: int, timeout: int = 30) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                result = sock.connect_ex(("localhost", port))
                if result == 0:
                    logger.info("Service on port %s is up", port)
                    return True
            time.sleep(1)
        logger.error(
            "Service on port %s did not start within %s seconds", port, timeout
        )
        return False

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

        runtime_envs = self._allocate_runtime_envs(component_spec)
        if runtime_envs is None:
            logger.error(
                "Failed to find a free port for %s: %s",
                component_kind,
                component_name,
            )
            return None

        start_envs: dict[str, Any] = dict(config.extra_envs)
        start_envs.update(runtime_envs)

        allocated_port = runtime_envs["PORT"]
        service_name = f"{component_name}-{self.id}-{allocated_port}"

        try:
            command = config.get_start_command(service_name, start_envs)
            logger.info("Running command: %s", " ".join(command))
            proc = self._run_command(command)
            if proc.returncode != 0:
                logger.error("Failed to start Apptainer instance: %s", proc.stderr)
                return None

            self._wait_for_service_start(allocated_port)

            service_url = f"localhost:{allocated_port}"
            logger.info("%s service available at: %s", component_kind, service_url)

            self.running_instances[service_name] = runtime_envs
            self.component_to_instance[f"{component_kind}:{component_name}"] = (
                service_name
            )

            return {
                "url": service_url,
                "service_name": service_name,
            }
        except Exception as exc:
            logger.exception("Failed to start Apptainer service: %s", exc)

    def start(
        self,
        services_spec: dict[str, Any],
        output_dir: str,
    ) -> dict[str, dict[str, Any]]:
        av_spec = dict(services_spec.get("av", {}))
        simulator_spec = dict(services_spec.get("simulator", {}))
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
            key="scenario_path",
        )

        output_host = str(Path(output_dir).resolve())
        Path(output_host).mkdir(parents=True, exist_ok=True)

        shared_bind_mounts: list[tuple[str, str]] = [
            (xodr_host, self.MAP_CONTAINER_PATHS["xodr_path"]),
            (osm_host, self.MAP_CONTAINER_PATHS["osm_path"]),
            (scenario_host, self.SCENARIO_CONTAINER_PATH),
            (output_host, self.OUTPUT_CONTAINER_PATH),
        ]

        av_bind_mounts = list(av_spec.get("bind_mounts", [])) + shared_bind_mounts
        av_service_config = dict(av_spec)
        av_service_config["bind_mounts"] = av_bind_mounts
        av_service_info = self._start_one_service(
            "av",
            av_service_config,
        )

        simulator_bind_mounts = (
            list(simulator_spec.get("bind_mounts", [])) + shared_bind_mounts
        )
        simulator_service_config = dict(simulator_spec)
        simulator_service_config["bind_mounts"] = simulator_bind_mounts
        simulator_service_info = self._start_one_service(
            "simulator",
            simulator_service_config,
        )

        if simulator_service_info is None or av_service_info is None:
            logger.error("Failed to start required services. Stopping all services.")
            self.stop_all_services()
            raise RuntimeError("Failed to start required services.")

        base_started_spec = {
            "map": {
                "xodr_path": self.MAP_CONTAINER_PATHS["xodr_path"],
                "osm_path": self.MAP_CONTAINER_PATHS["osm_path"],
            },
            "scenario_path": self.SCENARIO_CONTAINER_PATH,
            "output_path": self.OUTPUT_CONTAINER_PATH,
        }

        started_specs: dict[str, dict[str, Any]] = {
            "av": {
                "service_info": {
                    "url": av_service_info.get("url") if av_service_info else None
                },
                **dict(base_started_spec),
            },
            "simulator": {
                "service_info": {
                    "url": (
                        simulator_service_info.get("url")
                        if simulator_service_info
                        else None
                    )
                },
                **dict(base_started_spec),
            },
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
                logger.error(
                    "Failed to stop Apptainer instance %s: %s", service_name, exc
                )

        self.running_instances.clear()
        self.component_to_instance.clear()

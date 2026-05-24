from abc import ABC, abstractmethod
from collections import deque
from pathlib import Path
import random
import re
import socket
import subprocess
import threading
import time
from typing import Any, Optional

from loguru import logger

# CSI/OSC escape sequences (ANSI colour, cursor moves, etc.). Stripped
# before the wrapper snapshot is sent to the manager so raw `\x1b[32m`
# bytes don't show up verbatim in the Log Drawer's `<pre>`.
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")


def strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


class WrapperLogBuffer:
    """Per-service bounded line ring buffer.

    Used by service-manager subclasses to capture the most recent N lines
    of each wrapper container's combined stdout/stderr, so the executor
    can attach the tail to the final lifecycle POST when a task ends.
    Live streaming still goes to local stdout via _stream_output; this
    buffer is read once on task termination, not polled.
    """

    DEFAULT_LINES = 300

    def __init__(self, max_lines_per_service: int = DEFAULT_LINES):
        self._max = max_lines_per_service
        self._lines: dict[str, deque[str]] = {}
        self._lock = threading.Lock()

    def append(self, service_name: str, line: str) -> None:
        with self._lock:
            buf = self._lines.get(service_name)
            if buf is None:
                buf = deque(maxlen=self._max)
                self._lines[service_name] = buf
            # Normalise trailing newline so snapshot output joins cleanly.
            buf.append(line.rstrip("\n"))

    def snapshot(self) -> str:
        """Return one block per service, separated by a header. Empty if
        no service produced any output. Strips ANSI on the way out so the
        manager's Log Drawer renders cleanly."""
        with self._lock:
            services = [(name, list(buf)) for name, buf in self._lines.items() if buf]
        if not services:
            return ""
        parts: list[str] = []
        for name, lines in services:
            parts.append(
                f"--- last {len(lines)} lines of {name} ---\n"
                + strip_ansi("\n".join(lines))
            )
        return "\n\n".join(parts)


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


class ServiceManager(ABC):
    """Shared lifecycle orchestration for containerized AV/simulator services."""

    MAP_CONTAINER_PATHS = {
        "xodr_path": "/mnt/map/xodr",
        "osm_path": "/mnt/map/osm",
    }
    SCENARIO_CONTAINER_PATH = "/mnt/scenario"
    OUTPUT_CONTAINER_PATH = "/mnt/output"

    def __init__(self, id: str):
        self.id = id
        self.running_instances: dict[str, dict[str, Any]] = {}
        self.component_to_instance: dict[str, str] = {}
        # Per-service bounded buffer of wrapper output. Populated by the
        # backend's per-line capture path (apptainer reader thread, or
        # docker `logs --tail` at snapshot time) so the manager only
        # learns about wrapper noise when the task ends, never live.
        self.wrapper_logs = WrapperLogBuffer()

    def snapshot_wrapper_outputs(self) -> str:
        """Tail of each running wrapper's stdout, joined into one block.
        Backends that don't keep a buffer should override this to fetch
        their logs at call time (e.g. `docker logs --tail N`)."""
        return self.wrapper_logs.snapshot()

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
            # CARLA needs two distinct ports per task: the simulator
            # RPC port (CARLA_PORT) and the TrafficManager RPC port
            # (CARLA_TM_PORT). Without a per-task TM port every CARLA
            # task tries to bind TM on the wrapper config's default
            # (e.g. 8000) and the second concurrent task gets
            # "bind error" from `client.get_trafficmanager(...)`.
            picked: list[int] = []
            for _ in range(10):
                if len(picked) == 2:
                    break
                candidate_port = find_free_port()
                if (
                    candidate_port is not None
                    and candidate_port != service_port
                    and candidate_port not in picked
                ):
                    picked.append(candidate_port)
            if len(picked) < 2:
                return None
            runtime_envs["CARLA_PORT"] = picked[0]
            runtime_envs["CARLA_TM_PORT"] = picked[1]

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

        resolved_path = Path(str(spec[key])).resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(
                f"Host path for key '{key}' does not exist: {resolved_path}"
            )
        if not resolved_path.is_dir():
            logger.warning(
                f"Host path for key '{key}' is not a directory: {resolved_path}"
            )

        return str(resolved_path)

    def _wait_for_service_start(self, port: int, timeout: int = 120) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                result = sock.connect_ex(("localhost", port))
                if result == 0:
                    logger.debug(f"Service on port {port} is up")
                    return True
            time.sleep(1)
        logger.error(f"Service on port {port} did not start within {timeout} seconds")
        return False

    def _register_started_service(
        self,
        component_kind: str,
        component_name: str,
        service_name: str,
        runtime_envs: dict[str, Any],
    ) -> None:
        self.running_instances[service_name] = runtime_envs
        self.component_to_instance[f"{component_kind}:{component_name}"] = service_name

    def _start_shared_service(
        self,
        component_kind: str,
        component_spec: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        component_name = str(component_spec.get("name") or component_kind)
        runtime_envs = self._allocate_runtime_envs(component_spec)
        if runtime_envs is None:
            logger.error(
                f"Failed to find a free port for {component_kind}: {component_name}"
            )
            return None

        return self._start_backend_service(
            component_kind=component_kind,
            component_name=component_name,
            component_spec=component_spec,
            runtime_envs=runtime_envs,
        )

    @abstractmethod
    def _start_backend_service(
        self,
        component_kind: str,
        component_name: str,
        component_spec: dict[str, Any],
        runtime_envs: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        """Start one backend-specific service and return its connection info."""

    @abstractmethod
    def _stop_backend_service(self, service_name: str) -> None:
        """Stop one backend-specific service."""

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

        av_service_config = dict(av_spec)
        av_service_config["bind_mounts"] = list(av_spec.get("bind_mounts", [])) + list(
            shared_bind_mounts
        )
        av_service_info = self._start_shared_service("av", av_service_config)

        simulator_service_config = dict(simulator_spec)
        simulator_service_config["bind_mounts"] = list(
            simulator_spec.get("bind_mounts", [])
        ) + list(shared_bind_mounts)
        simulator_service_info = self._start_shared_service(
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

        return {
            "av": {
                "service_info": {"url": av_service_info.get("url")},
                **dict(base_started_spec),
            },
            "simulator": {
                "service_info": {"url": simulator_service_info.get("url")},
                **dict(base_started_spec),
            },
        }

    def stop_all_services(self) -> None:
        for service_name in list(self.running_instances.keys()):
            self._stop_backend_service(service_name)
            self.running_instances.pop(service_name, None)
        self.component_to_instance.clear()

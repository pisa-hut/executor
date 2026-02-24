"""Apptainer service configuration primitives."""

from pathlib import Path
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)


class ApptainerServiceConfig:
    """Configuration for an Apptainer service."""

    def __init__(
        self,
        sif_path: str,
        startup_wait: float = 2.0,
        preferred_port: Optional[int] = None,
        extra_ports: Optional[dict[str, int]] = None,
        bind_mounts: Optional[list[tuple[str, str]]] = None,
        nv_runtime: bool = False,
    ):
        self.sif_path = sif_path
        self.startup_wait = startup_wait
        self.preferred_port = preferred_port or 8000
        self.extra_ports = extra_ports or {}
        self.bind_mounts = bind_mounts or []
        self.nv_runtime = nv_runtime

    @staticmethod
    def _resolve_sif_path(image_path: str) -> str:
        raw = Path(image_path)
        if raw.is_absolute() or raw.exists():
            return str(raw)

        dot_sifs = Path(".sifs") / image_path
        if dot_sifs.exists():
            return str(dot_sifs)

        sifs = Path("sifs") / image_path
        if sifs.exists():
            return str(sifs)

        return str(dot_sifs)

    @classmethod
    def from_component_spec(
        cls,
        component_spec: dict[str, Any],
    ) -> Optional["ApptainerServiceConfig"]:
        image_path = component_spec.get("image_path")
        preferred_port = component_spec.get("preferred_port", 8000)
        extra_ports = component_spec.get("extra_ports")
        bind_mounts = component_spec.get("bind_mounts")

        if image_path is None:
            print("Missing required field 'image_path' in component spec")
            return None

        if not isinstance(extra_ports, dict):
            extra_ports = {}

        if not isinstance(bind_mounts, list):
            bind_mounts = []

        normalized_bind_mounts = [
            (str(host), str(container)) for host, container in bind_mounts
        ]

        nv_runtime = bool(component_spec.get("nv_runtime", False))

        try:
            return cls(
                sif_path=cls._resolve_sif_path(str(image_path)),
                preferred_port=int(preferred_port),
                extra_ports={
                    str(key): int(value) for key, value in extra_ports.items()
                },
                bind_mounts=normalized_bind_mounts,
                nv_runtime=nv_runtime,
            )
        except (TypeError, ValueError):
            print("Invalid component spec types for Apptainer service config")
            return None

    def _append_runtime_flags(self, cmd: list[str]) -> None:
        for host_path, container_path in self.bind_mounts:
            cmd.extend(["--bind", f"{host_path}:{container_path}"])

        if self.nv_runtime:
            cmd.append("--nv")

    def get_start_command(
        self, instance_name: str, ports: dict[str, int], id: int
    ) -> list[str]:
        cmd = ["apptainer", "instance", "start", "--containall"]
        cmd.extend(["--env", f"ROS_DOMAIN_ID={id}"])
        for env_var, port in ports.items():
            cmd.extend(["--env", f"{env_var}={port}"])

        self._append_runtime_flags(cmd)
        cmd.extend([self.sif_path, instance_name])
        return cmd

    @staticmethod
    def get_stop_command(instance_name: str) -> list[str]:
        return ["apptainer", "instance", "stop", instance_name]

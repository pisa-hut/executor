from pathlib import Path
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


class ApptainerServiceConfig:
    """Configuration for an Apptainer service."""

    def __init__(
        self,
        sif_path: str,
        startup_wait: float = 2.0,
        bind_mounts: list[tuple[str, str]] = [],
        extra_envs: dict[str, str] = {},
        nv_runtime: bool = False,
    ):
        self.sif_path = sif_path
        self.startup_wait = startup_wait
        self.bind_mounts = bind_mounts
        self.extra_envs = extra_envs
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
        if image_path is None:
            logger.error("Missing required field 'image_path' in component spec")
            return None

        bind_mounts = component_spec.get("bind_mounts", [])
        nv_runtime = bool(component_spec.get("nv_runtime", False))
        extra_envs = component_spec.get("extra_envs", {})

        try:
            return cls(
                sif_path=cls._resolve_sif_path(str(image_path)),
                bind_mounts=bind_mounts,
                extra_envs=extra_envs,
                nv_runtime=nv_runtime,
            )
        except (TypeError, ValueError):
            logger.error("Invalid component spec types for Apptainer service config")
            return None

    def _append_runtime_flags(self, cmd: list[str]) -> None:
        for host_path, container_path in self.bind_mounts:
            cmd.extend(["--bind", f"{host_path}:{container_path}"])

        if self.nv_runtime:
            cmd.append("--nv")

    def get_start_command(
        self, instance_name: str, env_vars: dict[str, Any]
    ) -> list[str]:
        cmd = ["apptainer", "instance", "start"]
        for env_var, value in env_vars.items():
            cmd.extend(["--env", f"{env_var}={value}"])

        self._append_runtime_flags(cmd)
        cmd.extend([self.sif_path, instance_name])
        return cmd

    @staticmethod
    def get_stop_command(instance_name: str) -> list[str]:
        return ["apptainer", "instance", "stop", instance_name]

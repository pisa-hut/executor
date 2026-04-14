import os
from typing import Any, Optional

from loguru import logger


class DockerServiceConfig:
    """Configuration for a Docker service."""

    def __init__(
        self,
        image: str,
        bind_mounts: list[tuple[str, str]] | None = None,
        extra_envs: dict[str, str] | None = None,
        nv_runtime: bool = False,
    ):
        self.image = image
        self.bind_mounts = bind_mounts or []
        self.extra_envs = extra_envs or {}
        self.nv_runtime = nv_runtime

    @staticmethod
    def _resolve_image_path(component_spec: dict[str, Any]) -> Optional[str]:
        image_path = component_spec.get("image_path")
        if isinstance(image_path, dict):
            resolved = image_path.get("docker")
            return str(resolved) if resolved is not None else None
        if isinstance(image_path, str):
            return image_path
        return None

    @classmethod
    def from_component_spec(
        cls,
        component_spec: dict[str, Any],
    ) -> Optional["DockerServiceConfig"]:
        image = cls._resolve_image_path(component_spec)
        if image is None:
            logger.error("Missing required field 'image_path' in component spec")
            return None

        bind_mounts = component_spec.get("bind_mounts", [])
        extra_envs = component_spec.get("extra_envs", {})
        nv_runtime = bool(component_spec.get("nv_runtime", False))

        try:
            return cls(
                image=str(image),
                bind_mounts=bind_mounts,
                extra_envs=extra_envs,
                nv_runtime=nv_runtime,
            )
        except (TypeError, ValueError):
            logger.error("Invalid component spec types for Docker service config")
            return None

    def get_start_command(
        self,
        service_name: str,
        env_vars: dict[str, Any],
        port: int,
    ) -> list[str]:
        cmd = [
            "docker",
            "run",
            "-d",
            "--rm",
            "--log-driver",
            "syslog",
            "--log-opt",
            "syslog-address=udp://localhost:514",
            "--name",
            service_name,
            "--hostname",
            service_name,
            "--user",
            f"{os.getuid()}:{os.getgid()}",
        ]

        for env_var, value in env_vars.items():
            cmd.extend(["-e", f"{env_var}={value}"])

        cmd.extend(["-p", f"{port}:{port}"])

        if "CARLA_PORT" in env_vars:
            carla_port = env_vars["CARLA_PORT"]
            cmd.extend(["-p", f"{carla_port}:{carla_port}"])

        for host_path, container_path in self.bind_mounts:
            cmd.extend(["-v", f"{host_path}:{container_path}"])

        if self.nv_runtime:
            cmd.extend(["--gpus", "all"])

        cmd.append(self.image)
        return cmd

    @staticmethod
    def get_stop_command(service_name: str) -> list[str]:
        return ["docker", "stop", service_name]

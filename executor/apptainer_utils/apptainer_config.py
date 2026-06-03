import os
import re
import subprocess
import time

from loguru import logger
from pathlib import Path
from typing import Any, Optional

# URI schemes apptainer pull accepts. When image_path starts with one of
# these we treat it as a remote reference, pull (cache-aware) into
# PISA_SIF_DIR, and use the local path for `apptainer run`.
_URI_SCHEMES: tuple[str, ...] = (
    "oras://",
    "docker://",
    "library://",
    "http://",
    "https://",
    "shub://",
    "file://",
)


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
        # URI-shaped value → `apptainer pull --force` into PISA_SIF_DIR
        # under a stable per-URI filename. --force re-fetches the manifest
        # so tag rotations (e.g., CI pushes a new :main) propagate without
        # operator action. The previous "skip-if-cached" workaround was
        # added when we pulled directly from Docker Hub and the manifest
        # HEADs burned the 100/6h anonymous quota; with the zot.hcislab.org
        # pull-through cache now in front of Docker Hub, the manifest call
        # is LAN-local and cheap, so we can re-enable refresh-on-pull.
        if image_path.startswith(_URI_SCHEMES):
            cache_dir = Path(os.environ.get("PISA_SIF_DIR", "/opt/pisa/sif"))
            cache_dir.mkdir(parents=True, exist_ok=True)
            local_name = re.sub(r"[^A-Za-z0-9._-]", "_", image_path).strip("_") + ".sif"
            local_path = cache_dir / local_name
            logger.info(f"apptainer pull {image_path} -> {local_path}")
            # Stream the subprocess's stdout/stderr line-by-line through
            # loguru so the pull's progress lands in the manager Log
            # Drawer (LogCapture only sees loguru + stdlib logging
            # records, not raw subprocess output). Multi-GB pulls take
            # tens of seconds even from zot, so silence makes it look
            # like the executor is wedged.
            cmd = [
                "apptainer",
                "pull",
                "--force",
                "--dir",
                str(cache_dir),
                local_name,
                image_path,
            ]
            started = time.monotonic()
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # merge so order is preserved
                text=True,
                bufsize=1,  # line-buffered
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                stripped = line.rstrip()
                if stripped:
                    logger.info(f"apptainer: {stripped}")
            returncode = proc.wait()
            if returncode != 0:
                raise subprocess.CalledProcessError(returncode, cmd)
            elapsed = time.monotonic() - started
            logger.info(f"apptainer pull complete in {elapsed:.1f}s -> {local_path}")
            return str(local_path)

        # Filesystem fallback for entities that haven't migrated to URIs.
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

    @staticmethod
    def _resolve_image_path(component_spec: dict[str, Any]) -> Optional[str]:
        image_path = component_spec.get("image_path")
        if isinstance(image_path, dict):
            resolved = image_path.get("apptainer")
            return str(resolved) if resolved is not None else None
        if isinstance(image_path, str):
            return image_path
        return None

    @classmethod
    def from_component_spec(
        cls,
        component_spec: dict[str, Any],
    ) -> Optional["ApptainerServiceConfig"]:
        image_path = cls._resolve_image_path(component_spec)
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
        except TypeError, ValueError:
            logger.error("Invalid component spec types for Apptainer service config")
            return None

    def get_run_command(self, env_vars: dict[str, Any]) -> list[str]:
        # Foreground `apptainer run` so the container is a child of
        # this executor process — and the executor is itself a child
        # of the SLURM step. Daemonised `instance start` would escape
        # SLURM's hierarchy: SLURM couldn't account CPU/mem against
        # the job, kill it on time-limit, or reap it on `scancel`.
        # --containall isolates the container from the host's PID/IPC
        # namespaces and environment so the wrapper doesn't inherit
        # SLURM/login-shell env vars that can collide with what the
        # container expects (PYTHONPATH, ROS_*, LD_LIBRARY_PATH, etc.).
        cmd = ["apptainer", "run", "--containall", "--writable-tmpfs"]

        for env_var, value in env_vars.items():
            cmd.extend(["--env", f"{env_var}={value}"])

        for host_path, container_path in self.bind_mounts:
            cmd.extend(["--bind", f"{host_path}:{container_path}"])

        if self.nv_runtime:
            cmd.append("--nv")

        cmd.append(self.sif_path)
        return cmd

import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request

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

# OCI manifest media types accepted in HEAD probes. Sending these as
# Accept ensures the registry returns the proper Docker-Content-Digest
# header (some registries 404 if Accept doesn't match).
_OCI_MANIFEST_ACCEPT = ",".join(
    [
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    ]
)


def _remote_manifest_digest(image_path: str) -> Optional[str]:
    """Fast HEAD probe for the registry manifest digest.

    Returns the registry's Docker-Content-Digest header (e.g.
    "sha256:abc..."), or None if the probe fails for any reason — in
    which case the caller falls back to a full apptainer pull.

    Only oras://, docker://, http(s):// are handled; other schemes
    return None and the caller does an unconditional pull.
    """
    for scheme in ("oras://", "docker://", "https://", "http://"):
        if image_path.startswith(scheme):
            rest = image_path[len(scheme) :]
            break
    else:
        return None
    if ":" not in rest or "/" not in rest:
        return None
    repo_part, tag = rest.rsplit(":", 1)
    host, _, repo = repo_part.partition("/")
    proto = "http" if image_path.startswith("http://") else "https"
    url = f"{proto}://{host}/v2/{repo}/manifests/{tag}"
    req = urllib.request.Request(
        url, method="HEAD", headers={"Accept": _OCI_MANIFEST_ACCEPT}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.headers.get("Docker-Content-Digest")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        logger.warning(f"manifest HEAD failed for {image_path}: {exc}")
        return None


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
        # URI-shaped value → pull into PISA_SIF_DIR under a stable
        # per-URI filename. Fast-path: HEAD the registry manifest and
        # compare its digest against a sidecar we wrote at last pull.
        # On match, skip apptainer pull entirely (saves the SIF-file
        # rewrite from cached blobs, ~0.5-1s per task). On mismatch /
        # no sidecar / HEAD failure: full pull, streaming output to
        # loguru so progress lands in the manager Log Drawer.
        if image_path.startswith(_URI_SCHEMES):
            cache_dir = Path(os.environ.get("PISA_SIF_DIR", "/opt/pisa/sif"))
            cache_dir.mkdir(parents=True, exist_ok=True)
            local_name = re.sub(r"[^A-Za-z0-9._-]", "_", image_path).strip("_") + ".sif"
            local_path = cache_dir / local_name
            digest_file = local_path.with_name(local_path.name + ".digest")

            remote_digest = _remote_manifest_digest(image_path)
            if (
                remote_digest
                and local_path.exists()
                and digest_file.exists()
                and digest_file.read_text().strip() == remote_digest
            ):
                logger.info(
                    f"apptainer SIF up-to-date ({remote_digest[:19]}…), skipping pull: {local_path}"
                )
                return str(local_path)

            logger.info(f"apptainer pull {image_path} -> {local_path}")
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
            # Watchdog so the manager keeps seeing heartbeats while a
            # layer is transferring. Apptainer's progress bar uses
            # carriage returns (no newlines), so the line-reader loop
            # below stays blocked through the entire layer fetch.
            # Without these log lines, no log/append PUT would fire and
            # the reaper (REAPER_STALE_SECS=300 default) would abort
            # the task_run mid-pull.
            heartbeat_stop = threading.Event()

            def _heartbeat() -> None:
                elapsed_s = 0
                while not heartbeat_stop.wait(60):
                    elapsed_s += 60
                    logger.info(
                        f"apptainer pull still running, {elapsed_s}s elapsed"
                    )

            watchdog = threading.Thread(
                target=_heartbeat, name="apptainer-pull-watchdog", daemon=True
            )
            watchdog.start()
            try:
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
            finally:
                heartbeat_stop.set()
                watchdog.join(timeout=2)
            elapsed = time.monotonic() - started
            logger.info(f"apptainer pull complete in {elapsed:.1f}s -> {local_path}")

            # Record the digest we just pulled so the next task can
            # short-circuit. We use the HEAD-probed digest; if that
            # probe failed (remote_digest is None), skip the sidecar
            # write — next task will pull again, which is correct
            # (we can't confirm what we have).
            if remote_digest:
                digest_file.write_text(remote_digest)

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

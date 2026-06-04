import json
import os
import socket
import subprocess
import uuid
from importlib import metadata
from pathlib import Path


def collect_executor_identity() -> dict[str, str | int]:
    slurm_job_id = int(os.getenv("SLURM_JOB_ID", "0"))
    slurm_node_list = os.getenv("SLURM_NODELIST", "unknown")
    slurm_cluster_name = os.getenv("SLURM_CLUSTER_NAME", "unknown")
    hostname = socket.gethostname()
    return {
        "executor_uuid": str(uuid.uuid4()),
        "hostname": hostname,
        "job_id": slurm_job_id,
        "node_list": slurm_node_list,
        "cluster_name": slurm_cluster_name,
    }


# Backward-compatible alias for existing imports.
def collect_worker_identity() -> dict[str, str | int]:
    return collect_executor_identity()


def executor_commit_sha() -> str:
    """Return the SHA of the executor checkout, or "unknown" if not in a
    git working tree. Walks up from this file to find the repo root."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parent,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return "unknown"
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip() or "unknown"


def simcore_commit_sha() -> str:
    """Return the simcore commit SHA from PEP 610 direct_url.json (set by
    uv/pip for git-sourced installs). Falls back to "unknown" if simcore
    was installed from a non-git source (e.g. editable path) or the file
    is missing."""
    try:
        dist = metadata.distribution("simcore")
        raw = dist.read_text("direct_url.json")
    except metadata.PackageNotFoundError:
        return "unknown"
    if not raw:
        return "unknown"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return "unknown"
    commit = data.get("vcs_info", {}).get("commit_id")
    if commit:
        return commit
    # Editable install: direct_url.json has dir_info instead — try git there.
    local_path = data.get("url", "")
    if local_path.startswith("file://"):
        src = Path(local_path[len("file://") :])
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=src,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return "unknown"
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    return "unknown"

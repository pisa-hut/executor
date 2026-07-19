"""Per-task phase-timing CSV, enabled by EXECUTOR_TIMING_DIR.

One executor process handles one task, so each process appends exactly one
line to its own per-job file — no cross-process interleaving.
"""

import os
import socket
from pathlib import Path

MARK_FIELDS = [
    "claim_start",
    "claim_end",
    "stage_start",
    "stage_end",
    "container_start",
    "container_up",
    "report_start",
    "report_end",
]


def csv_line(
    task_id: int,
    task_run_id: int | None,
    mode: str,
    label: str,
    hostname: str,
    marks: dict[str, float],
) -> str:
    values = [str(task_id), "" if task_run_id is None else str(task_run_id)]
    values += [mode, label, hostname]
    values += [f"{marks[f]:.6f}" if f in marks else "" for f in MARK_FIELDS]
    return ",".join(values)


def write_line(
    task_id: int,
    task_run_id: int | None,
    mode: str,
    marks: dict[str, float],
) -> Path | None:
    out_dir = os.getenv("EXECUTOR_TIMING_DIR")
    if not out_dir:
        return None
    label = os.getenv("EXECUTOR_TIMING_LABEL") or "unlabeled"
    job = os.getenv("SLURM_JOB_ID") or f"local-{os.getpid()}"
    path = Path(out_dir).expanduser() / label / f"job{job}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    line = csv_line(task_id, task_run_id, mode, label, socket.gethostname(), marks)
    with open(path, "a") as f:
        f.write(line + "\n")
    return path

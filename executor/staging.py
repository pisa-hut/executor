"""Fetch per-task inputs (map/scenario/config bytes) from the manager and lay
them out on the local filesystem so the container bind-mounts don't need a
shared FS with the manager."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote as url_quote

import requests
from loguru import logger


@dataclass
class StagedPaths:
    """Absolute host paths created during staging."""

    xodr_dir: Path
    osm_dir: Path
    scenario_dir: Path
    av_config: Path
    simulator_config: Path
    sampler_config: Optional[Path]
    monitor_config: Path


# Default simcore monitor config bundled with the executor so we don't need
# another `PISA_DATA_DIR/config/monitor/*.yaml` on disk. Single timeout
# root keeps Monitor happy (it rejects empty-children composites).
DEFAULT_MONITOR_YAML = """\
condition:
  type: or
  name: default
  children:
    - type: timeout
      name: scenario-timeout
      timeout_ms: 60000
"""


def _safe_dest(base_dir: Path, rel: str) -> Path:
    """Return ``base_dir / rel``, raising ``ValueError`` on path traversal.

    Rejects absolute ``rel`` values and any path that, after resolution,
    escapes ``base_dir``.  This prevents a malicious manager response from
    overwriting files outside the staging root.
    """
    if Path(rel).is_absolute():
        raise ValueError(f"Refusing absolute relative_path from manager: {rel!r}")
    resolved_base = base_dir.resolve()
    dest = (base_dir / rel).resolve()
    try:
        dest.relative_to(resolved_base)
    except ValueError:
        raise ValueError(
            f"relative_path {rel!r} escapes staging directory {base_dir}"
        )
    return dest


def _fetch_into(session: requests.Session, url: str, dest: Path, timeout: int) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Fetching {url} -> {dest}")
    with session.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    fh.write(chunk)


def stage_task_inputs(
    manager_url: str,
    stage_root: Path,
    map_id: int,
    scenario_id: int,
    av_id: int,
    simulator_id: int,
    sampler_id: int,
    timeout: int = 60,
) -> StagedPaths:
    """Download everything the task needs into `stage_root`.

    Layout produced:
        <stage_root>/map/xodr/<rel files under map_file relative_path "xodr/...">
        <stage_root>/map/osm/<rel files under map_file relative_path "osm/...">
        <stage_root>/scenario/<scenario_file relative_path...>
        <stage_root>/config/{av,simulator,sampler}.yaml

    The wrapper container bind-mounts `map/xodr` -> `/mnt/map/xodr`,
    `map/osm` -> `/mnt/map/osm`, and `scenario` -> `/mnt/scenario`, so the
    relative paths saved in the database must mirror that layout (e.g. the
    map row for `tyms` stores its files as `xodr/tyms.xodr` and
    `osm/tyms.osm`).
    """
    if stage_root.exists():
        shutil.rmtree(stage_root)
    stage_root.mkdir(parents=True)

    map_dir = stage_root / "map"
    xodr_dir = map_dir / "xodr"
    osm_dir = map_dir / "osm"
    scenario_dir = stage_root / "scenario"
    config_dir = stage_root / "config"
    for d in (xodr_dir, osm_dir, scenario_dir, config_dir):
        d.mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    map_listing = session.get(
        f"{manager_url}/map/{map_id}/file", timeout=timeout
    )
    map_listing.raise_for_status()
    for entry in map_listing.json():
        rel = entry["relative_path"]
        dest = _safe_dest(map_dir, rel)
        _fetch_into(
            session,
            f"{manager_url}/map/{map_id}/file/{url_quote(rel, safe='/')}",
            dest,
            timeout,
        )

    scn_listing = session.get(
        f"{manager_url}/scenario/{scenario_id}/file", timeout=timeout
    )
    scn_listing.raise_for_status()
    for entry in scn_listing.json():
        rel = entry["relative_path"]
        dest = _safe_dest(scenario_dir, rel)
        _fetch_into(
            session,
            f"{manager_url}/scenario/{scenario_id}/file/{url_quote(rel, safe='/')}",
            dest,
            timeout,
        )

    av_config = config_dir / "av.yaml"
    _fetch_into(session, f"{manager_url}/av/{av_id}/config", av_config, timeout)

    sim_config = config_dir / "simulator.yaml"
    _fetch_into(
        session,
        f"{manager_url}/simulator/{simulator_id}/config",
        sim_config,
        timeout,
    )

    sampler_config: Optional[Path] = None
    if sampler_id:
        try:
            sp = config_dir / "sampler.yaml"
            _fetch_into(
                session,
                f"{manager_url}/sampler/{sampler_id}/config",
                sp,
                timeout,
            )
            sampler_config = sp
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 404:
                logger.debug(f"Sampler {sampler_id} has no config; skipping")
            else:
                raise

    monitor_config = config_dir / "monitor.yaml"
    monitor_config.write_text(DEFAULT_MONITOR_YAML, encoding="utf-8")

    return StagedPaths(
        xodr_dir=xodr_dir.resolve(),
        osm_dir=osm_dir.resolve(),
        scenario_dir=scenario_dir.resolve(),
        av_config=av_config.resolve(),
        simulator_config=sim_config.resolve(),
        sampler_config=sampler_config.resolve() if sampler_config else None,
        monitor_config=monitor_config.resolve(),
    )

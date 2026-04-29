import copy
from loguru import logger
import os
import re
from pathlib import Path
from typing import Any

import yaml

from executor.staging import StagedPaths


def sanitize_path(name: str) -> str:
    """Sanitize a string for safe use as a single directory name component."""
    name = name.replace(os.sep, "_")
    if os.altsep:
        name = name.replace(os.altsep, "_")
    name = re.sub(r"\.{2,}", "_", name)
    name = name.replace(" ", "_")
    return name


def resolve_host_path(host_path: str | None) -> str:
    """Resolve `$PISA_DATA_DIR`-relative paths to absolute host paths.

    Kept only for `RMLIB_PATH` — the libesminiRMLib.so binary that the
    executor dlopens and SIF images that the container runtime consumes.
    Every other task input (maps, scenarios, configs) comes from the
    manager and lands in a staging dir; none of those go through here.
    """
    if host_path is None:
        logger.warning("Received None as host path to resolve. Returning empty string.")
        return ""

    PISA_DATA_DIR = os.getenv("PISA_DATA_DIR", "/opt/pisa")

    if not os.path.isabs(host_path):
        host_path = os.path.join(PISA_DATA_DIR, host_path)
    expanded_path = os.path.expandvars(os.path.expanduser(host_path))
    absolute_path = os.path.abspath(expanded_path)
    return absolute_path


def build_services_spec(
    claimed_av: dict[str, Any],
    claimed_simulator: dict[str, Any],
    claimed_map: dict[str, Any],
    claimed_scenario: dict[str, Any],
    staged: StagedPaths,
) -> dict[str, dict[str, Any]]:
    return {
        "simulator": {
            "name": claimed_simulator.get("name"),
            "image_path": claimed_simulator.get("image_path"),
            "nv_runtime": claimed_simulator.get("nv_runtime", False),
            "ros_runtime": claimed_simulator.get("ros_runtime", False),
            "carla_runtime": claimed_simulator.get("carla_runtime", False),
        },
        "av": {
            "name": claimed_av.get("name"),
            "image_path": claimed_av.get("image_path"),
            "nv_runtime": claimed_av.get("nv_runtime", False),
            "ros_runtime": claimed_av.get("ros_runtime", False),
            "carla_runtime": claimed_av.get("carla_runtime", False),
        },
        "map": {
            "xodr_path": str(staged.xodr_dir),
            "osm_path": str(staged.osm_dir),
        },
        "scenario": {
            "scenario_path": str(staged.scenario_dir),
        },
    }


def build_runner_spec(
    claimed_spec: dict[str, dict[str, Any]],
    claimed_simulator: dict[str, Any],
    claimed_av: dict[str, Any],
    claimed_map: dict[str, Any],
    claimed_scenario: dict[str, Any],
    started_specs: dict[str, dict[str, Any]],
    staged: StagedPaths,
    job_id: Any,
    output_dir: str,
) -> dict[str, Any]:
    simulator_started_spec = started_specs.get("simulator", {})
    av_started_spec = started_specs.get("av", {})

    return {
        "runtime": {
            "dt": 0.05,
        },
        "task": {
            "job_id": str(job_id),
            "output_dir": output_dir,
        },
        "simulator": {
            "config_path": str(staged.simulator_config),
            "map": simulator_started_spec.get("map", {}),
            "scenario": {
                "format": claimed_scenario.get("format"),
                "name": claimed_scenario.get("title"),
                "path": simulator_started_spec.get("scenario_path", {}),
            },
            "output_path": simulator_started_spec.get("output_path", {}),
            "url": simulator_started_spec.get("service_info", {}).get("url", {}),
        },
        "av": {
            "config_path": str(staged.av_config),
            "map": av_started_spec.get("map", {}),
            "output_path": av_started_spec.get("output_path", {}),
            "url": av_started_spec.get("service_info", {}).get("url", {}),
        },
        "map": {
            "name": claimed_map.get("name"),
            "osm_path": str(staged.osm_dir),
            "xodr_path": str(staged.xodr_dir),
        },
        "scenario": {
            "goal_config": _read_goal_config(staged.scenario_dir),
            "title": claimed_scenario.get("title"),
            "scenario_path": str(staged.scenario_dir),
            "rmlib_path": resolve_host_path(
                os.getenv(
                    "RMLIB_PATH",
                    f"{os.getenv('PISA_DATA_DIR', '/opt/pisa')}/lib/libesminiRMLib.so",
                )
            ),
        },
        "sampler": _build_sampler_spec(claimed_spec, staged),
        "monitor": {
            "module_path": "simcore.monitor.base:Monitor",
            "config_path": str(staged.monitor_config),
        },
    }


def _build_sampler_spec(
    claimed_spec: dict[str, dict[str, Any]],
    staged: StagedPaths,
) -> dict[str, Any]:
    sampler = copy.deepcopy(claimed_spec.get("sampler", {}))
    if staged.sampler_config is not None:
        sampler["config_path"] = str(staged.sampler_config)
    else:
        sampler.pop("config_path", None)
    return sampler


def _read_goal_config(scenario_dir: Path) -> dict[str, Any]:
    """Parse the scenario's spec.yaml and return the ego dict in the shape
    simcore expects (position + target_speed). Tolerates the legacy
    spec.yaml variant where the destination lives under `goal` instead of
    `position`."""
    spec_path = scenario_dir / "spec.yaml"
    if not spec_path.is_file():
        logger.warning(f"{spec_path} missing; running with empty goal_config")
        return {}
    try:
        data = yaml.safe_load(spec_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        logger.warning(f"{spec_path}: YAML parse failed ({exc}); empty goal_config")
        return {}
    ego = data.get("ego") if isinstance(data, dict) else None
    if not isinstance(ego, dict):
        return {}
    if "position" not in ego and "goal" in ego:
        ego = dict(ego)
        ego["position"] = ego.pop("goal")
    return ego

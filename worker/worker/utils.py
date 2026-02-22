import copy
import os
from typing import Any


def resolve_host_path(host_path: str) -> str:
    """
    Resolve a host path, expanding user and environment variables, and converting to an absolute path.

    Args:
        host_path (str): The input host path to resolve.
    Returns:
        str: The resolved absolute host path.
    """

    SBSVF_DIR = os.getenv("SBSVF_DIR", "/opt/sbsvf")

    if not os.path.isabs(host_path):
        host_path = os.path.join(SBSVF_DIR, host_path)
    expanded_path = os.path.expandvars(os.path.expanduser(host_path))
    absolute_path = os.path.abspath(expanded_path)
    return absolute_path


def build_services_spec(
    claimed_simulator: dict[str, Any],
    claimed_av: dict[str, Any],
    claimed_map: dict[str, Any],
    claimed_scenario: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    worker_scenario_path = claimed_scenario.get("scenario_path")

    return {
        "simulator": {
            "name": claimed_simulator.get("name"),
            "image_path": claimed_simulator.get("image_path"),
            "extra_ports": copy.deepcopy(claimed_simulator.get("extra_ports")),
            "nv_runtime": claimed_simulator.get("nv_runtime", False),
        },
        "av": {
            "name": claimed_av.get("name"),
            "image_path": claimed_av.get("image_path"),
            "nv_runtime": claimed_av.get("nv_runtime", False),
        },
        "map": {
            "osm_path": claimed_map.get("osm_path"),
            "xodr_path": claimed_map.get("xodr_path"),
        },
        "scenario": {
            "scenario_path": worker_scenario_path,
        },
    }


def build_runner_spec(
    claimed_spec: dict[str, dict[str, Any]],
    claimed_simulator: dict[str, Any],
    claimed_av: dict[str, Any],
    claimed_map: dict[str, Any],
    claimed_scenario: dict[str, Any],
    started_specs: dict[str, dict[str, Any]],
    job_id: Any,
    output_dir: str,
) -> dict[str, Any]:
    simulator_started_spec = started_specs.get("simulator", {})
    av_started_spec = started_specs.get("av", {})

    return {
        "runtime": {
            "dt": 0.01,
        },
        "task": {
            "job_id": str(job_id),
            "output_dir": output_dir,
        },
        "simulator": {
            "config_path": resolve_host_path(claimed_simulator.get("config_path")),
            "map": simulator_started_spec.get("map", {}),
            "scenario": {
                "title": claimed_scenario.get("title"),
                "path": simulator_started_spec.get("scenario_path", {}),
            },
            "output_path": simulator_started_spec.get("output_path", {}),
            "url": simulator_started_spec.get("service_info", {}).get("url", {}),
        },
        "av": {
            "config_path": resolve_host_path(claimed_av.get("config_path")),
            "map": av_started_spec.get("map", {}),
            "scenario": {
                "title": claimed_scenario.get("title"),
                "path": av_started_spec.get("scenario", {}).get(
                    "path", av_started_spec.get("scenario_path", {})
                ),
            },
            "output_path": av_started_spec.get("output_path", {}),
            "url": av_started_spec.get("service_info", {}).get("url", {}),
        },
        "map": {
            "name": claimed_map.get("name"),
            "osm_path": resolve_host_path(claimed_map.get("osm_path")),
            "xodr_path": resolve_host_path(claimed_map.get("xodr_path")),
        },
        "scenario": {
            "goal_config": claimed_scenario.get("goal_config"),
            "title": claimed_scenario.get("title"),
            "scenario_path": resolve_host_path(claimed_scenario.get("scenario_path")),
            "rmlib_path": resolve_host_path(
                os.getenv(
                    "RMLIB_PATH",
                    f"{os.getenv('SBSVF_DIR', '/opt/sbsvf')}/lib/libesminiRMLib.so",
                )
            ),
        },
        "sampler": copy.deepcopy(claimed_spec.get("sampler", {})),
    }

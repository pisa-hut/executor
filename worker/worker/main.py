import copy
import dotenv
import logging
import os
from pprint import pprint
from typing import Any

from worker.apptainer_utils.apptainer_manager import ApptainerServiceManager
from worker.manager_client import ManagerClient
from worker.runner.runner import Runner
from worker.system import collect_worker_identity
from worker.utils import resolve_host_path

dotenv.load_dotenv()


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)


def main():
    logger.info("Starting worker...")
    client = ManagerClient()
    slurm_info = collect_worker_identity()
    worker_info = client.register_worker(slurm_info)
    logger.info(f"Registered worker with ID: {worker_info['id']}")
    job_id = slurm_info.get("job_id", "unknown")
    worker_id = worker_info.get("id", "unknown")

    assert isinstance(worker_info["id"], int)
    response = client.claim_task(worker_info["id"])
    if response is None:
        print("No tasks available to claim.")
        return
    assert isinstance(response, dict)

    claimed_spec: dict[str, dict[str, Any]] = response
    task_id = claimed_spec.get("task", {}).get("id")
    logger.info(f"Claimed task with ID: {task_id}")

    claimed_scenario = dict(claimed_spec.get("scenario", {}))
    claimed_simulator = dict(claimed_spec.get("simulator", {}))
    claimed_av = dict(claimed_spec.get("av", {}))
    claimed_map = dict(claimed_spec.get("map", {}))

    worker_scenario_path = claimed_scenario.get("scenairo_path")
    if worker_scenario_path is None:
        worker_scenario_path = claimed_scenario.get("scenario_path")

    services_spec: dict[str, dict[str, Any]] = {
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
            "scenairo_path": worker_scenario_path,
        },
    }

    logger.info(f"Claimed scenario: {claimed_scenario.get('title', 'unknown')}")

    service_manager = ApptainerServiceManager()

    output_dir = str(f"./outputs/job_{job_id}_worker_{worker_id}")
    os.makedirs(output_dir, exist_ok=True)

    started_specs = service_manager.start(
        services_spec=services_spec,
        output_dir=output_dir,
    )
    simulator_started_spec = started_specs.get("simulator", {})
    av_started_spec = started_specs.get("av", {})

    logger.info(f"Started services: {list(started_specs.keys())}")
    runner_spec: dict[str, Any] = {
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
                "path": av_started_spec.get("scenario_path", {}),
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

    assert isinstance(runner_spec["scenario"], dict)

    pprint(runner_spec)
    try:
        runner = Runner(runner_spec)
        runner.exec()
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {str(exc)}"
        logger.exception(f"Task execution failed with error: {err_msg}")
        client.task_failed(task_id, reason=err_msg)
    else:
        client.task_succeeded(task_id)
    finally:
        service_manager.stop_all_services()


if __name__ == "__main__":
    main()

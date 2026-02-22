import dotenv
import logging
import os
from pprint import pprint
from typing import Any, Optional

from worker.apptainer_utils.apptainer_manager import ApptainerServiceManager
from worker.manager_client import ManagerClient
from worker.runner.runner import Runner
from worker.system import collect_worker_identity
from worker.utils import build_runner_spec, build_services_spec

dotenv.load_dotenv()


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)


def _claim_task_spec(
    client: ManagerClient,
    worker_id: int,
) -> Optional[dict[str, dict[str, Any]]]:
    response = client.claim_task(worker_id)
    if response is None:
        logger.info("No tasks available to claim.")
        return None
    assert isinstance(response, dict)
    return response


def _execute_runner_task(
    client: ManagerClient,
    task_id: Any,
    runner_spec: dict[str, Any],
) -> None:
    assert isinstance(runner_spec.get("scenario"), dict)

    pprint(runner_spec)
    try:
        runner = Runner(runner_spec)
        runner.exec()
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {str(exc)}"
        logger.exception("Task execution failed with error: %s", err_msg)
        client.task_failed(task_id, reason=err_msg)
    else:
        client.task_succeeded(task_id)


def main():
    logger.info("Starting worker...")
    client = ManagerClient()
    slurm_info = collect_worker_identity()
    worker_info = client.register_worker(slurm_info)
    logger.info("Registered worker with ID: %s", worker_info["id"])

    job_id = slurm_info.get("job_id", "unknown")
    worker_id = worker_info.get("id", "unknown")
    assert isinstance(worker_info["id"], int)

    claimed_spec = _claim_task_spec(client, worker_info["id"])
    if claimed_spec is None:
        return

    task_id = claimed_spec.get("task", {}).get("id")
    logger.info("Claimed task with ID: %s", task_id)

    claimed_scenario = dict(claimed_spec.get("scenario", {}))
    claimed_simulator = dict(claimed_spec.get("simulator", {}))
    claimed_av = dict(claimed_spec.get("av", {}))
    claimed_map = dict(claimed_spec.get("map", {}))
    logger.info("Claimed scenario: %s", claimed_scenario.get("title", "unknown"))

    services_spec = build_services_spec(
        claimed_simulator=claimed_simulator,
        claimed_av=claimed_av,
        claimed_map=claimed_map,
        claimed_scenario=claimed_scenario,
    )

    output_dir = str(f"./outputs/job_{job_id}_worker_{worker_id}")
    os.makedirs(output_dir, exist_ok=True)

    service_manager = ApptainerServiceManager()
    try:
        started_specs = service_manager.start(
            services_spec=services_spec,
            output_dir=output_dir,
        )
        logger.info("Started services: %s", list(started_specs.keys()))

        runner_spec = build_runner_spec(
            claimed_spec=claimed_spec,
            claimed_simulator=claimed_simulator,
            claimed_av=claimed_av,
            claimed_map=claimed_map,
            claimed_scenario=claimed_scenario,
            started_specs=started_specs,
            job_id=job_id,
            output_dir=output_dir,
        )
        _execute_runner_task(client=client, task_id=task_id, runner_spec=runner_spec)
    finally:
        service_manager.stop_all_services()


if __name__ == "__main__":
    main()

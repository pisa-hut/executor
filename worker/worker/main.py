import argparse
import dotenv
import logging
import os
from pprint import pprint
from typing import Any

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
    except KeyboardInterrupt:
        logger.warning("Task execution interrupted by user.")
        client.task_failed(task_id, reason="Task interrupted by user")
    except Exception as exc:
        if isinstance(exc, RuntimeError):
            if "Exceeded maximum retries for route not found errors" in str(exc):
                logger.error(
                    f"Task execution failed due to repeated route not found errors: {exc}"
                )
            elif "AV reset timeout error" in str(exc):
                logger.error(
                    f"Task execution failed due to repeated AV reset timeout errors: {exc}"
                )
            client.task_invalid(task_id, reason=str(exc))
        else:
            err_msg = f"{type(exc).__name__}: {str(exc)}"
            logger.error("Task execution failed with error: %s", err_msg)
            client.task_failed(task_id, reason=err_msg)
    else:
        logger.info("Task execution succeeded for task ID: %s", task_id)
        client.task_succeeded(task_id)


def parse_args(
    avs: dict[str, int], simulators: dict[str, int], samplers: dict[str, int]
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Worker process that claims and executes tasks from the manager."
    )
    parser.add_argument(
        "--plan-id",
        type=int,
        default=None,
        help="ID of the plan to filter tasks by (optional)",
    )
    parser.add_argument(
        "--av",
        type=str,
        choices=list(avs.keys()),
        default=None,
        help="Name of the AV to filter tasks by (optional)",
    )
    parser.add_argument(
        "--simulator",
        type=str,
        choices=list(simulators.keys()),
        default=None,
        help="Name of the simulator to filter tasks by (optional)",
    )
    parser.add_argument(
        "--sampler",
        type=str,
        choices=list(samplers.keys()),
        default=None,
        help="Name of the sampler to filter tasks by (optional)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (e.g., DEBUG, INFO, WARNING, ERROR)",
    )
    return parser.parse_args()


def main():
    client = ManagerClient()
    client.fetch()  # Fetch AVs, simulators, and samplers to cache their IDs

    args = parse_args(client.avs, client.simulators, client.samplers)
    logger.setLevel(getattr(logging, args.log_level.upper()))

    logger.info("Starting worker...")
    slurm_info = collect_worker_identity()

    job_id = int(slurm_info.get("job_id", "unknown"))

    claimed_spec = client.claim_task_spec(
        slurm_info,
        plan_id=args.plan_id,
        av_name=args.av,
        simulator_name=args.simulator,
        sampler_name=args.sampler,
    )

    if claimed_spec is None:
        logger.info("No task claimed. Worker will exit.")
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

    output_dir = str(f"./outputs/job_{job_id}")
    os.makedirs(output_dir, exist_ok=True)

    service_manager = ApptainerServiceManager(id=f"job{job_id:02d}")
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
    except Exception as exc:
        logger.error("Worker failed with error: %s", exc)
        if task_id is not None:
            err_msg = f"{type(exc).__name__}: {str(exc)}"
            client.task_failed(task_id, reason=err_msg)

    finally:
        service_manager.stop_all_services()


if __name__ == "__main__":
    main()

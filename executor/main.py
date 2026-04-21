import argparse
import dotenv
import json
from loguru import logger
import os
import sys
from pathlib import Path
from typing import Any

from simcore.engine import SimulationEngine

from executor.apptainer_utils.apptainer_manager import ApptainerServiceManager
from executor.docker_utils.docker_manager import DockerServiceManager
from executor.manager_client import ManagerClient
from executor.log_capture import LogCapture, install as install_log_capture
from executor.log_streamer import LogStreamer
from executor.service_manager import ServiceManager
from executor.staging import stage_task_inputs
from executor.system import collect_executor_identity
from executor.utils import (
    build_runner_spec,
    build_services_spec,
    sanitize_path,
)

dotenv.load_dotenv()


def _create_service_manager(backend: str, job_id: int) -> ServiceManager:
    service_manager_id = f"job{job_id:02d}"
    if backend == "apptainer":
        return ApptainerServiceManager(id=service_manager_id)
    if backend == "docker":
        return DockerServiceManager(id=service_manager_id)

    raise ValueError(f"Unsupported backend: {backend}")


def _execute_runner_task(
    client: ManagerClient,
    task_id: Any,
    runner_spec: dict[str, Any],
    capture: "LogCapture | None" = None,
) -> None:
    def _log() -> "str | None":
        return capture.snapshot() if capture is not None else None

    try:
        engine = SimulationEngine(runner_spec)
        engine.exec()
    except KeyboardInterrupt:
        logger.warning("Task execution interrupted by user.")
        client.task_failed(task_id, reason="Task interrupted by user", log=_log())
    except Exception as exc:
        if isinstance(exc, RuntimeError):
            if (
                "Failed to set Autoware route points.".lower() in str(exc).lower()
                or "not reachable from".lower() in str(exc).lower()
                or "Failed to find a reachable route candidate".lower()
                in str(exc).lower()
            ):
                logger.error(
                    f"Task execution failed due to route not found error: {exc}"
                )
                client.task_invalid(task_id, reason=str(exc), log=_log())
                return
            elif (
                "Exception calling application: failed validating <Element".lower()
                in str(exc).lower()
            ):
                logger.error(
                    f"Task execution failed due to scenario validation error: {exc}"
                )
                client.task_invalid(task_id, reason=str(exc), log=_log())
                return
            else:
                logger.error(f"Task execution failed with runtime error: {exc}")
                client.task_failed(task_id, reason=str(exc), log=_log())
                return
        else:
            err_msg = f"{type(exc).__name__}: {str(exc)}"
            logger.error(f"Task execution failed with error: {err_msg}")
            client.task_failed(task_id, reason=err_msg, log=_log())
    else:
        logger.info(f"Task execution succeeded for task ID: {task_id}")
        client.task_succeeded(task_id, log=_log())


def parse_args(
    maps: dict[str, int],
    avs: dict[str, int],
    simulators: dict[str, int],
    samplers: dict[str, int],
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executor process that claims and executes tasks from the manager."
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
        "--map",
        type=str,
        choices=list(maps.keys()),
        default=None,
        help="Name of the map to filter tasks by (optional)",
    )
    parser.add_argument(
        "--task-id",
        type=int,
        default=None,
        help="Claim a specific task by ID",
    )
    parser.add_argument(
        "--scenario-id",
        type=int,
        default=None,
        help="ID of the scenario to filter tasks by (optional)",
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
        type=str.lower,
        choices=[
            "debug",
            "info",
            "warning",
            "error",
            "critical",
        ],
        default="INFO",
        help="Logging level for the executor (default: INFO)",
    )
    parser.add_argument(
        "--backend",
        type=str,
        choices=["apptainer", "docker"],
        default="apptainer",
        help="Container backend to use for services (default: apptainer)",
    )
    return parser.parse_args()


def main():
    client = ManagerClient()
    client.fetch()  # Fetch AVs, simulators, and samplers to cache their IDs

    args = parse_args(client.maps, client.avs, client.simulators, client.samplers)
    logger.remove()  # Remove default logger
    logger.add(
        sink=sys.stdout,
        level=args.log_level.upper(),
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )

    # Capture everything the executor + simcore prints so we can PUT it back
    # to the task_run row and render it in the web UI.
    capture = LogCapture()
    install_log_capture(capture)

    logger.debug("Starting executor...")
    logger.info(f"Arguments: {args}")

    executor_info = collect_executor_identity()

    job_id = int(executor_info.get("job_id", "unknown"))

    claimed_spec = client.claim_task_spec(
        executor_info,
        task_id=args.task_id,
        av_name=args.av,
        simulator_name=args.simulator,
        map_name=args.map,
        scenario_id=args.scenario_id,
        sampler_name=args.sampler,
    )

    if claimed_spec is None:
        logger.info("No task claimed. Executor will exit.")
        return

    task_id = claimed_spec.get("task", {}).get("id")
    task_run_id = claimed_spec.get("task_run_id")
    if task_id is None:
        logger.error("Claimed spec does not contain a valid task ID. Aborting.")
        return
    logger.info(f"Claimed task with ID: {task_id} (task_run #{task_run_id})")

    log_streamer: LogStreamer | None = None
    if task_run_id is not None:
        log_streamer = LogStreamer(
            capture=capture,
            manager_url=client.manager_url,
            task_run_id=int(task_run_id),
        )
        log_streamer.start()

    claimed_av = dict(claimed_spec.get("av", {}))
    claimed_simulator = dict(claimed_spec.get("simulator", {}))
    claimed_map = dict(claimed_spec.get("map", {}))
    claimed_scenario = dict(claimed_spec.get("scenario", {}))
    scenario_title = claimed_scenario.get("title", "unknown_scenario")
    logger.info(f"Claimed scenario: {scenario_title}")

    av = claimed_av.get("name", "unknown_av")
    sim = claimed_simulator.get("name", "unknown_simulator")
    map_name = claimed_map.get("name", "unknown_map")
    cla = f"{av}_{sim}"

    output_dir = str(
        f"./outputs/{cla}/{task_id}-{sanitize_path(map_name)}-{sanitize_path(scenario_title)}"
    )
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "claimed_spec.json"), "w") as f:
        json.dump(claimed_spec, f, indent=4)

    staged = stage_task_inputs(
        manager_url=client.manager_url,
        stage_root=Path(output_dir) / ".staged",
        map_id=int(claimed_map["id"]),
        scenario_id=int(claimed_scenario["id"]),
        av_id=int(claimed_av["id"]),
        simulator_id=int(claimed_simulator["id"]),
        sampler_id=int(claimed_spec.get("sampler", {}).get("id", 0)),
    )
    logger.debug(f"Staged inputs under {Path(output_dir) / '.staged'}")

    services_spec = build_services_spec(
        claimed_av=claimed_av,
        claimed_simulator=claimed_simulator,
        claimed_map=claimed_map,
        claimed_scenario=claimed_scenario,
        staged=staged,
    )

    service_manager = _create_service_manager(args.backend, job_id)
    try:
        started_specs = service_manager.start(
            services_spec=services_spec,
            output_dir=output_dir,
        )

        runner_spec = build_runner_spec(
            claimed_spec=claimed_spec,
            claimed_simulator=claimed_simulator,
            claimed_av=claimed_av,
            claimed_map=claimed_map,
            claimed_scenario=claimed_scenario,
            started_specs=started_specs,
            staged=staged,
            job_id=job_id,
            output_dir=output_dir,
        )
        with open(os.path.join(output_dir, "runner_spec.json"), "w") as f:
            json.dump(runner_spec, f, indent=4)
        logger.debug(
            f"Runner spec available at: {os.path.join(output_dir, 'runner_spec.json')}"
        )

        _execute_runner_task(
            client=client,
            task_id=task_id,
            runner_spec=runner_spec,
            capture=capture,
        )
    except Exception as exc:
        logger.error(f"Executor failed with error: {exc}")
        if task_id is not None:
            err_msg = f"{type(exc).__name__}: {str(exc)}"
            client.task_failed(task_id, reason=err_msg, log=capture.snapshot())

    finally:
        if log_streamer is not None:
            log_streamer.stop()
        service_manager.stop_all_services()

    logger.debug("Executor finished execution.")


if __name__ == "__main__":
    main()

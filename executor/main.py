import argparse
import dotenv
import json
import shutil
import signal
import socket
import tempfile
import time
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


def _install_shutdown_handler(state: dict[str, Any]) -> None:
    """Handle SIGTERM/SIGINT cleanly: report the task as failed, flush the
    final log chunk, stop the containers, and exit. SLURM delivers
    SIGTERM ~60 s before the SIGKILL time-limit guillotine (see
    `--signal=TERM@60` in scripts/run.sh), which is enough headroom for
    the manager round-trip.

    `state` is a live dict populated by main() as the run progresses; we
    read whatever's in it at signal time. Missing keys just skip their
    piece of cleanup."""

    def handler(signum: int, _frame) -> None:
        # Classify the signal: SIGINT is always user-initiated. SIGTERM is
        # ambiguous — SLURM uses it for both `scancel` and time-limit
        # pre-kill. Compare against `SLURM_JOB_END_TIME` (epoch seconds);
        # if we're still far from the end, it's scancel.
        is_abort = signum == signal.SIGINT
        if signum == signal.SIGTERM:
            end_raw = os.environ.get("SLURM_JOB_END_TIME")
            if end_raw:
                try:
                    remaining = int(end_raw) - int(time.time())
                    # Our sbatch script requests TERM 60 s before the end;
                    # anything comfortably more than that is scancel.
                    is_abort = remaining > 90
                except ValueError:
                    pass
            else:
                # No SLURM env (running under plain docker / local shell):
                # SIGTERM is almost always someone typing `kill` by hand.
                is_abort = True

        logger.warning(
            f"Received signal {signum}; reporting task "
            f"{'aborted' if is_abort else 'failed'} and exiting"
        )

        # Order matters: stop containers + clean staged inputs FIRST so
        # the "Stopping Apptainer container ..." lines reach the manager
        # live (the log streamer keeps flushing while the task_run is
        # still `running`). Stop the streamer next so its final flush
        # carries any tail bytes. Only then send the terminal POST,
        # which finalises the task_run and triggers 410 on any
        # subsequent appends. Reversing this order is what made the
        # cleanup phase invisible in the Log Drawer.
        svc_mgr: ServiceManager | None = state.get("service_manager")
        if svc_mgr is not None:
            try:
                svc_mgr.stop_all_services()
            except Exception as exc:
                logger.error(f"service_manager stop during signal handling: {exc}")
        sr = state.get("staged_root")
        if sr is not None and Path(sr).exists():
            try:
                shutil.rmtree(sr)
            except Exception as exc:
                logger.error(f"staged_root cleanup during signal handling: {exc}")
        streamer = state.get("log_streamer")
        if streamer is not None:
            try:
                streamer.stop()
            except Exception as exc:
                logger.error(f"log streamer stop failed during signal handling: {exc}")

        task_id = state.get("task_id")
        client: ManagerClient | None = state.get("client")
        capture: LogCapture | None = state.get("capture")
        engine_obj = state.get("engine")
        useful = (
            int(getattr(engine_obj, "completed_concrete_runs", 0))
            if engine_obj is not None
            else 0
        )
        if task_id is not None and client is not None:
            try:
                snap = _build_terminal_log(capture, svc_mgr)
                if is_abort:
                    client.task_aborted(
                        task_id,
                        reason=f"Executor received signal {signum} (cancelled)",
                        log=snap,
                        concrete_scenarios_executed=useful,
                    )
                else:
                    client.task_failed(
                        task_id,
                        reason=(
                            f"Executor received signal {signum}"
                            " (SLURM time limit)"
                        ),
                        log=snap,
                        concrete_scenarios_executed=useful,
                    )
            except Exception as exc:
                logger.error(f"lifecycle call during signal handling: {exc}")
        # 128 + signum is the conventional exit code for "terminated by
        # signal N" (e.g. 143 for SIGTERM, 130 for SIGINT).
        sys.exit(128 + signum)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def _create_service_manager(backend: str, job_id: int) -> ServiceManager:
    service_manager_id = f"job{job_id:02d}"
    if backend == "apptainer":
        return ApptainerServiceManager(id=service_manager_id)
    if backend == "docker":
        return DockerServiceManager(id=service_manager_id)

    raise ValueError(f"Unsupported backend: {backend}")


def _build_terminal_log(
    capture: "LogCapture | None",
    service_manager: "ServiceManager | None",
) -> "str | None":
    """Compose the log payload attached to a terminal lifecycle POST:
    the executor's own captured log, optionally followed by a tail of
    each wrapper container's stdout. Live streaming carries only the
    executor log; wrapper output is appended once, here, so failures
    have context without flooding the live Log Drawer."""
    base = capture.snapshot() if capture is not None else None
    tail = service_manager.snapshot_wrapper_outputs() if service_manager is not None else ""
    if not tail:
        return base
    sep = "\n\n=== wrapper output (appended at task end) ===\n\n"
    return (base + sep + tail) if base else (sep.lstrip() + tail)


def _execute_runner_task(
    runner_spec: dict[str, Any],
    shutdown_state: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """Drive one task's SimulationEngine and return the terminal verb +
    reason. Does NOT send the lifecycle POST — caller does, AFTER
    cleanup, so the apptainer-stop / staged-cleanup log lines reach the
    manager live and end up in the final task_run.log too.

    Verb maps to the manager endpoint:
        exec() returns    -> ("succeeded", "")
        exec() raises     -> ("failed", reason)
        SIGTERM/SIGINT    -> ("aborted", reason) — handled in the
                              signal handler, not here.

    `concrete_scenarios_executed` is orthogonal and stays on the engine;
    main() reads it off `shutdown_state["engine"]` after this returns.
    """
    engine: SimulationEngine | None = None
    try:
        engine = SimulationEngine(runner_spec)
        # Expose the engine to the SIGTERM handler so signal-triggered
        # aborts report an accurate concrete-run count too.
        if shutdown_state is not None:
            shutdown_state["engine"] = engine
        engine.exec()
    except KeyboardInterrupt:
        logger.warning("Task execution interrupted by user.")
        return ("failed", "Task interrupted by user")
    except Exception as exc:
        # Any exception is a failure — including route-not-found and
        # scenario-validation errors that used to be reported as
        # `task_invalid`. The manager decides whether to permanently
        # invalidate the task: 10 consecutive runs with
        # concrete_scenarios_executed == 0. A single run that managed
        # to finish some concretes still counts as useful progress,
        # even if a later concrete crashed with "no route found".
        err_msg = (
            str(exc) if isinstance(exc, RuntimeError) else f"{type(exc).__name__}: {exc}"
        )
        logger.error(f"Task execution failed: {err_msg}")
        return ("failed", err_msg)
    logger.info("Task execution succeeded.")
    return ("succeeded", "")


def _send_terminal(
    client: ManagerClient,
    task_id: Any,
    verb: str,
    reason: str,
    capture: "LogCapture | None",
    service_manager: "ServiceManager | None",
    concrete_scenarios_executed: int,
) -> None:
    """Send the appropriate lifecycle POST with the full log payload —
    executor capture plus wrapper-output tail. Called after cleanup so
    container-stop and staged-cleanup lines are part of the final log."""
    log = _build_terminal_log(capture, service_manager)
    if verb == "succeeded":
        client.task_succeeded(
            task_id, log=log, concrete_scenarios_executed=concrete_scenarios_executed
        )
    elif verb == "aborted":
        client.task_aborted(
            task_id,
            reason=reason,
            log=log,
            concrete_scenarios_executed=concrete_scenarios_executed,
        )
    else:
        client.task_failed(
            task_id,
            reason=reason,
            log=log,
            concrete_scenarios_executed=concrete_scenarios_executed,
        )


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
        "--av-id",
        type=int,
        default=None,
        help="ID of the AV to filter tasks by. Takes precedence over --av.",
    )
    parser.add_argument(
        "--simulator",
        type=str,
        choices=list(simulators.keys()),
        default=None,
        help="Name of the simulator to filter tasks by (optional)",
    )
    parser.add_argument(
        "--simulator-id",
        type=int,
        default=None,
        help="ID of the simulator to filter tasks by. Takes precedence over --simulator.",
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

    # Hook SIGTERM/SIGINT early so even a pre-claim kill (unlikely, but
    # possible) doesn't leave a half-written row behind.
    shutdown_state: dict[str, Any] = {"client": client, "capture": capture}
    _install_shutdown_handler(shutdown_state)

    logger.debug("Starting executor...")
    logger.info(f"Arguments: {args}")

    executor_info = collect_executor_identity()

    job_id = int(executor_info.get("job_id", "unknown"))

    claimed_spec = client.claim_task_spec(
        executor_info,
        task_id=args.task_id,
        av_name=args.av,
        av_id=args.av_id,
        simulator_name=args.simulator,
        simulator_id=args.simulator_id,
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
    shutdown_state["task_id"] = task_id

    log_streamer: LogStreamer | None = None
    if task_run_id is not None:
        log_streamer = LogStreamer(
            capture=capture,
            manager_url=client.manager_url,
            task_run_id=int(task_run_id),
        )
        log_streamer.start()
        shutdown_state["log_streamer"] = log_streamer

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

    # Stage map / scenario / config bytes onto worker-LOCAL disk
    # (TMPDIR, falling back to /tmp). BeeGFS metadata-cache lag was
    # racing Apptainer's bind-mount pre-stat under parallel load, and
    # the bind-mount source needs to be visible in microseconds — local
    # disk simply has no such lag. Outputs (logs, runner_spec.json,
    # container stdout/stderr) still go to BeeGFS so they survive the
    # worker.
    #
    # The stage dir is unique per task (mkdtemp adds a random suffix),
    # so two parallel executors on the same node — or a retry of the
    # same task on the same node — never collide. We drop a breadcrumb
    # `stage_location.txt` into the BeeGFS output_dir so a post-mortem
    # can find the (host, path) the data was on.
    tmpdir = os.environ.get("TMPDIR") or "/tmp"
    staged_root = Path(
        tempfile.mkdtemp(prefix=f"pisa-stage-task{task_id}-", dir=tmpdir)
    )
    with open(os.path.join(output_dir, "stage_location.txt"), "w") as f:
        f.write(f"host={socket.gethostname()}\npath={staged_root}\n")
    shutdown_state["staged_root"] = staged_root
    # Monitor is required by the manager (m20260513 migration); claim
    # responses always include it. KeyError here means the manager
    # is older than this executor — surface as a configuration error.
    claimed_monitor = claimed_spec["monitor"]
    staged = stage_task_inputs(
        manager_url=client.manager_url,
        stage_root=staged_root,
        map_id=int(claimed_map["id"]),
        scenario_id=int(claimed_scenario["id"]),
        av_id=int(claimed_av["id"]),
        simulator_id=int(claimed_simulator["id"]),
        sampler_id=int(claimed_spec.get("sampler", {}).get("id", 0)),
        monitor_id=int(claimed_monitor["id"]),
    )
    logger.debug(f"Staged inputs under {staged_root}")

    services_spec = build_services_spec(
        claimed_av=claimed_av,
        claimed_simulator=claimed_simulator,
        claimed_map=claimed_map,
        claimed_scenario=claimed_scenario,
        staged=staged,
    )

    service_manager = _create_service_manager(args.backend, job_id)
    shutdown_state["service_manager"] = service_manager
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

        terminal_verb, terminal_reason = _execute_runner_task(
            runner_spec=runner_spec,
            shutdown_state=shutdown_state,
        )
    except Exception as exc:
        logger.error(f"Executor failed with error: {exc}")
        terminal_verb = "failed"
        terminal_reason = f"{type(exc).__name__}: {str(exc)}"

    finally:
        # Order matters: do cleanup BEFORE the terminal POST so
        # "Stopping Apptainer container ..." / staged-inputs cleanup
        # lines stream live to the manager (the streamer keeps
        # flushing while task_run is still `running`) and end up in
        # the final task_run.log too. Stop the streamer between
        # cleanup and the POST so its final flush carries any tail
        # bytes; once the POST lands, /log/append returns 410.
        try:
            service_manager.stop_all_services()
        except Exception as exc:
            logger.error(f"service_manager stop failed: {exc}")
        # Reclaim TMPDIR: the staged map / scenario / config bytes can
        # be tens of MB per task and the worker's /tmp would otherwise
        # accumulate one stage tree per task across all runs until the
        # node reboots. The manager keeps the canonical copy on
        # BeeGFS, so a re-run just re-stages from scratch.
        # Errors here are non-fatal — log + carry on so a stuck
        # filesystem can't prevent the executor from exiting cleanly.
        if staged_root.exists():
            try:
                shutil.rmtree(staged_root)
                logger.debug(f"Cleaned staged inputs at {staged_root}")
            except Exception as e:
                logger.warning(f"Failed to clean staged inputs at {staged_root}: {e}")
        if log_streamer is not None:
            log_streamer.stop()
        if task_id is not None:
            engine_obj = shutdown_state.get("engine")
            useful = (
                int(getattr(engine_obj, "completed_concrete_runs", 0))
                if engine_obj is not None
                else 0
            )
            try:
                _send_terminal(
                    client=client,
                    task_id=task_id,
                    verb=terminal_verb,
                    reason=terminal_reason,
                    capture=capture,
                    service_manager=service_manager,
                    concrete_scenarios_executed=useful,
                )
            except Exception as exc:
                logger.error(f"Terminal lifecycle POST failed: {exc}")

    logger.debug("Executor finished execution.")


if __name__ == "__main__":
    main()

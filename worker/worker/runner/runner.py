# sv/runner.py
from time import sleep, time
import traceback
from typing import Any, Optional
import logging
from pathlib import Path
import importlib

from worker.runner.av_wrapper import AVWrapper
from worker.runner.utils.sps import ScenarioPack
from worker.runner.sim_wrapper import SimWrapper

RETRY_ROUTE_NOT_FOUND_MAX = 10

logger = logging.getLogger(__name__)


class Runner:
    def __init__(self, spec: dict[str, Any]):
        runtime_spec = spec.get("runtime", {})
        task_spec = spec.get("task", {})
        sim_spec = spec.get("simulator", {})
        av_spec = spec.get("av", {})
        sampler_spec = spec.get("sampler", {})
        scenario_spec = spec.get("scenario", {})
        map_spec = spec.get("map", {})

        # Bridge
        # TODO: default to NoneBridge
        # bridge_spec = {"name": "none", "module_path": "sv.bridge.none:NoneBridge"}

        # TODO: default to defaultMonitor
        monitor_spec = {
            "name": "default",
            "module_path": "sv.monitor.default:defaultMonitor",
            "config_path": "configs/monitor/default.yaml",
            "module_path": "sv.monitor.default:defaultMonitor",
            "config_path": "configs/monitor/default.yaml",
        }

        self._id = task_spec.get("worker_id", "default_worker")

        self._dt_s = runtime_spec.get("dt", None)
        if self._dt_s is None:
            logger.warning("No 'dt' specified in runtime_spec; defaulting to 0.01s")
            self._dt_s = 0.01

        logger.info(f"Runner ID: {self._id}")

        self.output_base = (
            Path(task_spec.get("output_dir", "./outputs")).expanduser().resolve()
        )
        self.output_base.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output base directory set to: {self.output_base}")

        self.sps = ScenarioPack.from_dict(scenario_spec, map_spec)

        try:
            self.sim = SimWrapper(
                sim_spec=sim_spec,
                dt_ns=int(self._dt_s * 1e9),
            )
            # self.sim.init(sim_spec=sim_spec, dt=self._dt_s)
        except Exception as exc:
            logger.error("Simulator initialization failed")
            raise exc

        try:
            self.av = AVWrapper(
                av_spec=av_spec,
                dt_ns=int(self._dt_s * 1e9),
                sps=self.sps,
            )
            # self.av.init(av_spec=av_spec, dt=self._dt_s)
        except Exception as exc:
            logger.error("AV initialization failed")
            raise exc

        # module = importlib.import_module(bridge_spec["module_path"].split(":")[0])
        # bridge_class = getattr(module, bridge_spec["module_path"].split(":")[1])
        # self.bridge = bridge_class(cfg_path=bridge_spec.get("config_path", None))

        # module = importlib.import_module(monitor_spec["module_path"].split(":")[0])
        # monitor_class = getattr(module, monitor_spec["module_path"].split(":")[1])
        # self.monitor = monitor_class(
        #     cfg_path=monitor_spec.get("config_path", None),
        #     plan_name=self._id,
        # )

        if self.sps.param_range_file is not None:
            logger.info("Parameter range file provided: %s", self.sps.param_range_file)
            # param_sampler
            module = importlib.import_module(sampler_spec["module_path"].split(":")[0])
            sampler_class = getattr(module, sampler_spec["module_path"].split(":")[1])
            self.param_sampler = sampler_class(
                param_range_file=self.sps.param_range_file,
                past_results=None,
            )
        else:
            logger.info(
                "No parameter range file provided; seem as testing a concrete scenario; skipping parameter sampler."
            )
            self.param_sampler = None

    def exec(self) -> None:
        """
        Run the scenario(s) according to the provided specifications.
        If a parameter sampler is provided, it will iterate through all parameter combinations;
        otherwise, it will run a single concrete scenario.
        """
        try:
            if self.param_sampler is not None:
                logger.info("Starting parameter sampling execution.")
                total = self.param_sampler.total_permutations()

                logger.info(f"Total parameter combinations: {total}")

                route_not_found_count = 0

                for i in range(total):
                    logger.info(f"Sampling iteration {i+1}/{total}")
                    params = self.param_sampler.next()

                    if params is None:
                        logger.info("Parameter sampling completed.")
                        break

                    logger.info(f"Running scenario with parameters: {params}")
                    try:
                        self.run_concrete(f"iteration_{i+1}", self.sps, params)
                    except RuntimeError as e:
                        if "AV route not found during reset" in str(e):
                            route_not_found_count += 1
                            logger.warning(
                                f"Encountered known issue during scenario execution: {str(e)}. This can be caused by the AV cannot find a valid route for the scenario, which may be due to limitations in the scenario design or the AV's routing capabilities. The current parameter combination will be skipped."
                            )
                            logger.warning(
                                f"RouteNotFoundError count: {route_not_found_count}/{RETRY_ROUTE_NOT_FOUND_MAX}"
                            )
                            if route_not_found_count >= RETRY_ROUTE_NOT_FOUND_MAX:
                                logger.error(
                                    f"Exceeded maximum retries for route not found errors ({RETRY_ROUTE_NOT_FOUND_MAX}). Aborting further execution."
                                )
                                raise RuntimeError(
                                    f"Exceeded maximum retries for route not found errors ({RETRY_ROUTE_NOT_FOUND_MAX}). Aborting further execution."
                                ) from e
                            continue
                        else:
                            logger.error(
                                f"RuntimeError during scenario execution: {str(e)}"
                            )
                            raise e

                    except Exception as exc:
                        logger.error(f"Scenario failed at iteration {i+1}: {exc}")
                        continue
                    else:
                        # Reset route_not_found_count after a successful execution
                        route_not_found_count = 0
            else:
                logger.info("Running a single concrete scenario.")
                try:
                    self.run_concrete("concrete", self.sps)
                except Exception as exc:
                    logger.error(f"Scenario failed: {exc}")
                    raise exc

            logger.info("Runner execution completed.")
        except Exception as exc:
            logger.error(f"Runner execution failed: {exc}")
            raise exc
        finally:
            self.close()

    def run_concrete(
        self,
        output_related: str,
        sps: ScenarioPack,
        params: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Run a single concrete scenario with the given parameters.
        """
        raw_obs = None
        output_path = self.output_base / output_related
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Resetting simulator for scenario '{sps.name}' with map '{sps.map_name}'..."
        )
        try:
            raw_obs = self.sim.reset(output_related, sps, params)
        except Exception as e:
            logger.error(f"Simulator reset failed: {e}")
            raise e

        logger.info("Resetting AV...")
        try:
            ctrl_for_sim = self.av.reset(output_related, sps, raw_obs)
        except Exception as e:
            logger.error(f"AV reset failed: {e}")
            raise e

        dt_s = self._dt_s
        dt_ns = int(dt_s * 1e9)

        use_real_time = False
        if dt_ns <= 0:  # use real-time stepping
            dt_ns = 0
            use_real_time = True
            prev = time()

        sim_time_ns = 0  # Simulation time in nanoseconds
        logger.info("Starting execution loop. using dt_s=%.3f", dt_s)
        try:
            real_start_time_s = time()
            while True:
                loop_start_time = time()
                if self.sim.should_quit():
                    logger.info("Simulator requested to quit.")
                    break
                elif self.av.should_quit():
                    logger.info("AV requested to quit.")
                    break

                if use_real_time:
                    t = time()
                    dt_ns = int((t - prev) * 1e9)
                    prev = t

                raw_obs = self.sim.step(ctrl_for_sim, sim_time_ns)
                ctrl_for_sim = self.av.step(raw_obs, sim_time_ns)
                sim_time_ns += dt_ns

                cur_time_s = time()
                time_use_s = cur_time_s - real_start_time_s

                loop_need_time = time() - loop_start_time
                sleep_time_s = dt_s - loop_need_time
                if sleep_time_s > 0:
                    sleep(sleep_time_s)

                # print(
                #     f"time use = {time_use_s:.2f} s, sim_time = {sim_time_ns / 1e9:.2f} s",
                #     end="\r",
                # )

            sim_time_need = time() - real_start_time_s

        except Exception as e:
            logger.error(f"Error during scenario execution: {e}")
            raise e

        logger.info(
            f"Completed {sim_time_ns / 1e9:.2f} seconds scenario, using {sim_time_need:.2f} sec."
        )
        logger.info("Scenario finished.")

    def close(self):
        try:
            self.av.stop()
        except Exception:
            logger.exception("av.stop() failed")
        try:
            self.sim.stop()
        except Exception:
            logger.exception("sim.stop() failed")

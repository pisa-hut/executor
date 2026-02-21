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

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)


class Runner:
    def __init__(self, spec: dict[str, Any]):

        logging.info("Initializing Runner...")

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

        base = Path(task_spec.get("output_dir", "artifacts")).expanduser().resolve()
        self.output_dir = base / self._id
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory set to: {self.output_dir}")

        self.sps = ScenarioPack.from_dict(scenario_spec, map_spec)

        try:
            self.sim = SimWrapper(
                output_dir=self.output_dir,
                sim_spec=sim_spec,
                dt_ns=int(self._dt_s * 1e9),
            )
            # self.sim.init(sim_spec=sim_spec, dt=self._dt_s)
        except Exception:
            logger.exception("Simulator initialization failed")
            return

        try:
            self.av = AVWrapper(
                output_dir=self.output_dir,
                av_spec=av_spec,
                dt_ns=int(self._dt_s * 1e9),
                sps=self.sps,
            )
            # self.av.init(av_spec=av_spec, dt=self._dt_s)
        except Exception:
            logger.exception("AV initialization failed")
            return

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

                for i in range(total):
                    logger.info(f"Sampling iteration {i+1}/{total}")
                    params = self.param_sampler.next()

                    if params is None:
                        logger.info("Parameter sampling completed.")
                        break

                    logger.info(f"Running scenario with parameters: {params}")
                    cur_output_dir = self.output_dir / f"iteration_{i+1}"
                    cur_output_dir.mkdir(parents=True, exist_ok=True)
                    try:
                        self.run_concrete(cur_output_dir, self.sps, params)
                    except Exception:
                        logger.exception(f"Scenario failed at iteration {i+1}")
                        continue
            else:
                logger.info("Running a single concrete scenario.")
                try:
                    self.run_concrete(self.output_dir, self.sps)
                except Exception:
                    logger.exception("Scenario failed")

            logger.info("Runner execution completed.")

        finally:
            self.close()

    def run_concrete(
        self,
        output_dir: Path,
        sps: ScenarioPack,
        params: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Run a single concrete scenario with the given parameters.
        """
        raw_obs = None

        logger.info(
            f"Resetting simulator for scenario '{sps.name}' with map '{sps.map_name}'..."
        )
        try:
            raw_obs = self.sim.reset(output_dir, sps, params)
        except Exception as e:
            logger.error(f"Simulator reset failed: {e}")
            return

        logger.info("Resetting AV...")
        try:
            ctrl_for_sim = self.av.reset(output_dir, sps, raw_obs)
        except Exception as e:
            logger.error(f"AV reset failed: {e}")
            traceback_str = traceback.format_exc()
            logger.error(f"Stack trace:\n{traceback_str}")
            return

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

                print(
                    f"time use = {time_use_s:.2f} s, sim_time = {sim_time_ns / 1e9:.2f} s",
                    end="\r",
                )

            sim_time_need = time() - real_start_time_s

        except Exception as e:
            logger.error(f"Error during scenario execution: {e}")
            traceback_str = traceback.format_exc()
            logger.error(f"Stack trace:\n{traceback_str}")
            return

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

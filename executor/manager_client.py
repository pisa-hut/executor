import os
import requests
from typing import Any
from loguru import logger


class ManagerClient:
    def __init__(self):
        self.manager_url = os.getenv("MANAGER_URL")
        assert self.manager_url, "MANAGER_URL environment variable must be set"
        self.timeout = int(os.getenv("TIMEOUT", "30"))

        self.avs: dict[str, int] = {}
        self.simulators: dict[str, int] = {}
        self.maps: dict[str, int] = {}
        self.samplers: dict[str, int] = {}

    def _list_entities(self, entity_type: str) -> dict[str, Any]:
        r = requests.get(
            f"{self.manager_url}/{entity_type}",
            timeout=self.timeout,
        )
        r.raise_for_status()

        entities = r.json()
        if not isinstance(entities, list):
            raise ValueError(f"Expected a list of {entity_type}s, got: {entities}")
        return {entity["name"]: entity["id"] for entity in entities}

    def _register_executor(self, info: dict[str, str | int]) -> dict[str, str | int]:
        payload = {
            "job_id": int(info.get("job_id", 0)),
            "node_list": str(info.get("node_list", "unknown")),
            "hostname": str(info.get("hostname", "unknown")),
        }
        r = requests.post(
            f"{self.manager_url}/executor",
            json=payload,
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def _get_id_by_name(self, entity_type: str, name: str | None) -> int | None:
        if name is None:
            return None
        if entity_type == "av":
            return self.avs.get(name)
        elif entity_type == "simulator":
            return self.simulators.get(name)
        elif entity_type == "map":
            return self.maps.get(name)
        elif entity_type == "sampler":
            return self.samplers.get(name)
        else:
            raise ValueError(f"Unknown entity type: {entity_type}")

    def _claim_task_by_id(
        self,
        executor_id: int,
        task_id: int | None = None,
        av_id: int | None = None,
        simulator_id: int | None = None,
        map_id: int | None = None,
        scenario_id: int | None = None,
        sampler_id: int | None = None,
    ) -> dict[str, dict[str, Any]] | None:
        payload = {
            "executor_id": executor_id,
            "task_id": task_id,
            "av_id": av_id,
            "simulator_id": simulator_id,
            "map_id": map_id,
            "scenario_id": scenario_id,
            "sampler_id": sampler_id,
        }
        logger.debug(f"Attempting to claim task with payload: {payload}")
        r = requests.post(
            f"{self.manager_url}/task/claim",
            json=payload,
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def fetch(self) -> None:
        self.maps: dict[str, int] = self._list_entities("map")
        self.avs: dict[str, int] = self._list_entities("av")
        self.simulators: dict[str, int] = self._list_entities("simulator")
        self.samplers: dict[str, int] = self._list_entities("sampler")

    def claim_task_spec(
        self,
        executor_info: dict[str, str | int],
        task_id: int | None = None,
        av_name: str | None = None,
        av_id: int | None = None,
        simulator_name: str | None = None,
        simulator_id: int | None = None,
        map_name: str | None = None,
        scenario_id: int | None = None,
        sampler_name: str | None = None,
    ) -> dict[str, dict[str, Any]] | None:
        """Claim a task from the manager.

        Filters may be passed by name or by id. When both are given for
        the same entity, the id wins (more specific; avoids a redundant
        name → id lookup). The scheduler's bucket-aware loop passes ids
        from `/queue/demand`; older paths that only know names still work.
        """
        executor = self._register_executor(executor_info)
        logger.debug(f"Registered executor with ID: {executor['id']}")
        return self._claim_task_by_id(
            executor_id=int(executor["id"]),
            task_id=task_id,
            map_id=self._get_id_by_name("map", map_name),
            scenario_id=scenario_id,
            av_id=av_id if av_id is not None else self._get_id_by_name("av", av_name),
            simulator_id=(
                simulator_id
                if simulator_id is not None
                else self._get_id_by_name("simulator", simulator_name)
            ),
            sampler_id=self._get_id_by_name("sampler", sampler_name),
        )

    def _concrete_run_body(
        self,
        finished_concrete_runs: int | None,
        aborted_concrete_runs: int | None,
        skipped_concrete_runs: int | None,
    ) -> dict[str, int]:
        """Map count kwargs into a JSON body fragment, omitting any
        field that's `None`. Lets the SIGTERM / init-failure paths send
        no counts and have the manager inherit the prior task_run's
        cumulative snapshot."""
        body: dict[str, int] = {}
        if finished_concrete_runs is not None:
            body["finished_concrete_runs"] = finished_concrete_runs
        if aborted_concrete_runs is not None:
            body["aborted_concrete_runs"] = aborted_concrete_runs
        if skipped_concrete_runs is not None:
            body["skipped_concrete_runs"] = skipped_concrete_runs
        return body

    def create_concrete_runs(self, task_run_id: int, outcomes: list[Any]) -> None:
        if not outcomes:
            return
        rows = []
        for outcome in outcomes:
            rows.append(
                {
                    "concrete_key": getattr(outcome, "concrete_key"),
                    "status": getattr(outcome, "status"),
                    "test_outcome": getattr(outcome, "test_outcome", "unknown"),
                    "reason": getattr(outcome, "reason", None),
                    "stop_condition": getattr(outcome, "stop_condition", None),
                    "params": getattr(outcome, "params", None),
                    "final_sim_time_ms": getattr(outcome, "final_sim_time_ms", None),
                    "wall_time_ms": getattr(outcome, "wall_time_ms", None),
                    "total_steps": getattr(outcome, "total_steps", None),
                }
            )
        logger.info(
            f"Reporting {len(rows)} concrete outcome(s) for task_run {task_run_id}"
        )
        r = requests.post(
            f"{self.manager_url}/task_run/{task_run_id}/concrete_runs",
            json=rows,
            timeout=self.timeout,
        )
        r.raise_for_status()

    def report_progress(
        self,
        task_run_id: int,
        finished_concrete_runs: int,
        aborted_concrete_runs: int,
        skipped_concrete_runs: int,
        expected_concrete_runs: int | None,
    ) -> None:
        """Best-effort mid-run progress ping so the UI can show ongoing
        concrete progress. Failures are swallowed — telemetry must never
        disrupt a running simulation."""
        body = {
            "finished_concrete_runs": finished_concrete_runs,
            "aborted_concrete_runs": aborted_concrete_runs,
            "skipped_concrete_runs": skipped_concrete_runs,
            "expected_concrete_runs": expected_concrete_runs,
        }
        try:
            r = requests.put(
                f"{self.manager_url}/task_run/{task_run_id}/progress",
                json=body,
                timeout=self.timeout,
            )
            r.raise_for_status()
        except Exception as exc:
            logger.debug(f"progress report for task_run {task_run_id} failed: {exc}")

    def task_failed(
        self,
        task_id: int,
        reason: str,
        log: str | None = None,
        finished_concrete_runs: int | None = None,
        aborted_concrete_runs: int | None = None,
        skipped_concrete_runs: int | None = None,
    ):
        logger.info(
            f"Reporting task failure for task ID {task_id} "
            f"(finished={finished_concrete_runs}, "
            f"aborted={aborted_concrete_runs}, "
            f"skipped={skipped_concrete_runs})"
        )
        body = {
            "task_id": task_id,
            "reason": reason,
            "log": log,
            **self._concrete_run_body(
                finished_concrete_runs,
                aborted_concrete_runs,
                skipped_concrete_runs,
            ),
        }
        r = requests.post(
            f"{self.manager_url}/task/failed",
            json=body,
            timeout=self.timeout,
        )
        r.raise_for_status()

    def task_aborted(
        self,
        task_id: int,
        reason: str,
        log: str | None = None,
        finished_concrete_runs: int | None = None,
        aborted_concrete_runs: int | None = None,
        skipped_concrete_runs: int | None = None,
    ):
        logger.info(
            f"Reporting task aborted for task ID {task_id} "
            f"(finished={finished_concrete_runs}, "
            f"aborted={aborted_concrete_runs}, "
            f"skipped={skipped_concrete_runs})"
        )
        body = {
            "task_id": task_id,
            "reason": reason,
            "log": log,
            **self._concrete_run_body(
                finished_concrete_runs,
                aborted_concrete_runs,
                skipped_concrete_runs,
            ),
        }
        r = requests.post(
            f"{self.manager_url}/task/aborted",
            json=body,
            timeout=self.timeout,
        )
        r.raise_for_status()

    def task_succeeded(
        self,
        task_id: int,
        log: str | None = None,
        finished_concrete_runs: int | None = None,
        aborted_concrete_runs: int | None = None,
        skipped_concrete_runs: int | None = None,
    ):
        logger.info(
            f"Reporting task success for task ID {task_id} "
            f"(finished={finished_concrete_runs}, "
            f"aborted={aborted_concrete_runs}, "
            f"skipped={skipped_concrete_runs})"
        )
        body = {
            "task_id": task_id,
            "log": log,
            **self._concrete_run_body(
                finished_concrete_runs,
                aborted_concrete_runs,
                skipped_concrete_runs,
            ),
        }
        r = requests.post(
            f"{self.manager_url}/task/succeeded",
            json=body,
            timeout=self.timeout,
        )
        r.raise_for_status()

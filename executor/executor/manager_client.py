import logging
import os
import requests
from typing import Any


logger = logging.getLogger(__name__)


class ManagerClient:
    def __init__(self):
        self.manager_url = os.getenv("MANAGER_URL")
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
            "array_id": int(info.get("array_id", 0)),
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
        av_id: int | None = None,
        simulator_id: int | None = None,
        map_id: int | None = None,
        scenario_id: int | None = None,
        sampler_id: int | None = None,
    ) -> dict[str, dict[str, Any]] | None:
        payload = {
            "executor_id": executor_id,
            "av_id": av_id,
            "simulator_id": simulator_id,
            "map_id": map_id,
            "scenario_id": scenario_id,
            "sampler_id": sampler_id,
        }
        logger.info(f"Attempting to claim task with payload: {payload}")
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
        av_name: str | None = None,
        simulator_name: str | None = None,
        map_name: str | None = None,
        scenario_id: int | None = None,
        sampler_name: str | None = None,
    ) -> dict[str, dict[str, Any]] | None:
        executor = self._register_executor(executor_info)
        logger.info("Registered executor with ID: %s", executor["id"])
        return self._claim_task_by_id(
            executor_id=int(executor["id"]),
            map_id=self._get_id_by_name("map", map_name),
            scenario_id=scenario_id,
            av_id=self._get_id_by_name("av", av_name),
            simulator_id=self._get_id_by_name("simulator", simulator_name),
            sampler_id=self._get_id_by_name("sampler", sampler_name),
        )

    # Backward-compatible alias.
    def _register_worker(self, info: dict[str, str | int]) -> dict[str, str | int]:
        return self._register_executor(info)

    def task_failed(self, task_id: int, reason: str):
        logger.info(f"Reporting task failure for task ID {task_id}")
        r = requests.post(
            f"{self.manager_url}/task/failed",
            json={
                "task_id": task_id,
                "reason": reason,
            },
            timeout=self.timeout,
        )
        r.raise_for_status()

    def task_invalid(self, task_id: int, reason: str):
        logger.info(f"Reporting task invalid for task ID {task_id}")
        r = requests.post(
            f"{self.manager_url}/task/invalid",
            json={
                "task_id": task_id,
                "reason": reason,
            },
            timeout=self.timeout,
        )
        r.raise_for_status()

    def task_succeeded(self, task_id: int):
        logger.info(f"Reporting task success for task ID {task_id}")
        r = requests.post(
            f"{self.manager_url}/task/succeeded",
            json={
                "task_id": task_id,
            },
            timeout=self.timeout,
        )
        r.raise_for_status()

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
        self.samplers: dict[str, int] = {}

    def _list_entities(self, entity_type: str) -> list[dict[str, Any]]:
        r = requests.get(
            f"{self.manager_url}/{entity_type}",
            timeout=self.timeout,
        )
        r.raise_for_status()
        # assume the response is a list of entities, each with 'id' and 'name'

        entities = r.json()
        if not isinstance(entities, list):
            raise ValueError(f"Expected a list of {entity_type}s, got: {entities}")
        return {entity["name"]: entity["id"] for entity in entities}

    def _register_worker(self, info: dict[str, str | int]) -> dict[str, str | int]:
        r = requests.post(
            f"{self.manager_url}/worker",
            json=info,
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
        elif entity_type == "sampler":
            return self.samplers.get(name)
        else:
            raise ValueError(f"Unknown entity type: {entity_type}")

    def _claim_task_by_id(
        self,
        worker_id: int,
        plan_id: int | None = None,
        av_id: int | None = None,
        simulator_id: int | None = None,
        sampler_id: int | None = None,
    ) -> dict[str, dict[str, Any]] | None:
        payload = {
            "worker_id": worker_id,
            "plan_id": plan_id,
            "av_id": av_id,
            "simulator_id": simulator_id,
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
        self.avs: dict[str, int] = self._list_entities("av")
        self.simulators: dict[str, int] = self._list_entities("simulator")
        self.samplers: dict[str, int] = self._list_entities("sampler")

    def claim_task_spec(
        self,
        slurm_info: dict[str, str | int],
        plan_id: int | None = None,
        av_name: str | None = None,
        simulator_name: str | None = None,
        sampler_name: str | None = None,
    ) -> dict[str, dict[str, Any]] | None:
        worker_info = self._register_worker(slurm_info)
        logger.info(f"Registered worker with ID: {worker_info['id']}")
        return self._claim_task_by_id(
            worker_id=worker_info["id"],
            plan_id=plan_id,
            av_id=self._get_id_by_name("av", av_name),
            simulator_id=self._get_id_by_name("simulator", simulator_name),
            sampler_id=self._get_id_by_name("sampler", sampler_name),
        )

    def task_failed(self, task_id: int, reason: str):
        logger.info(
            f"Reporting task failure for task ID {task_id} with reason: {reason}"
        )
        r = requests.post(
            f"{self.manager_url}/task/failed",
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

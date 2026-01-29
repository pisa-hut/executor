import requests
import os


class ManagerClient:
    def __init__(self):
        self.manager_url = os.getenv("MANAGER_URL")
        self.timeout = int(os.getenv("TIMEOUT", "30"))

    def register_worker(self, info: dict[str, str | int]) -> dict[str, str | int]:
        r = requests.post(
            f"{self.manager_url}/worker",
            json=info,
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def claim_task(self, worker_id: int) -> dict[str, dict[str, str | int]] | None:
        r = requests.post(
            f"{self.manager_url}/task/claim",
            json={"worker_id": worker_id},
            timeout=self.timeout,
        )
        if r.status_code == 204:
            return None
        r.raise_for_status()
        return r.json()

    def complete_task(self, task_id: int):
        r = requests.post(
            f"{self.manager_url}/task/complete",
            json={
                "task_id": task_id,
            },
            timeout=self.timeout,
        )
        r.raise_for_status()

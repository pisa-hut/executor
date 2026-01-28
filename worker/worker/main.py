import dotenv
import time

from worker.manager_client import ManagerClient
from worker.system import collect_worker_identity


dotenv.load_dotenv()


def main():
    client = ManagerClient()
    worker_info = client.register_worker(collect_worker_identity())
    print(f"Registered worker with ID: {worker_info['id']}")

    assert isinstance(worker_info["id"], int)
    task = client.claim_task(worker_info["id"])
    if task is None:
        print("No tasks available to claim.")
        return

    print(task)

    print(f"Claimed task: {task['task']['id']}")
    time.sleep(5)  # Simulate doing work
    print(f"Completed task: {task['task']['id']}")
    client.complete_task(task["task"]["id"])


if __name__ == "__main__":
    main()

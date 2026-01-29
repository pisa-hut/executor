import dotenv

from worker.manager_client import ManagerClient
from worker.system import collect_worker_identity
from sv.runner import Runner


dotenv.load_dotenv()


def main():
    client = ManagerClient()
    worker_info = client.register_worker(collect_worker_identity())
    print(f"Registered worker with ID: {worker_info['id']}")

    assert isinstance(worker_info["id"], int)
    response = client.claim_task(worker_info["id"])
    if response is None:
        print("No tasks available to claim.")
        return
    assert isinstance(response, dict)

    task_id: int = int(task["task"]["id"])

    print(f"Claimed task: {task_id}")
    # runner = Runner(
    #     sim_spec=task["simulator"],
    #     av_spec=task["av"],
    #     sampler_spec=task["sampler"],
    #     scenario_spec=task["scenario"],
    #     map_spec=task["map"],
    # )
    print(f"Completed task: {task_id}")
    client.complete_task(task_id)


if __name__ == "__main__":
    main()

import dotenv
from pprint import pprint
from typing import Any

from sv.runner import Runner

from worker.manager_client import ManagerClient
from worker.system import collect_worker_identity

dotenv.load_dotenv()


def main():
    print("Starting worker...")
    client = ManagerClient()
    slurm_info = collect_worker_identity()
    worker_info = client.register_worker(slurm_info)
    print(f"Registered worker with ID: {worker_info['id']}")
    job_id = slurm_info.get("job_id", "unknown")

    assert isinstance(worker_info["id"], int)
    response = client.claim_task(worker_info["id"])
    if response is None:
        print("No tasks available to claim.")
        return
    assert isinstance(response, dict)

    spec: dict[str, dict[str, Any]] = response
    task_id = spec["task"].pop("id", None)
    print(f"Claimed task ID: {task_id}")

    spec["task"]["output_dir"] = f"./output_dir"
    spec["task"]["job_id"] = str(job_id)
    spec["runtime"] = {"dt": 0.01}
    assert isinstance(spec["scenario"], dict)
    spec["scenario"]["ego"] = {
        "target_speed": 50,
        "spawn": {
            "type": "LanePosition",
            "value": [0, -2, 300, 0],
            "speed": 50,
        },
        "goal": {
            "type": "LanePosition",
            "value": [0, -4, 700, 0],
        },
    }
    spec["scenario"]["param_path"] = None

    pprint(spec)

    print(f"Claimed task: {task_id}")
    runner = Runner(
        task_spec=spec["task"],
        runtime_spec=spec["runtime"],
        sim_spec=spec["simulator"],
        av_spec=spec["av"],
        sampler_spec=spec["sampler"],
        scenario_spec=spec["scenario"],
        map_spec=spec["map"],
    )
    runner.exec()
    print(f"Completed task: {task_id}")
    client.complete_task(task_id)


if __name__ == "__main__":
    main()

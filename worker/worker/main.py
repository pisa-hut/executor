import dotenv
from pprint import pprint
from typing import Any

from worker.manager_client import ManagerClient
from worker.system import collect_worker_identity
from worker.apptainer_service import ApptainerServiceManager

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
    spec["simulator"] = {
        "name": "carla",
    }

    pprint(spec)
    print(f"Claimed task: {task_id}")

    # Initialize the Apptainer service manager
    service_manager = ApptainerServiceManager()

    # Get component names and start appropriate Apptainer services
    simulator_name = spec.get("simulator", {}).get("name", "unknown")
    av_name = spec.get("av", {}).get("name", "unknown")

    print(f"Simulator: {simulator_name}")
    print(f"AV: {av_name}")

    # Start simulator service and get URL
    simulator_service_info = service_manager.start_simulator_service(simulator_name)
    if simulator_service_info:
        spec["simulator"]["service_info"] = simulator_service_info
        print(f"Simulator service available at: {simulator_service_info['url']}")

    # Start AV service and get URL (if needed in the future)
    av_service_info = service_manager.start_av_service(av_name)
    if av_service_info:
        spec["av"]["service_info"] = av_service_info
        print(f"AV service available at: {av_service_info['url']}")

    try:
        # Run the scenario runner in Apptainer container
        print("Starting scenario runner...")
        exit_code = service_manager.run_runner(
            spec, task_id=task_id, worker_id=worker_info["id"]
        )

        if exit_code == 0:
            print(f"Completed task: {task_id}")
            client.complete_task(task_id)
        else:
            print(f"Task failed with exit code: {exit_code}")
            # TODO: Report task failure to manager
    finally:
        # Always stop all Apptainer services, even if the task fails
        print("Cleaning up Apptainer services...")
        service_manager.stop_all_services()


if __name__ == "__main__":
    main()

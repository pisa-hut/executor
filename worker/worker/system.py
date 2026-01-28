import os
import socket
import uuid


def collect_worker_identity() -> dict[str, str | int]:
    slurm_job_id = int(os.getenv("SLURM_JOB_ID", "0"))
    slurm_node_list = os.getenv("SLURM_NODELIST", "unknown")
    slurm_cluster_name = os.getenv("SLURM_CLUSTER_NAME", "unknown")
    hostname = socket.gethostname()
    return {
        "worker_uuid": str(uuid.uuid4()),
        "hostname": hostname,
        "job_id": slurm_job_id,
        "node_list": slurm_node_list,
        "cluster_name": slurm_cluster_name,
    }

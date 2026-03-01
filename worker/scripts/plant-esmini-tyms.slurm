#!/bin/bash
#SBATCH --job-name=pt/es-tyms
#SBATCH --output=outputs/stdout/job_%j
#SBATCH --error=outputs/stderr/job_%j
#SBATCH --time=01:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=12G
#SBATCH --gpus=1

echo "=== SLURM TEST JOB ==="
echo "Cluster:       $SLURM_CLUSTER_NAME"
echo "Job ID:        $SLURM_JOB_ID"
echo "Partition:     $SLURM_JOB_PARTITION"
echo "Job Name:      $SLURM_JOB_NAME"
echo "Node List:     $SLURM_NODELIST"
echo "Submit Dir:    $SLURM_SUBMIT_DIR"
echo "Start Time:    $(date)"
echo

echo "Hostname:      $(hostname)"
echo "CPU Info:"
echo

echo "Start Time:    $(date)"

GPU_ID=$SLURM_JOB_GPUS

if [[ -z $GPU_ID ]]; then
	echo "not set"
	env
	exit 1
fi

function skip_gpu () {
	if [[ "$1" -eq "$2" ]]; then
		echo "skip gpu $1"
		sleep 10000
		exit 1
	else
		echo "run on gpu $1"
	fi
}

# skip_gpu $GPU_ID 5
# skip_gpu $GPU_ID 5


CARLA_ROOT=${HOME}/carla-0.9.16/
CARLA_PORT=$((GPU_ID*10 + 2000))

# ${CARLA_ROOT}/CarlaUE4.sh -renderoffscreen -nosound -quality-level=Low -fps=1 -graphicsadapter=$GPU_ID -carla-port=$CARLA_PORT &
# CARLA_PID=$!
# echo "Carla server runing on PID: ${CARLA_PID}"

# sleep 20
echo "Starting runner"


CARLA_PORT=$CARLA_PORT uv run -m worker --av plant --simulator esmini --map tyms

if [ $? -ne 0 ]; then
    echo "Worker execution failed."
    exit 1
fi

# kill -2 ${CARLA_PID}
# kill -9 ${CARLA_PID}

echo "End Time:      $(date)"

#!/bin/bash
#SBATCH --job-name=worker
#SBATCH --output=logs/out.%j
#SBATCH --error=logs/err.%j
#SBATCH --time=00:10:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=40G

echo "=== SLURM TEST JOB ==="
echo "Cluster:       $SLURM_CLUSTER_NAME"
echo "Job ID:        $SLURM_JOB_ID"
echo "Array Job ID:  $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Partition:     $SLURM_JOB_PARTITION"
echo "Job Name:      $SLURM_JOB_NAME"
echo "Node List:     $SLURM_NODELIST"
echo "Submit Dir:    $SLURM_SUBMIT_DIR"
echo "Start Time:    $(date)"
echo

echo "Hostname:      $(hostname)"
echo "CPU Info:"
echo

echo "Memory Info:"
free -h || true
echo

echo "Running a quick workload..."
apptainer exec --writable-tmpfs worker.sif bash scripts/entry.bash
if [ $? -ne 0 ]; then
    echo "Worker execution failed."
    exit 1
fi

echo "End Time:      $(date)"

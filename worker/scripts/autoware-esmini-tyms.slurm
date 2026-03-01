#!/bin/bash
#SBATCH --job-name=aw/es-tyms
#SBATCH --output=outputs/stdout/job_%j
#SBATCH --error=outputs/stderr/job_%j
#SBATCH --time=01:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=4G

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
uv run -m worker --av autoware --simulator esmini --map tyms
if [ $? -ne 0 ]; then
    echo "Worker execution failed."
    exit 1
fi

echo "End Time:      $(date)"

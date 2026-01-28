#!/bin/bash
#SBATCH --job-name=slurm_test
#SBATCH --output=logs/slurm_test_%j.out
#SBATCH --error=logs/slurm_test_%j.err
#SBATCH --time=00:01:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G

echo "=== SLURM TEST JOB ==="
echo "Job ID:        $SLURM_JOB_ID"
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
uv run -m worker
if [ $? -ne 0 ]; then
    echo "Worker execution failed."
    exit 1
fi

echo "End Time:      $(date)"

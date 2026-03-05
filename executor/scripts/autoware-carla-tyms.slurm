#!/bin/bash
#SBATCH --job-name=aw/ca-tyms
#SBATCH --output=outputs/stdout/job_%j
#SBATCH --error=outputs/stderr/job_%j
#SBATCH --time=01:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=12G

source "$SLURM_SUBMIT_DIR/scripts/utils.sh"
header

echo "Starting executor"
uv run -m executor --av autoware --simulator carla --map tyms

if [ $? -ne 0 ]; then
    echo "Executor execution failed."
    exit 1
fi

echo "End Time:      $(date)"

#!/bin/bash
#SBATCH --job-name=all
#SBATCH --array=1
#SBATCH --output=outputs/stdout/job_%A_%a
#SBATCH --error=outputs/stderr/job_%A_%a
#SBATCH --time=01:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --mem=12G

source "$SLURM_SUBMIT_DIR/scripts/utils.sh"
header


echo Args: $@
echo "Starting executor"
uv run -m executor $@

if [ $? -ne 0 ]; then
    echo "Executor execution failed."
    exit 1
fi

echo "End Time:      $(date)"

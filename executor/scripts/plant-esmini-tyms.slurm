#!/bin/bash
#SBATCH --job-name=pt/es-tyms
#SBATCH --output=outputs/stdout/job_%j
#SBATCH --error=outputs/stderr/job_%j
#SBATCH --time=01:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=12G

source $(dirname "$0")/utils.sh
header

echo "Starting executor"
uv run -m executor --av plant --simulator esmini --map tyms

if [ $? -ne 0 ]; then
    echo "Executor execution failed."
    exit 1
fi

echo "End Time:      $(date)"

#!/bin/bash
#SBATCH --job-name=all
#SBATCH --array=1
#SBATCH --output=outputs/stdout/job_%A_%a
#SBATCH --error=outputs/stderr/job_%A_%a
#SBATCH --time=01:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --mem=12G
# Deliver SIGTERM 60 s before the time-limit SIGKILL so the executor's
# signal handler can report the task as failed, flush its log stream, and
# stop running containers. Without this the task_run row stays `running`
# forever until a server-side reaper cleans it up.
#SBATCH --signal=TERM@60

source "$SLURM_SUBMIT_DIR/scripts/utils.sh"
prologue

CMD="uv run -m executor $@"
echo "Executing command: $CMD"
uv run -m executor $@

if [ $? -ne 0 ]; then
    echo "Executor execution failed."
    exit 1
fi

epilogue

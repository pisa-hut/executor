# Scenario-Queue Executor

This is the executor for the PISA Scenario-Queue project. It is responsible for executing the logical scenarios and reporting the results back to the manager.

## Usage

### Configuration

Copy the `.env.example` file to `.env` and fill in the required environment variables.

### Running the Executor in SLURM

To run the executor in a SLURM cluster, use the following command:

```bash
sbatch scripts/xxx.slurm
```

Choose the appropriate SLURM script based on your requirements.

# Installation

## Requirements

- Python 3.10 or later
- Access to a Slurm cluster (for real job submission; `--dry-run` works without Slurm)
- The processing tools called by each script (Apptainer containers, FreeSurfer, etc.) must be available on the cluster

## Install from source

```bash
git clone https://github.com/GalKepler/snbb_scheduler.git
cd snbb_scheduler
pip install -e ".[dev]"
```

The `[dev]` extra installs pytest and coverage tools for running tests.

## Install docs dependencies

If you want to build or serve this documentation locally:

```bash
pip install -e ".[docs]"
mkdocs serve          # live preview at http://127.0.0.1:8000
mkdocs build --strict # build static site into site/
```

## Verify installation

```bash
snbb-scheduler --help
```

Expected output:
```
Usage: snbb-scheduler [OPTIONS] COMMAND [ARGS]...

  snbb-scheduler: rule-based scheduler for the SNBB neuroimaging pipeline.

Options:
  --config PATH        Path to YAML config file. Uses built-in defaults if omitted.
  --slurm-mem MEM      Memory limit for Slurm jobs (e.g. 32G). Overrides config file.
  --slurm-cpus N       CPUs per task for Slurm jobs. Overrides config file.
  --slurm-log-dir DIR  Directory for Slurm stdout/stderr logs. Overrides config file.
  --help               Show this message and exit.

Commands:
  manifest  Show the current task manifest without submitting.
  monitor   Poll sacct for in-flight job statuses and update the state file.
  retry     Remove failed state entries so they are retried on the next run.
  run       Discover sessions, evaluate rules, and submit jobs to Slurm.
  status    Show the current job state (pending/running/complete/failed).
```

## Run the tests

```bash
pytest
pytest --cov=snbb_scheduler   # with coverage report
```

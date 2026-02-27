# CLI Overview

`snbb-scheduler` is invoked as:

```bash
snbb-scheduler [GLOBAL OPTIONS] COMMAND [COMMAND OPTIONS]
```

## Global options

These options apply to all commands and are specified **before** the command name:

| Option | Description |
|---|---|
| `--config PATH` | Path to YAML config file. Uses built-in defaults if omitted. |
| `--slurm-mem MEM` | Memory limit for Slurm jobs (e.g. `32G`). Overrides config file. |
| `--slurm-cpus N` | CPUs per task for Slurm jobs. Overrides config file. |
| `--slurm-log-dir DIR` | Directory for Slurm stdout/stderr logs. Overrides config file. |

## Commands

| Command | Description |
|---|---|
| [`run`](run.md) | Discover sessions, evaluate rules, and submit jobs to Slurm |
| [`manifest`](manifest.md) | Show the pending task table without submitting |
| [`status`](status.md) | Show the full state file with current job statuses |
| [`monitor`](monitor.md) | Poll sacct and update job statuses in the state file |
| [`retry`](retry.md) | Clear failed entries so they are re-submitted on the next run |

## Examples

```bash
# Dry run — see what would be submitted
snbb-scheduler --config /etc/snbb/config.yaml run --dry-run

# Submit real jobs
snbb-scheduler --config /etc/snbb/config.yaml run

# Submit with more memory than the config specifies
snbb-scheduler --config /etc/snbb/config.yaml --slurm-mem 64G run

# Check job statuses
snbb-scheduler --config /etc/snbb/config.yaml status

# Update statuses from sacct
snbb-scheduler --config /etc/snbb/config.yaml monitor

# Retry all failed bids jobs for one subject
snbb-scheduler --config /etc/snbb/config.yaml retry --procedure bids --subject sub-0002
```

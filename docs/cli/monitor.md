# `monitor`

Poll `sacct` for in-flight job statuses and update the state file.

```bash
snbb-scheduler --config CONFIG monitor
```

## What it does

1. Loads the current state file
2. Finds all jobs with `status=pending` or `status=running`
3. Queries `sacct` for their current Slurm states
4. Maps Slurm states → scheduler statuses (see table below)
5. Runs `reconcile_with_filesystem` — marks jobs as `complete` if their output exists on disk even if sacct no longer tracks them
6. Saves the updated state file
7. Prints a transition count and summary table

## Example output

```
Updated 3 job status(es).
    procedure    status  count
         bids  complete     12
     bids_post  complete      8
     bids_post   running      4
      qsiprep    running      2
   freesurfer    failed       1
```

## Slurm state mapping

| Slurm state | Scheduler status |
|---|---|
| `PENDING` | `pending` |
| `RUNNING` | `running` |
| `COMPLETED` | `complete` |
| `FAILED` | `failed` |
| `TIMEOUT` | `failed` |
| `CANCELLED` | `failed` |
| `OUT_OF_MEMORY` | `failed` |
| `NODE_FAIL` | `failed` |

## Automatic monitoring

`snbb-scheduler run` automatically runs the monitor step before submission (unless `--skip-monitor` is passed). Use the standalone `monitor` command to update statuses without triggering a new submission pass.

## When sacct is unavailable

If `sacct` is not found or returns an error, the command logs a warning and falls back to filesystem reconciliation only. No exception is raised — statuses remain as-is.

## Notes

- Events are recorded to the audit log for every status transition
- `monitor` is safe to run at any time — it never submits new jobs
- See [Monitoring Jobs guide](../guides/monitoring-jobs.md) for recommended monitoring workflows

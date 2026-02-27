# `run`

Discover sessions, evaluate rules, and submit pending jobs to Slurm.

```bash
snbb-scheduler --config CONFIG run [OPTIONS]
```

## Options

| Option | Description |
|---|---|
| `--dry-run` | Print what would be submitted without actually calling sbatch |
| `--force` | Re-queue all procedures regardless of completion or in-flight status |
| `--procedure NAME` | Combined with `--force`: limit forced re-queuing to one procedure |
| `--skip-monitor` | Skip the automatic sacct status update that runs before submission |

## What it does

1. **Discover** sessions from `dicom_root` (or `sessions_file`)
2. **Evaluate** rules to find procedures that need to run
3. **Monitor** (unless `--skip-monitor`): poll sacct + reconcile filesystem to update in-flight statuses
4. **Filter** tasks that are already `pending` or `running`
5. **Submit** each remaining task via `sbatch`
6. **Save** the new state rows to the state file

## Examples

```bash
# Safe preview
snbb-scheduler --config config.yaml run --dry-run

# Real run
snbb-scheduler --config config.yaml run

# Force re-run of all qsiprep jobs (even if already complete)
snbb-scheduler --config config.yaml run --force --procedure qsiprep

# Force re-run of everything
snbb-scheduler --config config.yaml run --force

# Run without the pre-submission sacct poll
snbb-scheduler --config config.yaml run --skip-monitor
```

## Dry-run output

```
Discovering sessions…
  Found 47 session(s).
  12 task(s) need processing.
  10 task(s) after filtering in-flight jobs.
[DRY RUN] Would submit: sbatch --partition=debug --account=snbb --job-name=qsiprep_sub-0031 snbb_run_qsiprep.sh sub-0031
[DRY RUN] Would submit: sbatch --partition=debug --account=snbb --job-name=bids_sub-0044_ses-202411010600 snbb_run_bids.sh sub-0044 ses-202411010600 /data/snbb/dicom/sub-0044/ses-202411010600
...
[DRY RUN] Would submit 10 job(s).
```

## Real-run output

```
Discovering sessions…
  Found 47 session(s).
  12 task(s) need processing.
  10 task(s) after filtering in-flight jobs.
Submitting: sbatch --partition=debug --account=snbb --job-name=qsiprep_sub-0031 snbb_run_qsiprep.sh sub-0031
Submitting: sbatch --partition=debug --account=snbb --job-name=bids_sub-0044_ses-202411010600 snbb_run_bids.sh sub-0044 ses-202411010600 /data/snbb/dicom/sub-0044/ses-202411010600
...
Submitted 10 job(s). State saved to /data/snbb/.scheduler_state.parquet.
```

## Notes

- If `sbatch` is not available or exits non-zero, an exception is raised and the job is not recorded in the state file.
- The `--force` flag bypasses both the completion check and the in-flight filter. Use with care — it will submit duplicate jobs if the previous jobs are still running.
- See [Forcing a Rerun](../guides/forcing-rerun.md) for recommended force workflows.

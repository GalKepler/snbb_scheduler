# `status`

Show the current job state — all submitted jobs and their statuses.

```bash
snbb-scheduler --config CONFIG status
```

## What it shows

Reads the state file and prints:

1. A **summary table** grouped by procedure and status with counts
2. The **full details table** with one row per submitted job

When `slurm_log_dir` is configured, a `log_path` column is added showing the expected `.out` log file path for each job.

## Example output

```
Summary:
    procedure    status  count
         bids  complete     12
     bids_post  complete     10
     bids_post   running      2
      qsiprep  complete      8
      qsiprep   pending      1
   freesurfer  complete      7
   freesurfer   running      1

    subject                session   procedure    status           submitted_at  job_id
   sub-0001  ses-202407110849        bids       complete  2024-11-01 06:00:00   10234
   sub-0001  ses-202407110849        bids_post  complete  2024-11-01 06:00:00   10235
   sub-0001                          qsiprep    complete  2024-11-02 06:00:00   10891
   sub-0002  ses-202407110849        bids       complete  2024-11-01 06:00:00   10236
   sub-0002  ses-202407110849        bids_post  running   2024-11-03 06:00:00   11042
   sub-0003  ses-202410100845        bids       complete  2024-11-01 06:00:00   10237
   sub-0003  ses-202410100845        bids_post   failed   2024-11-01 06:00:00   10238
```

## With log paths

When `slurm_log_dir` is set in config, a `log_path` column appears:

```
   subject   procedure    status  job_id  log_path
  sub-0001    qsiprep    running   10891  /data/snbb/logs/slurm/qsiprep/qsiprep_sub-0001_10891.out
```

## Notes

- `status` reads the state file as-is — it does not poll Slurm. Use `monitor` to get fresh statuses from sacct.
- To clear failed entries, use `retry`.
- The state file can also be read directly with pandas: `pd.read_parquet(config.state_file)`

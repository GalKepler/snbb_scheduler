# `session-status`

Show per-session status with output paths or log file locations.

```bash
snbb-scheduler --config CONFIG session-status [OPTIONS]
```

## Options

| Option | Description |
|---|---|
| `--format {table,csv}` | Output format (default: `table`) |
| `--subject SUBJECT` | Filter to a single subject (e.g. `sub-0001`) |
| `--procedure PROCEDURE` | Show only this procedure column |

## What it shows

A table with one row per session and one column per procedure. Each cell shows:

| Priority | Value shown | Meaning |
|---|---|---|
| 1 | Output path | Procedure output exists on disk |
| 2 | Log file path | Output missing, but a state entry with a job ID exists and `slurm_log_dir` is configured |
| 3 | Status string | Output missing, state entry exists, but no log dir configured |
| 4 | `-` | No state entry at all |

For **subject-scoped** procedures (e.g. `freesurfer`), all sessions of the same subject show the same value.

## Example output

```
   subject                session  bids                                    qsiprep                              freesurfer
  sub-0001  ses-202407110849       /data/snbb/bids/sub-0001/ses-20240…    /data/snbb/logs/slurm/qsiprep/…      /data/snbb/derivatives/freesurfer/sub-0001
  sub-0001  ses-202410100845       /data/snbb/bids/sub-0001/ses-20241…    -                                    /data/snbb/derivatives/freesurfer/sub-0001
  sub-0002  ses-202407110849       failed                                  -                                    -
```

## CSV output

```bash
snbb-scheduler --config config.yaml session-status --format csv
```

Outputs comma-separated values suitable for piping or spreadsheet import.

## Filtering

```bash
# Show only one subject
snbb-scheduler --config config.yaml session-status --subject sub-0001

# Show only the qsiprep column
snbb-scheduler --config config.yaml session-status --procedure qsiprep

# Combine filters
snbb-scheduler --config config.yaml session-status --subject sub-0001 --procedure qsiprep
```

## Notes

- This command discovers sessions from `dicom_root` (or `sessions_file`) and cross-references with the state file.
- Unlike `status`, which shows one row per submitted job, `session-status` shows one row per session with all procedures as columns — making it easy to see overall progress at a glance.
- Use `monitor` first to get fresh statuses from sacct before checking session status.

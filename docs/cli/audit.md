# `audit`

Validate pipeline outputs, analyse Slurm logs, and generate audit reports across all sessions.

```bash
snbb-scheduler --config CONFIG audit [OPTIONS]
```

---

## What it does

1. Discovers all sessions (same logic as `run`)
2. Loads the current state file
3. For each session, checks DICOM source data and validates procedure outputs against their completion markers
4. Locates Slurm log files and scans them for known error patterns (OOM, timeout, segfaults, etc.)
5. Detects jobs that have been stuck in `pending` or `running` beyond the stale threshold
6. Computes a health score (0–1) per session and aggregates per-procedure statistics
7. Renders and optionally saves or emails the report

---

## Options

| Option | Description |
|---|---|
| `--session SUB/SES` | Audit a single session, e.g. `sub-0001/ses-01` |
| `--subject SUBJECT` | Audit all sessions for one subject |
| `--procedure NAME` | Show only one procedure in the output |
| `--format FORMAT` | Output format: `table` (default), `markdown`, `html`, `json` |
| `--output PATH` | Write the report to a file instead of stdout |
| `--email` | Send the report via email (requires `audit.email_recipients` in config) |
| `--dicom-only` | Skip procedure checks; validate DICOM source data only |
| `--logs-only` | Show log analysis only; suppress file-check detail |
| `--history` | Include a trend summary compared to the previous saved report |

---

## Examples

```bash
# Full audit, table output
snbb-scheduler --config config.yaml audit

# Single session
snbb-scheduler --config config.yaml audit --session sub-0001/ses-01

# All sessions for one subject
snbb-scheduler --config config.yaml audit --subject sub-0001

# Procedure-level view
snbb-scheduler --config config.yaml audit --procedure qsiprep --format markdown

# HTML report saved to file, then emailed
snbb-scheduler --config config.yaml audit --format html --output report.html --email

# DICOM validation only
snbb-scheduler --config config.yaml audit --dicom-only

# Log analysis for one procedure
snbb-scheduler --config config.yaml audit --logs-only --procedure bids

# Include trend vs previous run
snbb-scheduler --config config.yaml audit --history
```

---

## Report sections

### Executive Summary

Total session count and health distribution:

| Band | Condition |
|---|---|
| Healthy | health score ≥ 80 % |
| Degraded | health score 40–80 % |
| Critical | health score < 40 % |

### DICOM Source Issues

Sessions where the DICOM directory is missing or has fewer files than `audit.dicom_min_files`. Use `--dicom-only` to run this check in isolation.

### Stale Jobs

Jobs that have been in `pending` or `running` for longer than `audit.stale_job_threshold_hours` (default: 168 h = 7 days). These may have been silently cancelled by Slurm and need a `retry`.

### Procedure Status Matrix

A grid of session × procedure with one-character status icons:

| Icon | Status |
|---|---|
| `✓` | complete |
| `~` | incomplete (some markers found) |
| `✗` | failed |
| `-` | not started |
| `…` | pending |
| `↻` | running |

### Procedure Summaries

Per-procedure counts (complete / incomplete / failed / not started / stale) and the most common error patterns found in Slurm logs.

### Log Analysis

Top error matches across all scanned log files, showing the procedure, file, line number, and matched text.

---

## Health score

Each session's health score is the fraction of procedures that are `complete`:

```
health_score = completed_procedures / total_procedures
```

A score of `1.0` means all procedures have verified output on disk.

---

## Report history and trends

When `audit.report_dir` is set, each run automatically saves a JSON copy of the report:

```
<report_dir>/audit_YYYYMMDD_HHMMSS.json
```

Run with `--history` to compare the current audit against the most recent saved report. The trend summary shows:

- Average health score change
- Number of newly completed procedures
- Number of newly failed procedures
- Sessions added or removed since the previous run

---

## Log pattern detection

The auditor scans `.out` and `.err` files under `slurm_log_dir/<procedure>/` and matches against the following built-in patterns:

| Pattern | Severity | What it matches |
|---|---|---|
| `oom` | error | Out-of-memory kills (`Killed process`, `OUT_OF_MEMORY`) |
| `timeout` | error | `DUE TO TIME LIMIT`, `TIMEOUT` |
| `container_error` | error | Apptainer / Singularity fatal errors |
| `missing_file` | error | `No such file or directory`, `FileNotFoundError` |
| `permission_denied` | error | `Permission denied`, `Operation not permitted` |
| `disk_full` | error | `No space left on device`, quota exceeded |
| `segfault` | error | `Segmentation fault`, `signal 11`, `core dumped` |
| `python_traceback` | error | `Traceback (most recent call last)` |
| `freesurfer_error` | error | `ERROR: recon-all`, `mri_convert.*error` |
| `qsiprep_error` | error | `qsiprep.*error`, `RuntimeError.*qsiprep` |
| `qsiprep_warning` | warning | `qsiprep.*warning`, Nipype warnings |
| `slurm_node_fail` | error | `node.*fail`, `slurmstepd.*error` |
| `cuda_error` | error | `CUDA.*error`, `cudaError` |

Log files are located by looking up the `job_id` in the state file and globbing for `*_<job_id>.{out,err}` under the procedure's log subdirectory. If no `job_id` is available, a subject-based glob is used as a fallback.

Log scanning requires `slurm_log_dir` to be set in the config.

---

## Email delivery

Passing `--email` sends an HTML/plain-text multipart email via a local MTA (sendmail or Postfix) on port 25. No authentication is performed. Configure recipients in the config:

```yaml
audit:
  email_recipients:
    - pi@example.com
    - data-manager@example.com
  email_from: snbb-scheduler@localhost
```

See [Audit Configuration](../configuration/audit.md) for the full reference.

---

## Notes

- `audit` never modifies the state file or submits jobs — it is read-only
- Running `audit` frequently is safe; each run is independent unless `--history` is used
- Saved JSON reports grow over time; set up periodic cleanup in `report_dir` if needed

# Audit Configuration

The `audit:` block in the config file controls the behaviour of the `snbb-scheduler audit` command: DICOM validation thresholds, stale-job detection, report storage, and email delivery.

---

## Quick start

```yaml
audit:
  dicom_min_files: 10
  stale_job_threshold_hours: 168
  report_dir: /data/snbb/audit_reports
  email_recipients:
    - pi@example.com
    - data-manager@example.com
  email_from: snbb-scheduler@localhost
  smtp_host: smtp.gmail.com
  smtp_port: 587
  smtp_tls: true
  smtp_username: your@gmail.com
  smtp_password: your-app-password
```

All fields are optional. Omitting the `audit:` block entirely uses the defaults shown above.

---

## Field reference

| Field | Type | Default | Description |
|---|---|---|---|
| `dicom_min_files` | int | `10` | Sessions with fewer total DICOM files than this threshold are flagged as suspicious in the report |
| `stale_job_threshold_hours` | int | `168` | Jobs in `pending` or `running` for longer than this many hours are listed in the Stale Jobs section (default: 7 days) |
| `report_dir` | path | `null` | Directory where audit output is saved. Two files are written here: a timestamped JSON report after each `audit` run (required for `--history`), and `audit_report.html` — an HTML summary of the scheduler event log that is updated automatically after every submitted, status-change, or error event. When `null`, neither file is written. |
| `email_recipients` | list of str | `[]` | Email addresses to send the report to when `--email` is passed |
| `email_from` | str | `"snbb-scheduler@localhost"` | Sender address used in outgoing emails |
| `smtp_host` | str | `"localhost"` | SMTP server hostname |
| `smtp_port` | int | `25` | SMTP server port (use `587` for STARTTLS) |
| `smtp_tls` | bool | `false` | Issue STARTTLS after connecting |
| `smtp_username` | str | `null` | Username for SMTP authentication; omit for unauthenticated relays |
| `smtp_password` | str | `null` | Password for SMTP authentication; omit for unauthenticated relays |

---

## DICOM validation

When `snbb-scheduler audit` (or `--dicom-only`) runs, it checks each session's DICOM source directory for:

- **Existence** — the path must exist on disk
- **File count** — total files (recursive) must be ≥ `dicom_min_files`
- **Subdirectory structure** — DICOM series are normally stored in numbered subdirectories; a flat directory (no subdirs) is noted in the report

Sessions that fail any of these checks appear in the **DICOM Source Issues** section of the report.

### Tuning `dicom_min_files`

The right threshold depends on your acquisition protocol. A single structural session might have hundreds of DICOM files; a session with only a localiser might have fewer than 10. Start with the default and adjust based on your site's typical acquisitions.

```yaml
audit:
  dicom_min_files: 50   # flag sessions with fewer than 50 files
```

---

## Stale job detection

A job is flagged as **stale** when:

- Its state-file status is `pending` or `running`
- The time since `submitted_at` exceeds `stale_job_threshold_hours`

Stale jobs appear in the **Stale Jobs** section and are also counted in per-procedure summaries. They often indicate a job that was silently cancelled by Slurm (e.g. cluster maintenance, node failure) and never updated via `sacct`. Use `snbb-scheduler retry` to clear them.

```yaml
audit:
  stale_job_threshold_hours: 48   # flag anything stuck for more than 2 days
```

---

## Report persistence and history

When `report_dir` is set, two types of files are written there:

**`audit_report.html`** — An HTML summary of the scheduler event log (submissions, status changes, errors). This file is updated automatically after every scheduler event during normal `run`, `retry`, and `monitor` operations — no extra commands needed.

**Timestamped JSON reports** — Each `snbb-scheduler audit` run saves a snapshot:

```
/data/snbb/audit_reports/audit_20241115_060000.json
```

These files are used by `--history` to compute deltas between the current and previous run. The history comparison shows:

- **Health trend** — average health score change across all sessions
- **New completions** — procedures that moved to `complete` since the last run
- **New failures** — procedures that moved to `failed` since the last run
- **Sessions added / removed** — change in the total session count

To enable history:

```bash
snbb-scheduler --config config.yaml audit --history
```

Reports accumulate over time. Set up periodic cleanup (e.g. keep last 30 days) via a cron job or shell script:

```bash
find /data/snbb/audit_reports -name "audit_*.json" -mtime +30 -delete
```

---

## Email delivery

The `--email` flag sends a multipart HTML + plain-text email. By default it connects to `localhost:25` (a local Postfix/sendmail relay), but you can point it at any SMTP server including Gmail, Outlook, or a hosted relay.

See the full setup guide: [Email setup](../guides/email-setup.md).

### Requirements

- `email_recipients` must be set in the config
- The SMTP server must be reachable from the machine running the scheduler

### Configuration

**Local relay (HPC default)**
```yaml
audit:
  email_recipients:
    - pi@example.com
  email_from: snbb-scheduler@hpc.example.com
  # smtp_host/port default to localhost:25 — no auth, no TLS
```

**External SMTP (e.g. Gmail)**
```yaml
audit:
  email_recipients:
    - pi@example.com
  email_from: your@gmail.com
  smtp_host: smtp.gmail.com
  smtp_port: 587
  smtp_tls: true
  smtp_username: your@gmail.com
  smtp_password: your-app-password   # use a Gmail App Password, not your login password
```

### Usage

```bash
# Run audit and email the report
snbb-scheduler --config config.yaml audit --email

# HTML file + email
snbb-scheduler --config config.yaml audit --format html --output report.html --email
```

### Troubleshooting

If email delivery fails:

1. Check you are passing `--config` — the default config has no `email_recipients` and will print a warning instead of sending.
2. For local relay: `systemctl status postfix` and `/var/log/mail.log`
3. For external SMTP: verify host/port/TLS/credentials and that the server is reachable (`telnet smtp.gmail.com 587`)
4. For Gmail: ensure you are using an [App Password](https://myaccount.google.com/apppasswords), not your Google account password

---

## Complete example config

```yaml
dicom_root:       /data/snbb/dicom
bids_root:        /data/snbb/bids
derivatives_root: /data/snbb/derivatives
state_file:       /data/snbb/.scheduler_state.parquet
slurm_log_dir:    /data/snbb/logs/slurm

audit:
  dicom_min_files: 10
  stale_job_threshold_hours: 168
  report_dir: /data/snbb/audit_reports
  email_recipients:
    - pi@example.com
    - data-manager@example.com
  email_from: snbb-scheduler@hpc.example.com
  smtp_host: smtp.gmail.com
  smtp_port: 587
  smtp_tls: true
  smtp_username: your@gmail.com
  smtp_password: your-app-password
```

With `slurm_log_dir` set and `report_dir` configured, a full daily audit via cron looks like:

```bash
# /etc/cron.d/snbb-audit
0 8 * * * snbb /path/to/venv/bin/snbb-scheduler --config /etc/snbb/config.yaml audit --history --email
```

---

## See also

- [`audit` CLI command](../cli/audit.md) — all command options and report sections
- [Email setup guide](../guides/email-setup.md) — step-by-step for Gmail, Outlook, and local relay
- [Slurm log configuration](slurm.md) — setting up `slurm_log_dir`
- [Cron setup](../guides/cron-setup.md) — scheduling daily runs

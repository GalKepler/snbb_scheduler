# Getting Started

This guide walks you from a fresh install to a working pipeline in 5 steps.

---

## Step 1 — Install

```bash
git clone https://github.com/GalKepler/snbb_scheduler.git
cd snbb_scheduler
pip install -e ".[dev]"
```

Verify:

```bash
snbb-scheduler --help
```

---

## Step 2 — Create a config file

Create `/etc/snbb/config.yaml` (or any path you prefer):

```yaml
dicom_root:       /data/snbb/dicom
bids_root:        /data/snbb/bids
derivatives_root: /data/snbb/derivatives
state_file:       /data/snbb/.scheduler_state.parquet

slurm_partition: normal
slurm_account:   snbb
slurm_log_dir:   /data/snbb/logs/slurm
```

This uses the built-in default procedures (bids → bids_post → defacing, bids_post → qsiprep, bids_post → freesurfer, qsiprep + freesurfer → qsirecon).

### Expected directory layout

```
/data/snbb/dicom/
├── sub-0001/
│   └── ses-202407110849/
│       └── *.dcm
├── sub-0002/
│   └── ses-202407110849/
│       └── *.dcm
```

---

## Step 3 — Dry run

See what would be submitted without touching Slurm:

```bash
snbb-scheduler --config /etc/snbb/config.yaml run --dry-run
```

Expected output:

```
Discovering sessions…
  Found 2 session(s).
  2 task(s) need processing.
  2 task(s) after filtering in-flight jobs.
[DRY RUN] Would submit: sbatch --partition=normal --account=snbb --job-name=bids_sub-0001_ses-202407110849 snbb_run_bids.sh sub-0001 ses-202407110849 /data/snbb/dicom/sub-0001/ses-202407110849
[DRY RUN] Would submit: sbatch --partition=normal --account=snbb --job-name=bids_sub-0002_ses-202407110849 snbb_run_bids.sh sub-0002 ses-202407110849 /data/snbb/dicom/sub-0002/ses-202407110849
[DRY RUN] Would submit 2 job(s).
```

If the output looks wrong, check your config paths and verify the DICOM directory structure.

---

## Step 4 — Submit real jobs

```bash
snbb-scheduler --config /etc/snbb/config.yaml run
```

The scheduler submits the jobs and saves their job IDs to the state file.

---

## Step 5 — Check status

```bash
snbb-scheduler --config /etc/snbb/config.yaml status
```

```
Summary:
  procedure  status  count
       bids  pending      2

   subject                session  procedure  status           submitted_at  job_id
  sub-0001  ses-202407110849       bids       pending  2024-11-01 06:00:00   10234
  sub-0002  ses-202407110849       bids       pending  2024-11-01 06:00:00   10235
```

---

## After jobs complete

Once the `bids` jobs finish on Slurm, run `monitor` to update their status:

```bash
snbb-scheduler --config /etc/snbb/config.yaml monitor
```

Then run again to submit the next pipeline stage:

```bash
snbb-scheduler --config /etc/snbb/config.yaml run
```

The scheduler sees the `bids` output on disk and submits `bids_post` jobs for those sessions.

---

## If a job fails

```bash
# See what failed
snbb-scheduler --config /etc/snbb/config.yaml status

# Clear failed entries
snbb-scheduler --config /etc/snbb/config.yaml retry

# Resubmit
snbb-scheduler --config /etc/snbb/config.yaml run
```

---

## Setting up automation

Add a cron job to run the scheduler daily:

```cron
# /etc/cron.d/snbb-scheduler
0 6 * * * snbb-user snbb-scheduler --config /etc/snbb/config.yaml run >> /var/log/snbb_scheduler.log 2>&1
```

See [Cron / Systemd Setup](cron-setup.md) for a complete example with monitoring.

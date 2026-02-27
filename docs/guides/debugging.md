# Debugging

Common problems and how to diagnose them.

---

## No tasks appear in the manifest

**Symptom:** `snbb-scheduler manifest` outputs `No tasks pending.`

**Diagnosis:**

1. Check if sessions are discovered:
   ```bash
   python3 -c "
   from snbb_scheduler.config import SchedulerConfig
   from snbb_scheduler.sessions import discover_sessions
   cfg = SchedulerConfig.from_yaml('/etc/snbb/config.yaml')
   sessions = discover_sessions(cfg)
   print(f'Found {len(sessions)} sessions')
   print(sessions[['subject', 'session', 'dicom_exists']].to_string())
   "
   ```

2. Check if dicom_root exists and has the right structure:
   ```
   dicom_root/
   ├── sub-0001/
   │   └── ses-202407110849/
   │       └── *.dcm
   ```

3. Check if all procedures are already complete:
   ```python
   from snbb_scheduler.config import SchedulerConfig
   from snbb_scheduler.sessions import discover_sessions
   cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
   sessions = discover_sessions(cfg)
   # Check bids_exists for all sessions
   print(sessions[["subject", "session", "bids_exists", "qsiprep_exists"]])
   ```

---

## Jobs stuck as `pending`

**Symptom:** `status` shows jobs as `pending` long after submission.

**Cause:** sacct hasn't been polled, or the job is no longer in sacct's retention window.

**Fix:**

```bash
# Poll sacct and reconcile filesystem
snbb-scheduler --config config.yaml monitor

# If the output exists on disk, it will be marked complete automatically.
# If sacct reports the job as failed, status will update to failed.
```

If the job is not in sacct and the output doesn't exist, the entry remains `pending`. Clear it manually:

```python
import pandas as pd
state = pd.read_parquet("/data/snbb/.scheduler_state.parquet")
# Remove the stuck entry
state = state[~((state["subject"]=="sub-0001") & (state["procedure"]=="bids"))].reset_index(drop=True)
state.to_parquet("/data/snbb/.scheduler_state.parquet", index=False)
```

---

## `sbatch` fails with permission error

**Symptom:** `subprocess.CalledProcessError` on `sbatch`.

**Diagnosis:**
- Verify the script is on `$PATH` or provide a full path
- Verify the script is executable: `chmod +x snbb_run_bids.sh`
- Check Slurm account: `sacctmgr show user $USER`
- Check partition exists: `sinfo -p <partition>`

---

## Config errors

### `FileNotFoundError`

```
FileNotFoundError: [Errno 2] No such file or directory: '/etc/snbb/config.yaml'
```

Pass the correct path with `--config`.

### `ValueError: Invalid YAML`

```
ValueError: Invalid YAML in config.yaml: ...
```

Validate your YAML syntax:
```bash
python3 -c "import yaml; yaml.safe_load(open('config.yaml'))"
```

### `ValueError: Procedure X depends on Y which is not in the procedures list`

You defined a procedure in `depends_on` that doesn't exist. Check procedure names in your `procedures` list.

---

## Duplicate jobs submitted

**Symptom:** Multiple jobs with the same `subject`, `session`, `procedure` in the state file.

**Cause:** `--force` was used, or the state file was manually edited inconsistently.

**Fix:** The in-flight filter prevents this in normal operation. After using `--force`, the state file may have duplicate entries — that's expected. Use `monitor` and then check `status` to see their final states.

---

## FreeSurfer never marked complete

**Symptom:** `freesurfer_exists` is `True` but freesurfer keeps appearing in the manifest.

**Cause:** The specialized `freesurfer` check also verifies that the number of T1w inputs in `recon-all.done` matches the currently available T1w files. If a new session was added, the check fails.

**Fix:** Re-run freesurfer to incorporate the new session:
```bash
snbb-scheduler --config config.yaml run --force --procedure freesurfer --dry-run
snbb-scheduler --config config.yaml run --force --procedure freesurfer
```

---

## sacct not available

**Symptom:** `WARNING snbb_scheduler.monitor: sacct not found; skipping job status update.`

**Cause:** The `sacct` command is not on `$PATH` (common on login nodes without Slurm in the environment).

**Fix:** Ensure Slurm tools are loaded:
```bash
module load slurm
```
Or set `$PATH` to include the Slurm binaries. Filesystem reconciliation still works without sacct.

---

## State file corruption

**Symptom:** `pd.read_parquet` raises an error or returns unexpected data.

**Fix:** Back up the corrupt file and create a fresh one by deleting it. The scheduler will create a new empty state file on the next run. You will lose history of previously submitted jobs, but the filesystem is the source of truth — `reconcile_with_filesystem` will re-discover completed outputs.

```bash
cp /data/snbb/.scheduler_state.parquet /data/snbb/.scheduler_state.parquet.bak
rm /data/snbb/.scheduler_state.parquet
```

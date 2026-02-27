# Configuration

All site-specific paths and settings live in a single YAML file. The scheduler has built-in defaults; override only what differs on your system.

## Loading config

Pass the config path to every CLI command:

```bash
snbb-scheduler --config /etc/snbb/config.yaml run
```

Or load it in Python:

```python
from snbb_scheduler.config import SchedulerConfig

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
```

---

## Minimal config

```yaml
# /etc/snbb/config.yaml
dicom_root:       /data/snbb/dicom
bids_root:        /data/snbb/bids
derivatives_root: /data/snbb/derivatives
state_file:       /data/snbb/.scheduler_state.parquet

slurm_partition: debug
slurm_account:   snbb
```

With no `procedures` key the built-in defaults run: **bids → bids_post → defacing**, **bids_post → qsiprep**, **bids_post → freesurfer**, **qsiprep + freesurfer → qsirecon**.

---

## Full config reference

```yaml
# ── Paths ────────────────────────────────────────────────────────────────
dicom_root:       /data/snbb/dicom         # raw DICOM tree: sub-*/ses-*/
bids_root:        /data/snbb/bids          # BIDS dataset root
derivatives_root: /data/snbb/derivatives   # all derivatives

# ── State tracking ────────────────────────────────────────────────────────
state_file: /data/snbb/.scheduler_state.parquet
# Optional: defaults to <state_file_parent>/scheduler_audit.jsonl
log_file:   /data/snbb/scheduler_audit.jsonl

# ── Session discovery (optional) ─────────────────────────────────────────
# When set, filesystem walk is skipped and sessions are read from this CSV.
# CSV must have columns: SubjectCode, ScanID, dicom_path
sessions_file: /data/snbb/linked_sessions.csv

# ── Slurm settings ────────────────────────────────────────────────────────
slurm_partition: debug    # omit or "" for clusters without partitions
slurm_account:   snbb
slurm_mem:       32G      # optional: adds --mem=32G to sbatch
slurm_cpus_per_task: 8    # optional: adds --cpus-per-task=8 to sbatch

# Slurm log directory — adds --output and --error to sbatch.
# Subdirectories are created per procedure: <slurm_log_dir>/<procedure>/
slurm_log_dir: /data/snbb/logs/slurm

# ── Procedures ────────────────────────────────────────────────────────────
# Omit this section to use the built-in defaults.
procedures:
  - name: bids
    output_dir: ""          # empty string → outputs go in bids_root
    script: snbb_run_bids.sh
    scope: session
    depends_on: []
    completion_marker:
      - "anat/*_T1w.nii.gz"
      - "dwi/*dir-AP*_dwi.nii.gz"
      - "dwi/*dir-AP*_dwi.bvec"
      - "dwi/*dir-AP*_dwi.bval"
      - "dwi/*dir-PA*_dwi.nii.gz"
      - "fmap/*acq-func_dir-AP*epi.nii.gz"
      - "fmap/*acq-func_dir-PA*epi.nii.gz"
      - "func/*task-rest_bold.nii.gz"

  - name: bids_post
    output_dir: ""
    script: snbb_run_bids_post.sh
    scope: session
    depends_on: [bids]
    completion_marker: "fmap/*acq-dwi*_epi.nii.gz"

  - name: defacing
    output_dir: ""
    script: snbb_run_defacing.sh
    scope: session
    depends_on: [bids_post]
    completion_marker: "anat/*acq-defaced*_T1w.nii.gz"

  - name: qsiprep
    output_dir: qsiprep
    script: snbb_run_qsiprep.sh
    scope: subject
    depends_on: [bids_post]
    completion_marker:
      - "ses-*/dwi/*dir-AP*_dwi_preproc.nii.gz"
      - "ses-*/dwi/*dir-AP*_dwi_preproc.bvec"
      - "ses-*/dwi/*dir-AP*_dwi_preproc.bval"
      - "ses-*/dwi/*dir-AP*desc-image_qc.tsv"

  - name: freesurfer
    output_dir: freesurfer
    script: snbb_run_freesurfer.sh
    scope: subject
    depends_on: [bids_post]
    completion_marker: "scripts/recon-all.done"

  - name: qsirecon
    output_dir: qsirecon-MRtrix3_act-HSVS
    script: snbb_run_qsirecon.sh
    scope: subject
    depends_on: [qsiprep, freesurfer]
    completion_marker: null
```

---

## Field reference

| Field | Type | Default | Description |
|---|---|---|---|
| `dicom_root` | path | `/data/snbb/dicom` | Root of the raw DICOM tree |
| `bids_root` | path | `/data/snbb/bids` | BIDS dataset root |
| `derivatives_root` | path | `/data/snbb/derivatives` | Root for all derivative outputs |
| `state_file` | path | `/data/snbb/.scheduler_state.parquet` | Parquet state file |
| `log_file` | path | *(auto)* | JSONL audit log; defaults next to `state_file` |
| `sessions_file` | path | `null` | Optional pre-built session CSV |
| `slurm_partition` | str | `"debug"` | Slurm partition; omit `--partition` if empty |
| `slurm_account` | str | `"snbb"` | Slurm account for `--account` |
| `slurm_mem` | str | `null` | Memory per job, e.g. `"32G"` |
| `slurm_cpus_per_task` | int | `null` | CPUs per task |
| `slurm_log_dir` | path | `null` | Directory for `--output`/`--error` log files |
| `procedures` | list | *(built-in defaults)* | List of procedure declarations |

---

## Overriding settings from the CLI

Some fields can be overridden per-invocation without editing the config file:

```bash
snbb-scheduler --config config.yaml --slurm-mem 64G --slurm-cpus 16 run
snbb-scheduler --config config.yaml --slurm-log-dir /tmp/logs run --dry-run
```

See [CLI Overview](../cli/index.md) for the full list of global options.

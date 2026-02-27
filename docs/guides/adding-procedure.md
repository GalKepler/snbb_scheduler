# Adding a Procedure

Adding a new pipeline step to snbb-scheduler requires no code changes. You just declare the procedure.

## Option A — YAML only (recommended)

Add the procedure to the `procedures` list in your `config.yaml`. When you provide a `procedures` key, it **replaces** the built-in defaults, so include all the procedures you want to run.

### Example: add qsirecon with custom output dir

```yaml
procedures:
  - name: bids
    output_dir: ""
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

  # ─── New procedure ─────────────────────────────────────────────────────────
  - name: fmriprep
    output_dir: fmriprep
    script: snbb_run_fmriprep.sh
    scope: session
    depends_on: [bids]
    completion_marker: "**/*.html"   # fmriprep writes an HTML report when done
```

On the next `run`, the scheduler will:
- include `fmriprep_path` and `fmriprep_exists` columns in the sessions DataFrame
- generate a rule that triggers `fmriprep` when `bids` is complete and `fmriprep` is not
- pass `subject` and `session` as arguments to `snbb_run_fmriprep.sh`

---

## Option B — Python API (for programmatic setups)

Use this when you need to build configs dynamically or in code:

```python
from snbb_scheduler.config import DEFAULT_PROCEDURES, Procedure, SchedulerConfig

fmriprep = Procedure(
    name="fmriprep",
    output_dir="fmriprep",
    script="snbb_run_fmriprep.sh",
    scope="session",
    depends_on=["bids"],
    completion_marker="**/*.html",
)

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
cfg.procedures.append(fmriprep)
```

Or build the full config programmatically:

```python
cfg = SchedulerConfig(
    dicom_root="/data/snbb/dicom",
    bids_root="/data/snbb/bids",
    derivatives_root="/data/snbb/derivatives",
    state_file="/data/snbb/.scheduler_state.parquet",
    procedures=list(DEFAULT_PROCEDURES) + [fmriprep],
)
```

---

## Writing the shell script

Each procedure script must:

1. Accept `subject` as `$1` (all procedures)
2. Accept `session` as `$2` and `dicom_path` as `$3` (session-scoped procedures only)
3. Exit with code 0 on success, non-zero on failure

### Minimal session-scoped script template

```bash
#!/usr/bin/env bash
set -euo pipefail

SUBJECT=$1
SESSION=$2
DICOM_PATH=${3:-}   # optional; may be empty for file-based session discovery

# Your processing command here
apptainer run \
  --bind /data:/data \
  /containers/mytool.sif \
  --subject "$SUBJECT" \
  --session "$SESSION"
```

### Minimal subject-scoped script template

```bash
#!/usr/bin/env bash
set -euo pipefail

SUBJECT=$1

# Your processing command here
apptainer run \
  --bind /data:/data \
  /containers/mytool.sif \
  --subject "$SUBJECT"
```

### Environment variables available

Scripts run as Slurm batch jobs. They can read any environment variables set in the Slurm environment or exported by the submitting shell. See [Shell Scripts reference](../reference/scripts.md) for the full list of variables used by the built-in scripts.

---

## Checking your new procedure

After adding the procedure, verify:

```bash
# Check it appears in the manifest
snbb-scheduler --config config.yaml manifest

# Dry run to see the sbatch command
snbb-scheduler --config config.yaml run --dry-run
```

The new procedure's tasks should appear with the expected `subject`, `session`, and `priority`.

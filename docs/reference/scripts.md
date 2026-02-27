# Shell Scripts

Each procedure has a corresponding shell script in `scripts/`. These scripts are passed to `sbatch` and run on the cluster as Slurm batch jobs. All scripts accept positional arguments from the scheduler and read site-specific paths from environment variables.

---

## `snbb_run_bids.sh`

Converts DICOMs to BIDS format using [heudiconv](https://heudiconv.readthedocs.io) via Apptainer.

**Called as:** `sbatch ... snbb_run_bids.sh sub-XXXX ses-YY /path/to/dicom`

**Positional arguments:**

| `$1` | `SUBJECT` | BIDS subject label, e.g. `sub-0001` |
| `$2` | `SESSION` | BIDS session label, e.g. `ses-202407110849` |
| `$3` | `DICOM_PATH_ARG` | Optional explicit DICOM path; falls back to `SNBB_DICOM_SESSION_DIR` |

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `SNBB_DICOM_ROOT` | `/data/snbb/dicom` | Root of the DICOM tree |
| `SNBB_DICOM_SESSION_DIR` | `<SNBB_DICOM_ROOT>/<SESSION_ID>` | Fallback per-session DICOM path |
| `SNBB_BIDS_ROOT` | *(site-specific)* | Output BIDS dataset root |
| `SNBB_HEURISTIC` | *(site-specific)* | Path to the heudiconv heuristic Python file |
| `SNBB_HEUDICONV_SIF` | *(site-specific)* | Path to the heudiconv Apptainer image |
| `SNBB_DEBUG_LOG` | *(site-specific)* | Path for the per-run debug log |

**Embedded Slurm resources:**
```
#SBATCH --time=4:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=4
```

---

## `snbb_run_bids_post.sh`

Post-processing step: derives DWI fieldmap EPI sidecars from BIDS data.

**Called as:** `sbatch ... snbb_run_bids_post.sh sub-XXXX ses-YY /path/to/dicom`

Calls `scripts/snbb_bids_post.py` to generate the derived `fmap/*acq-dwi*_epi` files required by QSIPrep.

---

## `snbb_run_defacing.sh`

Defaces T1w images in-place within the BIDS dataset, writing defaced images as `anat/*acq-defaced*_T1w.nii.gz`.

**Called as:** `sbatch ... snbb_run_defacing.sh sub-XXXX ses-YY /path/to/dicom`

---

## `snbb_run_qsiprep.sh`

DWI preprocessing via [QSIPrep](https://qsiprep.readthedocs.io) using Apptainer.

**Called as:** `sbatch ... snbb_run_qsiprep.sh sub-XXXX`
(subject-scoped: processes all sessions for the subject in one job)

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `SNBB_BIDS_ROOT` | *(site-specific)* | BIDS dataset root (read-only input) |
| `SNBB_DERIVATIVES` | *(site-specific)* | Parent derivatives directory (qsiprep writes `qsiprep/` inside it) |
| `SNBB_FS_LICENSE` | *(site-specific)* | FreeSurfer license file path |
| `SNBB_WORK_DIR` | *(site-specific)* | QSIPrep working directory |
| `SNBB_QSIPREP_SIF` | *(site-specific)* | Path to the QSIPrep Apptainer image |
| `SNBB_ANATOMICAL_TEMPLATE` | `MNI152NLin2009cAsym` | Anatomical template space |
| `SNBB_SUBJECT_ANAT_REF` | `unbiased` | Subject anatomical reference |
| `SNBB_BIDS_FILTER_FILE` | *(optional)* | BIDS filter JSON file |
| `SNBB_TEMPLATEFLOW_HOME` | *(site-specific)* | TemplateFlow cache directory |
| `SNBB_LOCAL_TMP_ROOT` | *(empty)* | Enable local-scratch mode (see below) |

**Embedded Slurm resources:**
```
#SBATCH --time=12:00:00
#SBATCH --mem=20G
#SBATCH --cpus-per-task=8
```

### Local-scratch mode

When `SNBB_LOCAL_TMP_ROOT` is set, the script stages BIDS input and QSIPrep output on the compute node's local disk (`/tmp` or similar), then rsyncs results back to `SNBB_DERIVATIVES` on success. This improves I/O performance on clusters with slow network filesystems. On failure, the local workdir is preserved for manual recovery.

---

## `snbb_run_freesurfer.sh`

Structural reconstruction via [FreeSurfer](https://surfer.nmr.mgh.harvard.edu) `recon-all`, run inside an Apptainer container.

**Called as:** `sbatch ... snbb_run_freesurfer.sh sub-XXXX`
(subject-scoped)

Uses `scripts/snbb_recon_all_helper.py` to collect all T1w (and T2w) NIfTI files across all BIDS sessions and build the `-i` argument list for `recon-all`.

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `SNBB_BIDS_ROOT` | *(site-specific)* | BIDS dataset root |
| `SNBB_FS_OUTPUT` | *(site-specific)* | Final FreeSurfer output directory |
| `SNBB_TMP_FS_OUTPUT` | *(site-specific)* | Intermediate writable FreeSurfer directory |
| `SNBB_FS_LICENSE` | *(site-specific)* | FreeSurfer license file |
| `SNBB_FREESURFER_SIF` | *(site-specific)* | FreeSurfer Apptainer image |
| `SNBB_LOCAL_TMP_ROOT` | *(empty)* | Enable local-scratch mode |

**Embedded Slurm resources:**
```
#SBATCH --time=24:00:00
#SBATCH --mem=20G
#SBATCH --cpus-per-task=8
```

After `recon-all` completes, the script rsyncs results from the temporary directory to `SNBB_FS_OUTPUT` and removes the temp copy (only if `scripts/recon-all.done` is present).

---

## `snbb_run_qsirecon.sh`

Tractography and connectivity via [QSIRecon](https://qsirecon.readthedocs.io) using Apptainer.

**Called as:** `sbatch ... snbb_run_qsirecon.sh sub-XXXX`
(subject-scoped: processes all sessions for the subject)

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `SNBB_QSIPREP_DIR` | *(site-specific)* | QSIPrep output directory (input to QSIRecon) |
| `SNBB_QSIRECON_OUTPUT_DIR` | *(site-specific)* | QSIRecon output directory |
| `SNBB_FS_LICENSE` | *(site-specific)* | FreeSurfer license file |
| `SNBB_FS_SUBJECTS_DIR` | *(site-specific)* | FreeSurfer subjects directory |
| `SNBB_RECON_SPEC` | *(site-specific)* | QSIRecon reconstruction spec YAML |
| `SNBB_WORK_DIR` | *(site-specific)* | QSIRecon working directory |
| `SNBB_QSIRECON_SIF` | *(site-specific)* | QSIRecon Apptainer image |
| `SNBB_RESPONSES_DIR` | *(optional)* | Pre-computed MRtrix3 response functions |
| `SNBB_ATLASES_DIR` | *(optional)* | Atlas dataset directory |
| `SNBB_ATLASES` | *(optional)* | Space-separated atlas names |
| `SNBB_TEMPLATEFLOW_HOME` | *(site-specific)* | TemplateFlow cache directory |
| `SNBB_LOCAL_TMP_ROOT` | *(empty)* | Enable local-scratch mode |

**Embedded Slurm resources:**
```
#SBATCH --time=12:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8
```

---

## `snbb_recon_all_helper.py`

Python helper called by `snbb_run_freesurfer.sh`. Collects T1w and T2w NIfTI files for a subject (excluding defaced images, preferring `rec-norm` when available) and calls `recon-all` via the FreeSurfer Apptainer container.

**Arguments:**
```
--bids-dir    Path to BIDS root (or local copy)
--output-dir  FreeSurfer SUBJECTS_DIR
--subject     BIDS subject label (e.g. sub-0001)
--threads     Number of parallel threads for recon-all
--sif         Path to FreeSurfer Apptainer image
--fs-license  Path to FreeSurfer license file
```

---

## Setting environment variables

Environment variables can be set in three ways, in order of precedence:

1. **In the script itself** (edit the default values at the top of each `.sh` file)
2. **In your Slurm environment** (export before submitting, or set in `~/.bashrc`)
3. **Via `sbatch --export`** (pass specific vars to the job)

Example — set variables in your environment:

```bash
export SNBB_BIDS_ROOT=/data/site/bids
export SNBB_DERIVATIVES=/data/site/derivatives
export SNBB_FS_LICENSE=/opt/freesurfer/license.txt
export SNBB_QSIPREP_SIF=/containers/qsiprep-1.1.1.sif
export SNBB_FREESURFER_SIF=/containers/freesurfer-8.1.0.sif
export SNBB_QSIRECON_SIF=/containers/qsirecon-1.2.0.sif
export SNBB_TEMPLATEFLOW_HOME=/data/templateflow
```

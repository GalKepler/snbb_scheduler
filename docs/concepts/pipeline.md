# The SNBB Pipeline

This page describes what each procedure actually does — the tools, the science, the inputs, and the outputs. All procedures run inside [Apptainer](https://apptainer.org/) containers (except `bids_post` and `defacing`, which are pure Python/FSL).

---

## Pipeline overview

```
DICOM
  └─▶ bids          DICOM → BIDS (heudiconv)
        └─▶ bids_post    Derive DWI fieldmap EPI, add IntendedFor
              ├─▶ defacing       Deface T1w / T2w (FSL fsl_deface)
              ├─▶ qsiprep        DWI preprocessing (QSIPrep)
              │     └─▶ qsirecon  Tractography & connectivity (QSIRecon / MRtrix3)
              └─▶ freesurfer     Cortical reconstruction (FreeSurfer recon-all)
                    └─▶ qsirecon  (same node, waits for both)
```

---

## `bids` — DICOM to BIDS conversion

**Tool:** [heudiconv](https://heudiconv.readthedocs.io/) 1.3.4 via Apptainer
**BIDS spec:** [bids.neuroimaging.io](https://bids.neuroimaging.io/)
**Scope:** session
**Slurm resources:** 4 h, 8 GB RAM, 4 CPUs

### What it does

Converts raw DICOM files for one session into [BIDS](https://bids.neuroimaging.io/) format using [heudiconv](https://heudiconv.readthedocs.io/) with a site-specific heuristic (`scripts/heuristic.py`). heudiconv reads DICOM metadata, applies the heuristic to decide which series goes where, runs `dcm2niix` for NIfTI conversion, and writes properly named files with JSON sidecars.

### Expected outputs (completion markers)

All of the following must be present in `bids_root/sub-XX/ses-YY/` for the session to be considered complete:

| File pattern | Modality |
|---|---|
| `anat/*_T1w.nii.gz` | Structural T1-weighted |
| `dwi/*dir-AP*_dwi.nii.gz` / `.bvec` / `.bval` | DWI — AP phase-encode direction |
| `dwi/*dir-PA*_dwi.nii.gz` | DWI — PA reverse phase-encode (6 directions, b=1000) |
| `fmap/*acq-func_dir-AP*epi.nii.gz` | Functional fieldmap — AP |
| `fmap/*acq-func_dir-PA*epi.nii.gz` | Functional fieldmap — PA |
| `func/*task-rest_bold.nii.gz` | Resting-state fMRI |

---

## `bids_post` — BIDS fieldmap post-processing

**Tool:** Custom Python script (`scripts/snbb_bids_post.py`)
**Scope:** session
**Slurm resources:** 30 min, 2 GB RAM, 1 CPU

### What it does

heudiconv writes the short reverse phase-encode DWI acquisition as a DWI series in `dwi/`, but QSIPrep expects the corresponding fieldmap to live in `fmap/` as a proper EPI fieldmap. This script bridges that gap in three steps:

**Step 1 — Derive the DWI fieldmap EPI**
Reads each `dwi/*_dir-PA_dwi.nii.gz` and its companion `.bval`. Identifies b=0 volumes (bval < 100 s/mm²) and computes their mean to produce a single 3-D b0 image. Writes the result to `fmap/*_acq-dwi_dir-PA_epi.nii.gz` with a JSON sidecar.

**Step 2 — Add `IntendedFor` fields**
Updates every `fmap/*_epi.json` to include a BIDS-compliant `IntendedFor` field:
- `acq-dwi` fieldmaps → point to `dwi/*_dir-AP_dwi.nii.gz` (AP only)
- `acq-func` fieldmaps → point to `func/*_bold.nii.gz`

Without `IntendedFor`, downstream tools like QSIPrep and fMRIPrep cannot determine which fieldmap applies to which scan.

**Step 3 — Hide spurious `.bvec`/`.bval` in `fmap/`**
Some heudiconv versions write `.bvec`/`.bval` alongside EPI fieldmaps. These confuse BIDS validators and downstream tools. The script renames them with a leading dot (e.g. `.filename.bvec`) to hide them without deleting them.

### Completion marker

`fmap/*acq-dwi*_epi.nii.gz` — the derived DWI fieldmap EPI file.

---

## `defacing` — T1w / T2w defacing

**Tool:** [FSL](https://fsl.fmrib.ox.ac.uk/fsl/fslwiki/) `fsl_deface`
**Scope:** session
**Slurm resources:** 1 h, 8 GB RAM, 4 CPUs

### What it does

Applies `fsl_deface` to every T1w and T2w image in the session's `anat/` directory to remove facial features. The defaced images are written alongside the originals using the `acq-defaced` BIDS entity:

```
anat/sub-0001_ses-YY_T1w.nii.gz             ← original (kept)
anat/sub-0001_ses-YY_acq-defaced_T1w.nii.gz ← defaced copy
```

JSON sidecars are copied alongside each defaced image. Only files without an existing `acq-` entity are processed, preventing double-defacing.

`fsl_deface` uses FSL BET brain extraction and nonlinear registration to a face template to mask out the facial region. The method is conservative — it errs on the side of removing too much rather than leaving facial features.

### Why defacing matters

Raw T1w images may allow facial re-identification. SNBB shares derivatives publicly; the defaced images are what get shared while the originals remain on-site.

### Completion marker

`anat/*acq-defaced*_T1w.nii.gz`

---

## `qsiprep` — DWI preprocessing

**Tool:** [QSIPrep](https://qsiprep.readthedocs.io/) 1.1.1 via Apptainer
**Scope:** subject (all sessions processed together in one job)
**Slurm resources:** 12 h, 20 GB RAM, 8 CPUs

### What it does

[QSIPrep](https://qsiprep.readthedocs.io/) is a BIDS-App for preprocessing diffusion MRI data. For each subject it:

1. **Reads all DWI sessions** from the BIDS dataset simultaneously, enabling across-session head motion correction
2. **Susceptibility distortion correction (SDC)** using the `acq-dwi` EPI fieldmap produced by `bids_post` (Pepolar / TOPUP method: AP + PA b0 pair)
3. **Head motion correction** — rigid-body registration of each DWI volume to a b0 template
4. **Eddy current correction** — FSL eddy
5. **Co-registration** — DWI → T1w space using ANTs
6. **Resampling** to 1.6 mm isotropic voxels in T1w space (or MNI space for template maps)
7. **QC metrics** — per-volume and per-series quality control TSV files (`*desc-image_qc.tsv`)

The BIDS filter file (`examples/bids_filters.json`) can be used to restrict which runs QSIPrep processes.

### Completion markers

All of the following must be present in `derivatives/qsiprep/sub-XX/` per session:

| File pattern | Description |
|---|---|
| `ses-*/dwi/*dir-AP*_dwi_preproc.nii.gz` | Preprocessed DWI (NIfTI) |
| `ses-*/dwi/*dir-AP*_dwi_preproc.bvec` | Rotated gradient vectors |
| `ses-*/dwi/*dir-AP*_dwi_preproc.bval` | b-values |
| `ses-*/dwi/*dir-AP*desc-image_qc.tsv` | QC metrics |

### Local-scratch mode

When `SNBB_LOCAL_TMP_ROOT` is set, BIDS input and QSIPrep output are staged on the compute node's local disk to reduce NFS I/O, then rsynced back on success.

---

## `freesurfer` — Cortical reconstruction

**Tool:** [FreeSurfer](https://surfer.nmr.mgh.harvard.edu/) 8.1.0 via Apptainer
**Scope:** subject
**Slurm resources:** 24 h, 20 GB RAM, 8 CPUs

### What it does

Runs [FreeSurfer](https://surfer.nmr.mgh.harvard.edu/) `recon-all` — the standard pipeline for cortical surface reconstruction from T1w (and optionally T2w) MRI. For each subject:

1. **Image selection** — collects all T1w images across all sessions, excludes `acq-defaced` variants, and prefers `rec-norm` (normalized) variants when available. The same two-step filter applies to T2w.
2. **Multi-session input** — all available T1w files are passed as separate `-i` inputs to `recon-all`, enabling within-subject averaging for improved SNR.
3. **T2w pial surface refinement** — when T2w images are available, `recon-all -T2 ... -T2pial` is used to improve the accuracy of the pial surface, particularly at the GM/CSF boundary.
4. **Full `recon-all -all`** — runs the complete FreeSurfer pipeline in parallel (`-openmp 8`): skull stripping, surface tessellation, cortical parcellation (Desikan-Killiany, Brodmann, etc.), subcortical segmentation, thickness and curvature maps.

The FreeSurfer output is written to a temporary directory first, then rsynced to the final destination to handle read-only network filesystems.

### Completion check (specialized)

The standard marker `scripts/recon-all.done` is checked, **plus** the number of `-i` inputs recorded in the marker file's `#CMDARGS` line must equal the number of T1w images currently available in the BIDS dataset. This catches the case where new sessions were acquired after FreeSurfer ran — the subject is automatically re-queued to incorporate the new data.

### Output

Standard FreeSurfer `SUBJECTS_DIR` layout under `derivatives/freesurfer/sub-XX/`:
```
derivatives/freesurfer/sub-0001/
├── mri/           ← volumes (T1.mgz, aparc+aseg.mgz, ...)
├── surf/          ← surfaces (lh.white, rh.pial, ...)
├── label/         ← parcellation labels
├── stats/         ← cortical thickness, area, volume tables
└── scripts/
    └── recon-all.done   ← completion marker
```

---

## `qsirecon` — Tractography and connectivity

**Tool:** [QSIRecon](https://qsirecon.readthedocs.io/) 1.2.0 via Apptainer
**Reconstruction spec:** `scripts/qsirecon_full_spec.yaml` (`gal_multishell_scalars`)
**Scope:** subject (depends on both `qsiprep` and `freesurfer`)
**Slurm resources:** 12 h, 32 GB RAM, 8 CPUs

### What it does

[QSIRecon](https://qsirecon.readthedocs.io/) orchestrates a multi-step diffusion MRI reconstruction workflow defined in a YAML specification. The SNBB spec (`gal_multishell_scalars`) runs four stages:

---

#### Stage 1 — Diffusion scalar model fitting

Three complementary models are fit to the multi-shell data:

| Model | Software | Scalars produced |
|---|---|---|
| **DKI** — Diffusion Kurtosis Imaging | [DIPY](https://dipy.org/) | MK, AK, RK, MKT, KFA (kurtosis); MD, AD, RD, FA (diffusion tensor) |
| **NODDI** — Neurite Orientation Dispersion and Density Imaging | [AMICO](https://github.com/daducci/AMICO) | NDI (neurite density), ODI (orientation dispersion), Viso (isotropic fraction) |
| **MAP-MRI** — Mean Apparent Propagator MRI | [DIPY](https://dipy.org/) | RTOP, RTAP, RTPP, NG, PA, MSD, QIV |
| **GQI** — Generalized Q-sampling Imaging | [DSI Studio](https://dsi-studio.labsolver.org/) | QA, GFA, ISO, NQA, RQA |

NODDI parameters: `dIso=0.003`, `dPar=0.0017` (optimized for in-vivo brain at clinical field strengths).

#### Stage 2 — MNI template mapping

All scalar maps from Stage 1 are warped to MNI152NLin2009cAsym space using the T1w→MNI warp from QSIPrep, enabling group-level analysis without additional registration.

#### Stage 3 — MSMT-CSD fibre orientation distribution

[Multi-shell multi-tissue constrained spherical deconvolution (MSMT-CSD)](https://mrtrix.readthedocs.io/en/latest/constrained_spherical_deconvolution/msmt_csd.html) ([MRtrix3](https://www.mrtrix.org/)) is used to estimate white matter (WM), grey matter (GM), and CSF fibre orientation distributions (FODs) simultaneously from the multi-shell data.

**Response functions:** pre-computed group-average response functions from 1,426 balanced subjects are used (`median_response_wm/gm/csf_balanced_1426.txt`) rather than per-subject estimation. This improves stability for subjects with pathological tissue.

**mtnormalize:** multi-tissue log-domain intensity normalisation is applied after CSD to correct for global intensity differences.

#### Stage 4 — Tractography with anatomically constrained tractography (ACT)

Two complementary tractography algorithms are run using [MRtrix3](https://www.mrtrix.org/):

| Algorithm | Type | Streamlines | Details |
|---|---|---|---|
| **iFOD2** | Probabilistic | 1,000,000 | 2nd-order integration, backtracking, cropped at GM/WM interface |
| **SD_Stream** | Deterministic | 1,000,000 | Streamline deflection, cropped at GM/WM interface |

Both use **ACT** ([Anatomically Constrained Tractography](https://doi.org/10.1016/j.neuroimage.2012.06.005)) with a **Hybrid Surface and Volume Segmentation (HSVS)** 5-tissue-type (5TT) image derived from the FreeSurfer segmentation. ACT biologically constrains streamlines to begin and end in grey matter, dramatically reducing false-positive connections.

**SIFT2** ([Smith et al., 2015](https://doi.org/10.1016/j.neuroimage.2015.05.039)) is applied to both tractograms to weight streamlines by a factor proportional to the FOD amplitude, correcting for biases introduced by tractography algorithm and FOD amplitude differences. SIFT2 weights are used in connectivity matrix construction.

#### Stage 5 — Connectivity matrices

Structural connectivity matrices are constructed for both iFOD2 and SD_Stream tractograms using `tck2connectome`. Four connectivity measures are computed for each:

| Measure | Description |
|---|---|
| `sift_invnodevol_radius2_count` | SIFT2-weighted streamline count, normalised by inverse node volume |
| `radius2_count` | Raw streamline count |
| `sift_radius2_count` | SIFT2-weighted streamline count |
| `radius2_meanlength` | Mean streamline length |

Atlases are applied from `SNBB_ATLASES_DIR` (by default: `4S156Parcels` and `Schaefer2018N100n7Tian2020S1`).

#### Stage 6 — pyAFQ tractometry

[pyAFQ](https://yeatmanlab.github.io/pyAFQ/) recognises ~24 major white matter bundles from the iFOD2 tractogram using atlas-based bundle recognition and extracts tissue property profiles sampled at 100 nodes along each bundle. This produces along-tract scalar maps (FA, MD, DKI metrics, etc.) for tract-specific microstructure analysis.

### Completion check (specialized)

The number of `ses-*` subdirectories in the QSIRecon output must match the number of `ses-*` directories in the corresponding QSIPrep subject output. This ensures re-queuing when new sessions are processed by QSIPrep.

---

## Data flow summary

```
DICOM
  │
  ▼
bids_root/sub-XX/ses-YY/
  ├── anat/*_T1w.nii.gz
  ├── dwi/*_dir-AP_dwi.{nii.gz,bvec,bval}
  ├── dwi/*_dir-PA_dwi.nii.gz        ← b0 volumes only
  ├── fmap/*_acq-func_*.nii.gz        ← written by heudiconv
  ├── fmap/*_acq-dwi_dir-PA_epi.nii.gz  ← derived by bids_post
  ├── fmap/*_epi.json (IntendedFor)   ← updated by bids_post
  └── func/*_bold.nii.gz
  │
  ▼ (defacing)
  └── anat/*_acq-defaced_T1w.nii.gz
  │
  ▼ (qsiprep)
derivatives/qsiprep/sub-XX/ses-YY/dwi/
  ├── *_dwi_preproc.{nii.gz,bvec,bval}
  └── *_desc-image_qc.tsv
  │
  ├── ▼ (freesurfer)
  │ derivatives/freesurfer/sub-XX/
  │   ├── mri/ surf/ label/ stats/
  │   └── scripts/recon-all.done
  │
  ▼ (qsirecon — waits for both qsiprep + freesurfer)
derivatives/qsirecon-MRtrix3_act-HSVS/sub-XX/ses-YY/dwi/
  ├── *DIPYDKI*.nii.gz          ← DKI scalars
  ├── *AMICONODDI*.nii.gz       ← NODDI scalars
  ├── *DIPYMAPMRI*.nii.gz       ← MAP-MRI scalars
  ├── *DSIStudio*.nii.gz        ← GQI scalars
  ├── *MRtrix3_act-HSVS*.tck    ← tractograms (iFOD2, SD_Stream)
  ├── *MRtrix3_act-HSVS*_connectivity*.csv  ← connectivity matrices
  └── *pyAFQ_TRACTOMETRY*/      ← along-tract profiles
```

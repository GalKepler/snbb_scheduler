# Completion Markers

The `completion_marker` field on a `Procedure` controls how the scheduler decides whether a procedure's output is already complete. Conservative checks mean a procedure is re-run if the output looks incomplete.

## Types of markers

### `null` — directory must be non-empty

```yaml
completion_marker: null
```

The output directory must exist **and** contain at least one file or subdirectory.

**Used by:** `qsirecon`

---

### Single glob pattern

```yaml
completion_marker: "**/*.nii.gz"
```

At least one file matching the glob must exist inside the output directory (using `Path.glob`).

**Examples:**

```yaml
completion_marker: "fmap/*acq-dwi*_epi.nii.gz"   # bids_post: derived fieldmap written
completion_marker: "anat/*acq-defaced*_T1w.nii.gz" # defacing: defaced T1w present
completion_marker: "scripts/recon-all.done"         # freesurfer: exact file (no glob chars)
```

!!! note "Glob vs. literal path"
    If the marker contains `*`, `?`, or `[`, it is treated as a glob pattern.
    Otherwise it is treated as a literal relative path inside the output directory.

---

### List of glob patterns — ALL must match

```yaml
completion_marker:
  - "anat/*_T1w.nii.gz"
  - "dwi/*dir-AP*_dwi.nii.gz"
  - "dwi/*dir-AP*_dwi.bvec"
  - "dwi/*dir-AP*_dwi.bval"
```

Every pattern in the list must match at least one file. If any pattern has no match, the procedure is considered incomplete.

**Used by:** `bids` (checks all expected modalities), `qsiprep` (checks all output DWI files)

---

## Output path context

The marker is evaluated relative to the procedure's **output directory** for the given subject/session:

| Scope | Output path |
|---|---|
| `session` | `<proc_root>/sub-XXXX/ses-YYYY/` |
| `subject` | `<proc_root>/sub-XXXX/` |

Where `proc_root` is:
- `bids_root` when `output_dir` is empty (bids, bids_post, defacing)
- `derivatives_root/<output_dir>` otherwise

---

## Specialized checks

For `freesurfer`, `qsiprep`, and `qsirecon` the completion marker is augmented by specialized logic that also checks **session count consistency**:

- **freesurfer**: `scripts/recon-all.done` must exist, **and** the number of `-i` inputs in `recon-all.done`'s `CMDARGS` line must equal the number of T1w files currently available in the BIDS dataset for that subject
- **qsiprep**: at least one `ses-*` subdirectory must exist in the output, **and** the count must equal the number of BIDS sessions with DWI data for that subject
- **qsirecon**: similar to qsiprep but compares against the number of QSIPrep sessions

These checks prevent a procedure from appearing "complete" when new sessions have been added after it was originally run.

---

## Choosing the right marker

| Situation | Recommended marker |
|---|---|
| Tool writes a known completion flag file | `"scripts/recon-all.done"` or similar |
| Tool writes output NIfTI files in predictable locations | `"**/*.nii.gz"` or a specific glob |
| Tool writes many files, all-or-nothing | List of critical output globs |
| Tool output is non-empty directory (coarse check) | `null` |

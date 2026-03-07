# Completion Markers

The `completion_marker` field on a `Procedure` controls how the scheduler decides whether a procedure's output is already complete. Conservative checks mean a procedure is re-run if the output looks incomplete.

## Types of markers

### `null` — directory must be non-empty

```yaml
completion_marker: null
```

The output directory must exist **and** contain at least one file or subdirectory.

**Used by:** `freesurfer` (the specialized check overrides this; `null` here signals "use the registered check")

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

**Used by:** `bids` (checks all expected modalities), `qsiprep` (checks HTML report + all preproc DWI files)

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

Two procedures use registered specialized checks that replace marker evaluation entirely:

### `freesurfer`

Checks the full longitudinal FreeSurfer pipeline. For single-session subjects, verifies `<subject>/scripts/recon-all.done`. For multi-session subjects, verifies all three pipeline steps (cross-sectional, template, longitudinal) across every session.

Kwargs: `bids_root`, `subject` (both optional; falls back to checking `<path>/scripts/recon-all.done`).

### `qsirecon`

Session-scoped check. Verifies that an HTML report exists at `<qsirecon_root>/derivatives/qsirecon-<suffix>/<subject>_<session>.html`:

- **With `recon_spec`**: reads unique `qsirecon_suffix` values from the QSIRecon workflow YAML and checks each one individually (all must exist)
- **Without `recon_spec`**: wildcard — any `derivatives/*/<subject>_<session>.html` match suffices
- **Without session context**: falls back to non-empty directory check

Kwargs: `derivatives_root`, `subject`, `session` (required for session-level check); `recon_spec` (optional path to the QSIRecon workflow YAML).

---

## Choosing the right marker

| Situation | Recommended marker |
|---|---|
| Tool writes a known completion flag file | `"scripts/recon-all.done"` or similar |
| Tool writes output NIfTI files in predictable locations | `"**/*.nii.gz"` or a specific glob |
| Tool writes many files, all-or-nothing | List of critical output globs |
| Tool output is non-empty directory (coarse check) | `null` |

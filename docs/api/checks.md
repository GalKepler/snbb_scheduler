# `snbb_scheduler.checks`

Completion checking for procedure outputs.

```python
from snbb_scheduler.checks import is_complete
```

---

## `is_complete(proc, output_path, **kwargs)`

Return `True` if a procedure's output is considered complete.

```python
from snbb_scheduler.checks import is_complete
from pathlib import Path

result = is_complete(proc, output_path)
```

**Parameters:**
- `proc` — `Procedure` instance
- `output_path` — `Path` to the procedure's output directory for this subject/session
- `**kwargs` — passed to specialized check functions (see below)

**Returns:** `bool`

### Completion logic

| `proc.completion_marker` | Completion criterion |
|---|---|
| `None` | Output directory exists and is non-empty |
| `"path/to/file"` | That specific file exists inside `output_path` |
| `"**/*.nii.gz"` | At least one file matching the glob exists |
| `["pat1", "pat2"]` | ALL patterns match at least one file each |

If `output_path` does not exist, always returns `False`.

### Examples

```python
from pathlib import Path
from snbb_scheduler.config import Procedure
from snbb_scheduler.checks import is_complete

# Check a BIDS session (list marker)
bids_proc = Procedure(
    name="bids", output_dir="", script="snbb_run_bids.sh",
    completion_marker=["anat/*_T1w.nii.gz", "dwi/*_dwi.nii.gz"]
)
path = Path("/data/snbb/bids/sub-0001/ses-202407110849")
print(is_complete(bids_proc, path))  # True if both patterns match

# Check freesurfer with session-count validation
fs_proc = Procedure(
    name="freesurfer", output_dir="freesurfer", script="snbb_run_freesurfer.sh",
    scope="subject", completion_marker="scripts/recon-all.done"
)
path = Path("/data/snbb/derivatives/freesurfer/sub-0001")
print(is_complete(
    fs_proc, path,
    bids_root=Path("/data/snbb/bids"),
    subject="sub-0001",
))
```

---

## Specialized checks

Two procedures use custom completion logic that goes beyond the marker pattern:

### `freesurfer`

`is_complete(proc, path, bids_root=..., subject=...)` checks the full longitudinal pipeline:

- **Single-session subjects**: `<subject>/scripts/recon-all.done` exists and is a success marker
- **Multi-session subjects**: verifies all three pipeline steps:
  1. Cross-sectional — `<subject>_<session>/scripts/recon-all.done` for every BIDS session
  2. Template — `<subject>/scripts/recon-all.done`
  3. Longitudinal — `<subject>_<session>.long.<subject>/scripts/recon-all.done` for every BIDS session

Without `bids_root` and `subject`, falls back to checking only `<path>/scripts/recon-all.done`.

### `qsirecon`

`is_complete(proc, path, derivatives_root=..., subject=..., session=..., recon_spec=None)` checks:

- **With `recon_spec`** (path to a QSIRecon workflow YAML): reads the unique `qsirecon_suffix` values from the YAML's `nodes` list and requires that `<qsirecon_root>/derivatives/qsirecon-<suffix>/<subject>_<session>.html` exists for every suffix. If the spec is unreadable or has no suffixes, falls back to wildcard.
- **Without `recon_spec`** (or empty spec): checks that any file matching `<qsirecon_root>/derivatives/*/<subject>_<session>.html` exists.
- **Without `derivatives_root`, `subject`, or `session`**: falls back to `_dir_nonempty(output_path)`.

```python
from snbb_scheduler.checks import is_complete
from pathlib import Path

proc = cfg.get_procedure("qsirecon")
path = cfg.get_procedure_root(proc) / "sub-0001" / "ses-01"

# Basic wildcard check
print(is_complete(proc, path,
    derivatives_root=cfg.derivatives_root,
    subject="sub-0001",
    session="ses-01",
))

# Spec-driven check (verifies every suffix HTML)
print(is_complete(proc, path,
    derivatives_root=cfg.derivatives_root,
    subject="sub-0001",
    session="ses-01",
    recon_spec=Path("/path/to/qsirecon_spec.yaml"),
))
```

---

## Registering a custom specialized check

If you add a procedure with complex completion logic, register a specialized check:

```python
from snbb_scheduler.checks import _register_check
from snbb_scheduler.config import Procedure
from pathlib import Path

@_register_check("myprocedure")
def _check_myprocedure(proc: Procedure, output_path: Path, **kwargs) -> bool:
    # Custom logic here
    return (output_path / "DONE").exists()
```

!!! note
    `_register_check` is a private API. For most procedures, the standard `completion_marker` is sufficient.

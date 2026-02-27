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

Three procedures use custom completion logic that goes beyond the marker pattern:

### `freesurfer`

`is_complete(proc, path, bids_root=..., subject=...)` checks:
1. `scripts/recon-all.done` must exist
2. The number of `-i` inputs recorded in `recon-all.done`'s `#CMDARGS` line must equal the number of T1w NIfTI files currently available in the BIDS dataset for that subject

Without `bids_root` and `subject`, falls back to checking only for the marker file.

### `qsiprep`

`is_complete(proc, path, bids_root=..., subject=...)` checks:
1. At least one `ses-*` subdirectory exists in the QSIPrep subject output
2. The count of `ses-*` subdirectories equals the count of BIDS sessions with DWI data

### `qsirecon`

`is_complete(proc, path, derivatives_root=..., subject=...)` checks:
1. At least one `ses-*` subdirectory exists in the QSIRecon subject output
2. The count matches the number of `ses-*` subdirectories in the corresponding QSIPrep output

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

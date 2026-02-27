# `snbb_scheduler.config`

Procedure declarations and scheduler configuration.

```python
from snbb_scheduler.config import Procedure, SchedulerConfig, DEFAULT_PROCEDURES
```

---

## `Procedure`

Declaration of a single processing procedure.

```python
@dataclass
class Procedure:
    name: str
    output_dir: str
    script: str
    scope: Literal["session", "subject"] = "session"
    depends_on: list[str] = field(default_factory=list)
    completion_marker: str | list[str] | None = None
```

### Fields

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Unique identifier used throughout the scheduler |
| `output_dir` | `str` | Subdirectory under `derivatives_root`; `""` means outputs go in `bids_root` |
| `script` | `str` | Shell script filename passed to `sbatch` |
| `scope` | `"session"` or `"subject"` | Whether one job runs per session or per subject |
| `depends_on` | `list[str]` | Names of procedures that must complete first |
| `completion_marker` | `str`, `list[str]`, or `None` | How to determine output is complete |

### Example

```python
from snbb_scheduler.config import Procedure

my_proc = Procedure(
    name="fmriprep",
    output_dir="fmriprep",
    script="snbb_run_fmriprep.sh",
    scope="session",
    depends_on=["bids"],
    completion_marker="**/*.html",
)
```

---

## `DEFAULT_PROCEDURES`

The built-in procedure list, in dependency order:

```python
DEFAULT_PROCEDURES: list[Procedure] = [
    # bids → bids_post → defacing
    # bids_post → qsiprep (subject-scoped)
    # bids_post → freesurfer (subject-scoped)
    # qsiprep + freesurfer → qsirecon (subject-scoped)
]
```

To add procedures without losing the defaults:

```python
from snbb_scheduler.config import DEFAULT_PROCEDURES, Procedure, SchedulerConfig

extra = Procedure(name="fmriprep", ...)
cfg = SchedulerConfig(
    procedures=list(DEFAULT_PROCEDURES) + [extra],
)
```

---

## `SchedulerConfig`

All path conventions and settings in one dataclass.

```python
@dataclass
class SchedulerConfig:
    dicom_root: Path = Path("/data/snbb/dicom")
    bids_root: Path = Path("/data/snbb/bids")
    derivatives_root: Path = Path("/data/snbb/derivatives")
    slurm_partition: str = "debug"
    slurm_account: str = "snbb"
    slurm_mem: str | None = None
    slurm_cpus_per_task: int | None = None
    state_file: Path = Path("/data/snbb/.scheduler_state.parquet")
    slurm_log_dir: Path | None = None
    log_file: Path | None = None
    sessions_file: Path | None = None
    procedures: list[Procedure] = field(default_factory=lambda: list(DEFAULT_PROCEDURES))
```

### `SchedulerConfig.from_yaml(path)`

Load config from a YAML file, overriding defaults.

```python
cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
```

**Raises:**
- `FileNotFoundError` — if the path does not exist
- `ValueError` — if the file contains invalid YAML
- `ValueError` — if any `depends_on` references an unknown procedure name

### `SchedulerConfig.get_procedure(name)`

Look up a procedure by name.

```python
proc = cfg.get_procedure("freesurfer")
# raises KeyError if not found
```

### `SchedulerConfig.get_procedure_root(proc)`

Return the base output directory for a procedure.

```python
root = cfg.get_procedure_root(proc)
# proc.output_dir == ""  →  returns cfg.bids_root
# proc.output_dir != ""  →  returns cfg.derivatives_root / proc.output_dir
```

### `__post_init__` validation

On construction, `SchedulerConfig` validates that every procedure's `depends_on` references a known procedure name. This prevents silent misconfiguration:

```python
# This raises ValueError at construction time:
SchedulerConfig(procedures=[
    Procedure(name="qsiprep", depends_on=["typo_name"], ...)
])
# ValueError: Procedure 'qsiprep' depends on 'typo_name', which is not in the procedures list.
```

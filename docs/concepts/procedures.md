# Procedures

A **procedure** is one step in the neuroimaging pipeline. It is declared as a `Procedure` dataclass instance and carries all the metadata the scheduler needs to decide when to run it and how to submit it.

## The `Procedure` dataclass

```python
@dataclass
class Procedure:
    name: str                                    # unique identifier, e.g. "qsiprep"
    output_dir: str                              # subdirectory under derivatives_root
    script: str                                  # sbatch script filename
    scope: Literal["session", "subject"] = "session"
    depends_on: list[str] = field(default_factory=list)
    completion_marker: str | list[str] | None = None
```

### Fields

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Unique identifier used in the state file, manifest, and CLI filters |
| `output_dir` | `str` | Subdirectory under `derivatives_root`; empty string means outputs go in `bids_root` |
| `script` | `str` | Shell script filename passed to `sbatch` |
| `scope` | `"session"` or `"subject"` | Whether one job is run per session or one per subject |
| `depends_on` | `list[str]` | Names of procedures that must be complete before this one runs |
| `completion_marker` | `str`, `list[str]`, or `None` | How to decide the output is complete — see [Completion Markers](../configuration/completion-markers.md) |

---

## Default procedures

The scheduler ships with six built-in procedures:

| Name | Scope | Depends on | Output |
|---|---|---|---|
| `bids` | session | *(nothing)* | `bids_root/sub-XX/ses-YY/` |
| `bids_post` | session | `bids` | `bids_root/sub-XX/ses-YY/fmap/` |
| `defacing` | session | `bids_post` | `bids_root/sub-XX/ses-YY/anat/*acq-defaced*` |
| `qsiprep` | **subject** | `bids_post` | `derivatives_root/qsiprep/sub-XX/` |
| `freesurfer` | **subject** | `bids_post` | `derivatives_root/freesurfer/sub-XX/` |
| `qsirecon` | **subject** | `qsiprep`, `freesurfer` | `derivatives_root/qsirecon-MRtrix3_act-HSVS/sub-XX/` |

### Dependency graph

```
bids
 └── bids_post
      ├── defacing
      ├── qsiprep ──┐
      └── freesurfer─┤
                     └── qsirecon
```

---

## Subject scope vs. session scope

When a procedure has `scope: subject`, the scheduler:

- computes one output path per subject (`derivatives_root/<name>/sub-XXXX`)
- deduplicates: even if a subject has 3 sessions, only **one** job is submitted
- passes only `subject` as an argument to the script (not `session`)

When `scope: session` (the default):

- each `(subject, session)` pair gets its own job
- the script receives `subject` and `session` as positional arguments

---

## Adding a procedure

See the [Adding a Procedure guide](../guides/adding-procedure.md) for step-by-step instructions.

"""add_procedure.py — demonstrates adding a new procedure at runtime.

This script shows two patterns for extending the pipeline:
  A. Appending to the default procedure list in Python
  B. Building a config entirely from scratch

Neither pattern requires any changes to the snbb_scheduler source code.

Run with:
    python examples/add_procedure.py
"""

from snbb_scheduler.config import DEFAULT_PROCEDURES, Procedure, SchedulerConfig
from snbb_scheduler.manifest import build_manifest
from snbb_scheduler.rules import build_rules
from snbb_scheduler.sessions import discover_sessions

# ---------------------------------------------------------------------------
# Pattern A: Load a YAML config and append an extra procedure
# ---------------------------------------------------------------------------

print("Pattern A: append qsirecon and fmriprep to an existing config")
print("-" * 60)

cfg_a = SchedulerConfig(
    # Using in-memory defaults — replace with SchedulerConfig.from_yaml(path)
    # for a real deployment.
)

qsirecon = Procedure(
    name="qsirecon",
    output_dir="qsirecon",
    script="snbb_run_qsirecon.sh",
    scope="session",
    depends_on=["qsiprep"],          # won't fire until qsiprep is complete
    completion_marker=None,          # non-empty directory = done
)

fmriprep = Procedure(
    name="fmriprep",
    output_dir="fmriprep",
    script="snbb_run_fmriprep.sh",
    scope="session",
    depends_on=["bids"],             # runs in parallel with qsiprep
    completion_marker="**/*.html",   # fmriprep writes an HTML report on success
)

cfg_a.procedures.extend([qsirecon, fmriprep])

print("Registered procedures:")
for proc in cfg_a.procedures:
    deps = ", ".join(proc.depends_on) if proc.depends_on else "—"
    print(f"  {proc.name:<15} scope={proc.scope:<8} depends_on=[{deps}]")

# Rules are generated automatically from the updated procedure list
rules = build_rules(cfg_a)
print(f"\nAuto-generated rules: {list(rules.keys())}")


# ---------------------------------------------------------------------------
# Pattern B: Build a fully custom config from scratch (e.g. for a new site)
# ---------------------------------------------------------------------------

print()
print("Pattern B: custom config with a subject-scoped postprocessing step")
print("-" * 60)

# A hypothetical "connectome" procedure that runs per-subject after freesurfer
connectome = Procedure(
    name="connectome",
    output_dir="connectome",
    script="snbb_run_connectome.sh",
    scope="subject",                      # one run covers all sessions
    depends_on=["freesurfer", "qsirecon"],# needs both upstream outputs
    completion_marker="connectome.done",
)

cfg_b = SchedulerConfig(
    procedures=[*DEFAULT_PROCEDURES, connectome],
)

print("Dependency graph:")
for proc in cfg_b.procedures:
    arrow = " → " + ", ".join(proc.depends_on) if proc.depends_on else ""
    print(f"  {proc.name}{arrow}")


# ---------------------------------------------------------------------------
# Show how sessions DataFrame gains columns automatically
# ---------------------------------------------------------------------------

print()
print("Session DataFrame columns with extended config:")
print("-" * 60)

from snbb_scheduler.manifest import _empty_state  # noqa: E402 — demo only
import pandas as pd  # noqa: E402

# Simulate an empty sessions DataFrame with the right columns
# (discover_sessions against a real dicom_root would populate the rows)
empty = build_manifest(pd.DataFrame(columns=list(
    ["subject", "session", "dicom_path", "dicom_exists"]
    + [col for proc in cfg_b.procedures for col in (f"{proc.name}_path", f"{proc.name}_exists")]
)), cfg_b)

print("Manifest columns:", list(empty.columns))
print()
print("Path columns that would appear in the sessions DataFrame:")
for proc in cfg_b.procedures:
    print(f"  {proc.name}_path, {proc.name}_exists")

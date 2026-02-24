from __future__ import annotations

__all__ = ["build_manifest", "load_state", "save_state", "filter_in_flight"]

from pathlib import Path

import pandas as pd

from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.rules import build_rules

# Columns and dtypes for the state parquet file
_STATE_COLUMNS = {
    "subject": "object",
    "session": "object",
    "procedure": "object",
    "status": "object",
    "submitted_at": "datetime64[ns]",
    "job_id": "object",
}


def build_manifest(
    sessions: pd.DataFrame,
    config: SchedulerConfig,
    force: bool = False,
    force_procedures: list[str] | None = None,
) -> pd.DataFrame:
    """Evaluate rules against all sessions and return a task manifest.

    Returns a DataFrame with columns:
        subject, session, procedure, dicom_path, priority

    priority reflects the order of procedures in config.procedures
    (lower index = higher priority = submitted first).

    When *force* is True, the self-completion check is skipped for all
    procedures (or only those in *force_procedures* when provided), so
    already-complete procedures are resubmitted.
    """
    if sessions.empty:
        return pd.DataFrame(columns=["subject", "session", "procedure", "dicom_path", "priority"])

    rules = build_rules(config, force=force, force_procedures=force_procedures)
    priority = {proc.name: i for i, proc in enumerate(config.procedures)}
    subject_scoped = {proc.name for proc in config.procedures if proc.scope == "subject"}

    rows = []
    seen_subject_procs: set[tuple[str, str]] = set()
    for _, session_row in sessions.iterrows():
        for proc_name, rule in rules.items():
            if not rule(session_row):
                continue
            subject = session_row["subject"]
            if proc_name in subject_scoped:
                key = (subject, proc_name)
                if key in seen_subject_procs:
                    continue
                seen_subject_procs.add(key)
                session = ""
            else:
                session = session_row["session"]
            rows.append({
                "subject": subject,
                "session": session,
                "procedure": proc_name,
                "dicom_path": session_row["dicom_path"],
                "priority": priority[proc_name],
            })

    if not rows:
        return pd.DataFrame(columns=["subject", "session", "procedure", "dicom_path", "priority"])

    return pd.DataFrame(rows).sort_values("priority").reset_index(drop=True)


def load_state(config: SchedulerConfig) -> pd.DataFrame:
    """Load the state parquet file.

    Returns an empty DataFrame with the correct schema if the file does not exist.
    """
    if not Path(config.state_file).exists():
        return _empty_state()
    return pd.read_parquet(config.state_file)


def save_state(state: pd.DataFrame, config: SchedulerConfig) -> None:
    """Persist the state DataFrame to the parquet state file."""
    Path(config.state_file).parent.mkdir(parents=True, exist_ok=True)
    state.to_parquet(config.state_file, index=False)


def filter_in_flight(manifest: pd.DataFrame, state: pd.DataFrame) -> pd.DataFrame:
    """Remove tasks that are already pending or running in the state file."""
    if manifest.empty or state.empty:
        return manifest

    in_flight = state[state["status"].isin(["pending", "running"])][
        ["subject", "session", "procedure"]
    ]
    if in_flight.empty:
        return manifest

    merged = manifest.merge(
        in_flight, on=["subject", "session", "procedure"], how="left", indicator=True
    )
    return (
        merged[merged["_merge"] == "left_only"]
        .drop(columns="_merge")
        .reset_index(drop=True)
    )


def _empty_state() -> pd.DataFrame:
    """Return an empty DataFrame with the correct state schema and dtypes."""
    return pd.DataFrame(
        {col: pd.Series(dtype=dtype) for col, dtype in _STATE_COLUMNS.items()}
    )

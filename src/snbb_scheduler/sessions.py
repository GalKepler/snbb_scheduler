from __future__ import annotations

import pandas as pd

from snbb_scheduler.config import SchedulerConfig


def discover_sessions(config: SchedulerConfig) -> pd.DataFrame:
    """Scan filesystem and return DataFrame of all sessions with path info.

    Walks config.dicom_root looking for sub-*/ses-* directories and returns
    one row per (subject, session) pair, enriched with output paths and
    existence booleans for every registered procedure.

    Columns:
        subject, session, dicom_path, dicom_exists,
        <proc>_path, <proc>_exists  (one pair per procedure in config.procedures)
    """
    if not config.dicom_root.exists():
        return _empty_dataframe(config)

    rows = []
    for subject_dir in sorted(config.dicom_root.iterdir()):
        if not subject_dir.is_dir() or not subject_dir.name.startswith("sub-"):
            continue
        for session_dir in sorted(subject_dir.iterdir()):
            if not session_dir.is_dir() or not session_dir.name.startswith("ses-"):
                continue
            rows.append(_build_row(subject_dir.name, session_dir.name, session_dir, config))

    if not rows:
        return _empty_dataframe(config)

    return pd.DataFrame(rows)


def _build_row(subject: str, session: str, dicom_path, config: SchedulerConfig) -> dict:
    row: dict = {
        "subject": subject,
        "session": session,
        "dicom_path": dicom_path,
        "dicom_exists": dicom_path.exists(),
    }
    for proc in config.procedures:
        root = config.get_procedure_root(proc)
        if proc.scope == "subject":
            path = root / subject
        else:
            path = root / subject / session
        row[f"{proc.name}_path"] = path
        row[f"{proc.name}_exists"] = path.exists()
    return row


def _empty_dataframe(config: SchedulerConfig) -> pd.DataFrame:
    columns = ["subject", "session", "dicom_path", "dicom_exists"]
    for proc in config.procedures:
        columns += [f"{proc.name}_path", f"{proc.name}_exists"]
    return pd.DataFrame(columns=columns)

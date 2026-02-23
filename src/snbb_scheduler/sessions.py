from __future__ import annotations
from pathlib import Path
from typing import Union

__all__ = ["discover_sessions"]

import pandas as pd

from snbb_scheduler.config import SchedulerConfig


def sanitize_subject_code(subject_code: str) -> str:
    """Remove special characters and zero-pad to 4 digits."""
    return subject_code.replace("-", "").replace("_", "").replace(" ", "").zfill(4)


def sanitize_session_id(session_id: Union[str, int, float]) -> str:
    """Convert to string, clean, and zero-pad to 12 digits."""
    if isinstance(session_id, float):
        if pd.isna(session_id):
            return ""
        session_str = str(int(session_id))
    else:
        session_str = str(session_id)
    return session_str.replace("-", "").replace("_", "").replace(" ", "").zfill(12)

def load_sessions(csv_path: Union[str, Path]) -> pd.DataFrame:
    """Load and sanitize a linked_sessions CSV file.

    Expects columns ``SubjectCode``, ``ScanID``, and ``dicom_path``.
    Returns a deduplicated DataFrame with ``subject_code``, ``session_id``,
    and ``dicom_path`` columns. Rows with a missing ``dicom_path`` are dropped.

    Parameters
    ----------
    csv_path:
        Path to the linked_sessions.csv file.
    """
    df = pd.read_csv(csv_path)
    df["subject_code"] = df["SubjectCode"].apply(sanitize_subject_code)
    df["session_id"] = df["ScanID"].apply(sanitize_session_id)
    df = df.dropna(subset=["dicom_path"]).reset_index(drop=True)
    return df.drop_duplicates(subset=["subject_code", "session_id"]).reset_index(
        drop=True
    )

def discover_sessions(config: SchedulerConfig) -> pd.DataFrame:
    """Return DataFrame of all sessions with path info.

    When config.sessions_file is set, reads session list from that CSV
    (columns: subject_code, session_id, ScanID) and maps ScanID to flat
    DICOM subdirectories under dicom_root.

    Otherwise, walks config.dicom_root looking for sub-*/ses-* directories.

    Columns in both cases:
        subject, session, dicom_path, dicom_exists,
        <proc>_path, <proc>_exists  (one pair per procedure in config.procedures)
    """
    if config.sessions_file is not None:
        return _discover_from_file(config)

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


def _discover_from_file(config: SchedulerConfig) -> pd.DataFrame:
    """Build session DataFrame from a CSV file.

    Expected CSV columns: subject_code, session_id, ScanID.
    DICOM path is config.dicom_root / ScanID (flat layout).
    """
    df_csv = load_sessions(config.sessions_file)
    if df_csv.empty:
        return _empty_dataframe(config)

    rows = []
    for _, row in df_csv.iterrows():
        subject = f"sub-{row['subject_code']}"
        session = f"ses-{row['session_id']}"
        dicom_path = config.dicom_root / str(row["ScanID"])
        rows.append(_build_row(subject, session, dicom_path, config))

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

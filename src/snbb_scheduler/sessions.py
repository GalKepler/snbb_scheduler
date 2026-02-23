from __future__ import annotations

__all__ = ["discover_sessions"]

from pathlib import Path

import pandas as pd

from snbb_scheduler.config import SchedulerConfig

# Columns required in the raw linked_sessions CSV (used by load_sessions)
_REQUIRED_CSV_COLUMNS = {"SubjectCode", "ScanID", "dicom_path"}

# Columns required in the pre-sanitized sessions file (used by _discover_from_file)
_SESSION_FILE_COLUMNS = {"subject_code", "session_id", "ScanID"}


def sanitize_subject_code(subject_code: str) -> str:
    """Remove special characters and zero-pad to 4 digits."""
    return subject_code.replace("-", "").replace("_", "").replace(" ", "").zfill(4)


def sanitize_session_id(session_id: str | int | float) -> str:
    """Convert to string, clean, and zero-pad to 12 digits."""
    if isinstance(session_id, float):
        if pd.isna(session_id):
            return ""
        session_str = str(int(session_id))
    else:
        session_str = str(session_id)
    return session_str.replace("-", "").replace("_", "").replace(" ", "").zfill(12)

def load_sessions(csv_path: str | Path) -> pd.DataFrame:
    """Load and sanitize a linked_sessions CSV file.

    Expects columns ``SubjectCode``, ``ScanID``, and ``dicom_path``.
    Returns a deduplicated DataFrame with ``subject_code``, ``session_id``,
    and ``dicom_path`` columns. Rows with a missing ``dicom_path`` are dropped.

    Parameters
    ----------
    csv_path:
        Path to the linked_sessions.csv file.

    Raises
    ------
    ValueError
        If the CSV is missing any of the required columns
        (``SubjectCode``, ``ScanID``, ``dicom_path``).
    """
    df = pd.read_csv(csv_path)
    missing = _REQUIRED_CSV_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"Sessions CSV {csv_path!r} is missing required column(s): "
            f"{sorted(missing)}. Found: {sorted(df.columns.tolist())}"
        )
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
    """Build session DataFrame from a pre-sanitized sessions CSV file.

    Reads the CSV at ``config.sessions_file`` directly (no sanitization
    applied). DICOM path is resolved as ``config.dicom_root / ScanID``
    (flat layout).

    Expected CSV columns:
        ``subject_code`` — BIDS-style subject code without the ``sub-`` prefix
        (e.g. ``"0001"``).
        ``session_id`` — BIDS-style session identifier without the ``ses-``
        prefix (e.g. ``"01"``).
        ``ScanID`` — scan directory name under ``dicom_root``.

    Raises
    ------
    ValueError
        If the CSV is missing any of the required columns.
    FileNotFoundError
        If ``config.sessions_file`` does not exist.
    """
    # dtype=str preserves zero-padded values (e.g. "0001" → "0001", not 1)
    df_csv = load_sessions(config.sessions_file)
    missing = _SESSION_FILE_COLUMNS - set(df_csv.columns)
    if missing:
        raise ValueError(
            f"Sessions file {config.sessions_file!r} is missing required column(s): "
            f"{sorted(missing)}. Found: {sorted(df_csv.columns.tolist())}"
        )
    if df_csv.empty:
        return _empty_dataframe(config)

    rows = []
    for _, row in df_csv.iterrows():
        subject = f"sub-{row['subject_code']}"
        session = f"ses-{row['session_id']}"
        dicom_path = config.dicom_root / str(row["ScanID"])
        rows.append(_build_row(subject, session, dicom_path, config))

    return pd.DataFrame(rows)


def _build_row(subject: str, session: str, dicom_path: Path, config: SchedulerConfig) -> dict:
    """Build a single session row dict with path and existence columns.

    Parameters
    ----------
    subject:
        BIDS subject label, e.g. ``"sub-0001"``.
    session:
        BIDS session label, e.g. ``"ses-01"``.
    dicom_path:
        Absolute path to this session's DICOM directory.
    config:
        Scheduler configuration used to resolve procedure output paths.

    Returns
    -------
    dict
        Keys: ``subject``, ``session``, ``dicom_path``, ``dicom_exists``,
        plus ``<proc>_path`` and ``<proc>_exists`` for every procedure in
        *config.procedures*.
    """
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
    """Return an empty DataFrame with the correct session schema.

    Includes the base columns (``subject``, ``session``, ``dicom_path``,
    ``dicom_exists``) plus ``<proc>_path`` and ``<proc>_exists`` for every
    procedure in *config.procedures*.
    """
    columns = ["subject", "session", "dicom_path", "dicom_exists"]
    for proc in config.procedures:
        columns += [f"{proc.name}_path", f"{proc.name}_exists"]
    return pd.DataFrame(columns=columns)

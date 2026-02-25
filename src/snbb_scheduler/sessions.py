from __future__ import annotations

__all__ = ["discover_sessions"]

from pathlib import Path

import pandas as pd

from snbb_scheduler.config import SchedulerConfig

# Columns required in the raw linked_sessions CSV (used by load_sessions)
_REQUIRED_CSV_COLUMNS = {"SubjectCode", "ScanID", "dicom_path"}

# Columns required in the pre-sanitized sessions file used by _discover_from_file
# (columns: subject_code, session_id, dicom_path — dicom_path may be NaN)
_SESSION_FILE_COLUMNS = {"subject_code", "session_id", "dicom_path"}


def sanitize_subject_code(subject_code: str | int | float) -> str:
    """Remove special characters and zero-pad to 4 digits."""
    if isinstance(subject_code, float):
        subject_code = str(int(subject_code))
    else:
        subject_code = str(subject_code)
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
    """Load and sanitize a raw linked_sessions CSV file.

    Expects columns ``SubjectCode``, ``ScanID``, and ``dicom_path``.
    Returns a deduplicated DataFrame with ``subject_code``, ``session_id``,
    and ``dicom_path`` columns. Rows where ``dicom_path`` is NaN are retained
    (the caller may use NaN to infer that the DICOM directory does not exist).

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
    return df.drop_duplicates(subset=["subject_code", "session_id"]).reset_index(
        drop=True
    )


def discover_sessions(config: SchedulerConfig) -> pd.DataFrame:
    """Return DataFrame of all sessions with path info.

    When ``config.sessions_file`` is set, reads session list from that CSV
    (columns: ``subject_code``, ``session_id``, ``dicom_path``) and uses the
    ``dicom_path`` column directly — no filesystem recheck is performed.
    A NaN ``dicom_path`` sets ``dicom_exists=False`` for that session.

    Otherwise, walks ``config.dicom_root`` looking for sub-*/ses-* directories.

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

    Reads the CSV at ``config.sessions_file`` directly.  The ``dicom_path``
    column is used as-is — no filesystem check is performed.  A NaN value
    in ``dicom_path`` indicates that the DICOM directory does not exist and
    sets ``dicom_exists=False`` for that session.

    Expected CSV columns:
        ``subject_code`` — BIDS-style subject code without the ``sub-`` prefix
        (e.g. ``"0001"``).
        ``session_id`` — BIDS-style session identifier without the ``ses-``
        prefix (e.g. ``"01"``).
        ``dicom_path`` — absolute path to the DICOM directory, or NaN/empty
        if the directory does not exist.

    Raises
    ------
    ValueError
        If the CSV is missing any of the required columns.
    FileNotFoundError
        If ``config.sessions_file`` does not exist.
    """
    # dtype=str preserves zero-padded values (e.g. "0001" → "0001", not 1)
    df_csv = load_sessions(config.sessions_file)  # also validates required columns and sanitizes
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
        raw_dicom = row["dicom_path"]
        if pd.isna(raw_dicom):
            dicom_path = None
            dicom_exists = False
        else:
            dicom_path = Path(str(raw_dicom))
            dicom_exists = True
        rows.append(_build_row(subject, session, dicom_path, config, dicom_exists=dicom_exists))

    return pd.DataFrame(rows)


def _build_row(
    subject: str,
    session: str,
    dicom_path: Path | None,
    config: SchedulerConfig,
    *,
    dicom_exists: bool | None = None,
) -> dict:
    """Build a single session row dict with path and existence columns.

    Parameters
    ----------
    subject:
        BIDS subject label, e.g. ``"sub-0001"``.
    session:
        BIDS session label, e.g. ``"ses-01"``.
    dicom_path:
        Absolute path to this session's DICOM directory, or ``None`` when
        the path is not known.
    config:
        Scheduler configuration used to resolve procedure output paths.
    dicom_exists:
        When provided, this value is used directly without a filesystem check.
        Useful when ``dicom_path`` comes from a CSV where NaN already encodes
        absence.  If ``None`` (default), existence is checked via
        ``dicom_path.exists()``.

    Returns
    -------
    dict
        Keys: ``subject``, ``session``, ``dicom_path``, ``dicom_exists``,
        plus ``<proc>_path`` and ``<proc>_exists`` for every procedure in
        *config.procedures*.
    """
    if dicom_exists is None:
        actual_exists = dicom_path is not None and dicom_path.exists()
    else:
        actual_exists = dicom_exists

    row: dict = {
        "subject": subject,
        "session": session,
        "dicom_path": dicom_path,
        "dicom_exists": actual_exists,
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

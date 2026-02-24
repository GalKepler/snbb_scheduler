from pathlib import Path

import pandas as pd
import pytest

import math

from snbb_scheduler.config import DEFAULT_PROCEDURES, Procedure, SchedulerConfig
from snbb_scheduler.sessions import discover_sessions, load_sessions, sanitize_session_id


# ---------------------------------------------------------------------------
# sanitize_session_id — float / NaN handling
# ---------------------------------------------------------------------------


def test_sanitize_session_id_float_nan_returns_empty_string():
    """A float NaN session ID (e.g. from missing CSV value) returns an empty string."""
    assert sanitize_session_id(float("nan")) == ""


def test_sanitize_session_id_float_numeric_converts_to_int_string():
    """A float like 1.0 is treated as integer 1 and zero-padded."""
    result = sanitize_session_id(1.0)
    assert result == "1".zfill(12)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_discovers_correct_subjects_and_sessions(fake_config):
    df = discover_sessions(fake_config)
    assert set(df["subject"]) == {"sub-0001", "sub-0002"}
    assert list(df["session"]) == ["ses-01", "ses-01"]


def test_returns_dataframe(fake_config):
    df = discover_sessions(fake_config)
    assert isinstance(df, pd.DataFrame)


def test_empty_dicom_root_returns_empty_dataframe(tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "nonexistent",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    df = discover_sessions(cfg)
    assert df.empty
    assert "subject" in df.columns
    assert "session" in df.columns


def test_dicom_root_exists_but_no_sessions(tmp_path):
    """dicom_root exists but contains no sub-*/ses-* directories."""
    dicom = tmp_path / "dicom"
    dicom.mkdir()
    (dicom / "README").touch()  # a file, not a subject dir
    cfg = SchedulerConfig(
        dicom_root=dicom,
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    df = discover_sessions(cfg)
    assert df.empty
    assert "subject" in df.columns


def test_empty_dicom_root_has_procedure_columns(tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "nonexistent",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    df = discover_sessions(cfg)
    for proc in cfg.procedures:
        assert f"{proc.name}_path" in df.columns
        assert f"{proc.name}_exists" in df.columns


def test_non_subject_dirs_are_ignored(tmp_path):
    dicom = tmp_path / "dicom"
    (dicom / "sub-0001" / "ses-01").mkdir(parents=True)
    (dicom / "README").touch()                         # file
    (dicom / "misc_dir").mkdir()                       # dir without sub- prefix
    cfg = SchedulerConfig(
        dicom_root=dicom,
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    df = discover_sessions(cfg)
    assert list(df["subject"]) == ["sub-0001"]


def test_non_session_dirs_are_ignored(tmp_path):
    dicom = tmp_path / "dicom"
    (dicom / "sub-0001" / "ses-01").mkdir(parents=True)
    (dicom / "sub-0001" / "notasession").mkdir()
    cfg = SchedulerConfig(
        dicom_root=dicom,
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    df = discover_sessions(cfg)
    assert list(df["session"]) == ["ses-01"]


# ---------------------------------------------------------------------------
# Path enrichment
# ---------------------------------------------------------------------------


def test_dicom_path_points_to_session_dir(fake_config, fake_data_dir):
    df = discover_sessions(fake_config)
    row = df[df["subject"] == "sub-0001"].iloc[0]
    assert row["dicom_path"] == fake_data_dir / "dicom" / "sub-0001" / "ses-01"


def test_session_scoped_procedure_path(fake_config, fake_data_dir):
    df = discover_sessions(fake_config)
    row = df[df["subject"] == "sub-0001"].iloc[0]
    # bids is session-scoped
    assert row["bids_path"] == fake_data_dir / "bids" / "sub-0001" / "ses-01"
    # qsiprep is subject-scoped — path ends at subject, no session component
    assert row["qsiprep_path"] == fake_data_dir / "derivatives" / "qsiprep" / "sub-0001"


def test_subject_scoped_procedure_path(fake_config, fake_data_dir):
    df = discover_sessions(fake_config)
    # freesurfer is subject-scoped: path should NOT include session
    row = df[df["subject"] == "sub-0001"].iloc[0]
    assert row["freesurfer_path"] == fake_data_dir / "derivatives" / "freesurfer" / "sub-0001"


def test_subject_scoped_path_same_across_sessions(tmp_path):
    """Both sessions of the same subject share the same freesurfer_path."""
    dicom = tmp_path / "dicom"
    (dicom / "sub-0001" / "ses-01").mkdir(parents=True)
    (dicom / "sub-0001" / "ses-02").mkdir(parents=True)
    cfg = SchedulerConfig(
        dicom_root=dicom,
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    df = discover_sessions(cfg)
    fs_paths = df["freesurfer_path"].unique()
    assert len(fs_paths) == 1  # same subject → same path


# ---------------------------------------------------------------------------
# Existence booleans
# ---------------------------------------------------------------------------


def test_dicom_exists_true_when_dir_present(fake_config):
    df = discover_sessions(fake_config)
    assert df["dicom_exists"].all()


def test_bids_exists_true_only_for_sub0001(fake_config):
    df = discover_sessions(fake_config)
    sub01 = df[df["subject"] == "sub-0001"].iloc[0]
    sub02 = df[df["subject"] == "sub-0002"].iloc[0]
    assert sub01["bids_exists"] is True or sub01["bids_exists"] == True
    assert sub02["bids_exists"] is False or sub02["bids_exists"] == False


def test_derivative_exists_false_when_not_created(fake_config):
    df = discover_sessions(fake_config)
    assert not df["qsiprep_exists"].any()
    assert not df["freesurfer_exists"].any()


def test_existence_reflects_filesystem(tmp_path):
    dicom = tmp_path / "dicom"
    (dicom / "sub-0001" / "ses-01").mkdir(parents=True)
    qsiprep_out = tmp_path / "derivatives" / "qsiprep" / "sub-0001" / "ses-01"
    qsiprep_out.mkdir(parents=True)
    cfg = SchedulerConfig(
        dicom_root=dicom,
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    df = discover_sessions(cfg)
    assert df.iloc[0]["qsiprep_exists"] == True


# ---------------------------------------------------------------------------
# Dynamic procedures
# ---------------------------------------------------------------------------


def test_custom_procedure_columns_present(tmp_path):
    """Extra procedures registered in config appear as columns."""
    dicom = tmp_path / "dicom"
    (dicom / "sub-0001" / "ses-01").mkdir(parents=True)
    fmriprep = Procedure(
        name="fmriprep",
        output_dir="fmriprep",
        script="snbb_run_fmriprep.sh",
        depends_on=["bids"],
    )
    cfg = SchedulerConfig(
        dicom_root=dicom,
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        procedures=[*DEFAULT_PROCEDURES, fmriprep],
    )
    df = discover_sessions(cfg)
    assert "fmriprep_path" in df.columns
    assert "fmriprep_exists" in df.columns


def test_custom_procedure_path_value(tmp_path):
    dicom = tmp_path / "dicom"
    (dicom / "sub-0001" / "ses-01").mkdir(parents=True)
    fmriprep = Procedure(
        name="fmriprep",
        output_dir="fmriprep",
        script="snbb_run_fmriprep.sh",
    )
    cfg = SchedulerConfig(
        dicom_root=dicom,
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        procedures=[fmriprep],
    )
    df = discover_sessions(cfg)
    assert df.iloc[0]["fmriprep_path"] == tmp_path / "derivatives" / "fmriprep" / "sub-0001" / "ses-01"


# ---------------------------------------------------------------------------
# File-based discovery
# ---------------------------------------------------------------------------


def test_file_discovery_subject_session_values(fake_sessions_config):
    df = discover_sessions(fake_sessions_config)
    assert set(df["subject"]) == {"sub-0001", "sub-0002"}
    assert list(df["session"]) == ["ses-01", "ses-01"]


def test_file_discovery_bids_prefix_added(fake_sessions_config):
    df = discover_sessions(fake_sessions_config)
    assert all(s.startswith("sub-") for s in df["subject"])
    assert all(s.startswith("ses-") for s in df["session"])


def test_file_discovery_dicom_path_from_csv(fake_sessions_config, fake_sessions_csv):
    """dicom_path is taken directly from the sessions CSV, not reconstructed."""
    df = discover_sessions(fake_sessions_config)
    row = df[df["subject"] == "sub-0001"].iloc[0]
    assert row["dicom_path"] == fake_sessions_csv / "dicom" / "SCAN001"


def test_file_discovery_dicom_exists_true_when_dir_present(fake_sessions_config):
    df = discover_sessions(fake_sessions_config)
    assert df["dicom_exists"].all()


def test_file_discovery_dicom_exists_false_from_nan_in_csv(tmp_path):
    """A NaN dicom_path in the sessions file sets dicom_exists=False (no filesystem check)."""
    csv = tmp_path / "sessions.csv"
    import math
    pd.DataFrame([
        {"subject_code": "0001", "session_id": "01", "dicom_path": math.nan},
    ]).to_csv(csv, index=False)
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        sessions_file=csv,
    )
    df = discover_sessions(cfg)
    assert not df.iloc[0]["dicom_exists"]


def test_file_discovery_procedure_columns_present(fake_sessions_config):
    df = discover_sessions(fake_sessions_config)
    for proc in fake_sessions_config.procedures:
        assert f"{proc.name}_path" in df.columns
        assert f"{proc.name}_exists" in df.columns


def test_file_discovery_session_scoped_procedure_path(fake_sessions_config, fake_sessions_csv):
    df = discover_sessions(fake_sessions_config)
    row = df[df["subject"] == "sub-0001"].iloc[0]
    # bids is session-scoped
    assert row["bids_path"] == fake_sessions_csv / "bids" / "sub-0001" / "ses-01"
    # qsiprep is subject-scoped — path ends at subject, no session component
    assert row["qsiprep_path"] == fake_sessions_csv / "derivatives" / "qsiprep" / "sub-0001"


def test_file_discovery_subject_scoped_procedure_path(fake_sessions_config, fake_sessions_csv):
    df = discover_sessions(fake_sessions_config)
    row = df[df["subject"] == "sub-0001"].iloc[0]
    assert row["freesurfer_path"] == fake_sessions_csv / "derivatives" / "freesurfer" / "sub-0001"


def test_file_discovery_missing_csv_raises(tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        sessions_file=tmp_path / "nonexistent.csv",
    )
    with pytest.raises(FileNotFoundError):
        discover_sessions(cfg)


def test_file_discovery_empty_csv_returns_empty_dataframe(tmp_path):
    csv = tmp_path / "sessions.csv"
    pd.DataFrame(columns=["subject_code", "session_id", "dicom_path"]).to_csv(csv, index=False)
    (tmp_path / "dicom").mkdir()
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        sessions_file=csv,
    )
    df = discover_sessions(cfg)
    assert df.empty
    assert "subject" in df.columns
    assert "session" in df.columns


# ---------------------------------------------------------------------------
# CSV validation — load_sessions() (raw linked_sessions CSV)
# ---------------------------------------------------------------------------


def test_load_sessions_missing_column_raises_value_error(tmp_path):
    """load_sessions raises ValueError when a required column is missing."""
    csv = tmp_path / "bad.csv"
    pd.DataFrame([{"SubjectCode": "sub0001", "ScanID": "SCAN001"}]).to_csv(csv, index=False)
    # 'dicom_path' column is missing
    with pytest.raises(ValueError, match="dicom_path"):
        load_sessions(csv)


def test_load_sessions_error_lists_missing_columns(tmp_path):
    """ValueError message names each missing column."""
    csv = tmp_path / "bad.csv"
    pd.DataFrame([{"irrelevant": "x"}]).to_csv(csv, index=False)
    with pytest.raises(ValueError, match="SubjectCode"):
        load_sessions(csv)


def test_load_sessions_all_required_columns_present_does_not_raise(tmp_path):
    """A raw CSV with all required columns loads without error."""
    csv = tmp_path / "ok.csv"
    # Use a non-numeric SubjectCode to prevent pandas from casting to int on read
    pd.DataFrame([
        {"SubjectCode": "sub0001", "ScanID": "SCAN001", "dicom_path": "/data/SCAN001"},
    ]).to_csv(csv, index=False)
    df = load_sessions(csv)
    assert len(df) == 1


# ---------------------------------------------------------------------------
# CSV validation — _discover_from_file (pre-sanitized sessions file)
# ---------------------------------------------------------------------------


def test_sessions_file_missing_column_raises_value_error(tmp_path):
    """discover_sessions raises ValueError when the sessions file is missing a column."""
    csv = tmp_path / "sessions.csv"
    pd.DataFrame([{"subject_code": "0001", "dicom_path": "/data/SCAN001"}]).to_csv(csv, index=False)
    # 'session_id' column is missing
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        sessions_file=csv,
    )
    with pytest.raises(ValueError, match="session_id"):
        discover_sessions(cfg)

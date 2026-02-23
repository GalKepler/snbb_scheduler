import pandas as pd
import pytest

from snbb_scheduler.config import SchedulerConfig


# ---------------------------------------------------------------------------
# Generic config fixture (used in test_rules, test_manifest, and others)
# ---------------------------------------------------------------------------

@pytest.fixture
def cfg(tmp_path):
    """Minimal SchedulerConfig pointing at a temporary directory tree."""
    return SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )


# ---------------------------------------------------------------------------
# Filesystem-backed fake data
# ---------------------------------------------------------------------------

@pytest.fixture
def fake_data_dir(tmp_path):
    """Create a minimal fake SNBB directory tree.

    Layout:
      sub-0001/ses-01  — DICOM exists, BIDS complete (anat .nii.gz present)
      sub-0002/ses-01  — DICOM exists, no BIDS output
    """
    # DICOM for sub-0001/ses-01
    dicom1 = tmp_path / "dicom" / "sub-0001" / "ses-01"
    dicom1.mkdir(parents=True)
    (dicom1 / "file.dcm").touch()

    # BIDS complete for sub-0001/ses-01
    bids1 = tmp_path / "bids" / "sub-0001" / "ses-01" / "anat"
    bids1.mkdir(parents=True)
    (bids1 / "sub-0001_ses-01_T1w.nii.gz").touch()

    # DICOM for sub-0002/ses-01 — no BIDS output
    dicom2 = tmp_path / "dicom" / "sub-0002" / "ses-01"
    dicom2.mkdir(parents=True)
    (dicom2 / "file.dcm").touch()

    return tmp_path


@pytest.fixture
def fake_config(fake_data_dir):
    """SchedulerConfig pointing at fake_data_dir."""
    return SchedulerConfig(
        dicom_root=fake_data_dir / "dicom",
        bids_root=fake_data_dir / "bids",
        derivatives_root=fake_data_dir / "derivatives",
        state_file=fake_data_dir / "state.parquet",
    )


@pytest.fixture
def fake_sessions_csv(tmp_path):
    """Minimal CSV with two sessions and matching flat DICOM directories."""
    csv = tmp_path / "sessions.csv"
    pd.DataFrame([
        {"subject_code": "0001", "session_id": "01", "ScanID": "SCAN001"},
        {"subject_code": "0002", "session_id": "01", "ScanID": "SCAN002"},
    ]).to_csv(csv, index=False)
    (tmp_path / "dicom" / "SCAN001").mkdir(parents=True)
    (tmp_path / "dicom" / "SCAN002").mkdir(parents=True)
    return tmp_path


@pytest.fixture
def fake_sessions_config(fake_sessions_csv):
    """SchedulerConfig using CSV-based session discovery."""
    return SchedulerConfig(
        dicom_root=fake_sessions_csv / "dicom",
        bids_root=fake_sessions_csv / "bids",
        derivatives_root=fake_sessions_csv / "derivatives",
        state_file=fake_sessions_csv / "state.parquet",
        sessions_file=fake_sessions_csv / "sessions.csv",
    )

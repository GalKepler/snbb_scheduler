import pandas as pd
import pytest

from snbb_scheduler.config import SchedulerConfig


# ---------------------------------------------------------------------------
# Shared BIDS file creation helper
# ---------------------------------------------------------------------------

def _create_bids_session_files(bids_session_dir) -> None:
    """Create all 8 required BIDS modality files inside *bids_session_dir*.

    The filenames are chosen to match the 8 glob patterns in the BIDS
    Procedure's completion_marker list.
    """
    files = {
        "anat": ["sub_T1w.nii.gz"],
        "dwi": ["sub_dir-AP_dwi.nii.gz", "sub_dir-AP_dwi.bvec", "sub_dir-AP_dwi.bval"],
        "fmap": [
            "sub_acq-dwi_dir-AP_epi.nii.gz",
            "sub_acq-func_dir-AP_epi.nii.gz",
            "sub_acq-func_dir-PA_epi.nii.gz",
        ],
        "func": ["sub_task-rest_bold.nii.gz"],
    }
    for subdir, names in files.items():
        d = bids_session_dir / subdir
        d.mkdir(parents=True, exist_ok=True)
        for name in names:
            (d / name).touch()


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

    # BIDS complete for sub-0001/ses-01 — all 8 required modality files
    bids1_root = tmp_path / "bids" / "sub-0001" / "ses-01"
    _create_bids_session_files(bids1_root)

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
    """Minimal CSV with two sessions and matching flat DICOM directories.

    The CSV uses the pre-sanitized format expected by _discover_from_file:
    columns subject_code, session_id, and dicom_path (path to DICOM dir).
    """
    dicom1 = tmp_path / "dicom" / "SCAN001"
    dicom2 = tmp_path / "dicom" / "SCAN002"
    dicom1.mkdir(parents=True)
    dicom2.mkdir(parents=True)
    csv = tmp_path / "sessions.csv"
    pd.DataFrame([
        {"subject_code": "0001", "session_id": "01", "dicom_path": str(dicom1)},
        {"subject_code": "0002", "session_id": "01", "dicom_path": str(dicom2)},
    ]).to_csv(csv, index=False)
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

import pytest

from snbb_scheduler.config import SchedulerConfig


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

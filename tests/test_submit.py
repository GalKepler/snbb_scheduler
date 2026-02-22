"""Tests for submit.py — subprocess.run is mocked throughout."""
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.submit import submit_manifest, submit_task


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cfg(tmp_path):
    return SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_partition="normal",
        slurm_account="snbb",
    )


def make_row(subject="sub-0001", session="ses-01", procedure="bids"):
    return pd.Series({
        "subject": subject,
        "session": session,
        "procedure": procedure,
        "dicom_path": Path(f"/fake/{subject}/{session}"),
        "priority": 0,
    })


def make_manifest(*rows):
    return pd.DataFrame([
        {"subject": r[0], "session": r[1], "procedure": r[2],
         "dicom_path": Path(f"/fake/{r[0]}/{r[1]}"), "priority": i}
        for i, r in enumerate(rows)
    ])


def mock_sbatch(job_id="12345"):
    m = MagicMock()
    m.stdout = f"Submitted batch job {job_id}\n"
    return m


# ---------------------------------------------------------------------------
# submit_task — command construction
# ---------------------------------------------------------------------------

def test_submit_task_calls_sbatch(cfg):
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg)
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "sbatch"


def test_submit_task_partition_flag(cfg):
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg)
    cmd = mock_run.call_args[0][0]
    assert "--partition=normal" in cmd


def test_submit_task_account_flag(cfg):
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg)
    cmd = mock_run.call_args[0][0]
    assert "--account=snbb" in cmd


def test_submit_task_job_name(cfg):
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(procedure="qsiprep"), cfg)
    cmd = mock_run.call_args[0][0]
    assert "--job-name=qsiprep_sub-0001_ses-01" in cmd


def test_submit_task_uses_procedure_script(cfg):
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(procedure="bids"), cfg)
    cmd = mock_run.call_args[0][0]
    assert "snbb_run_bids.sh" in cmd


def test_submit_task_passes_subject_and_session(cfg):
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(subject="sub-0042", session="ses-02"), cfg)
    cmd = mock_run.call_args[0][0]
    assert "sub-0042" in cmd
    assert "ses-02" in cmd


def test_submit_task_script_from_procedure_registry(cfg):
    """Each procedure uses its own script, not a hardcoded map."""
    for proc_name, expected_script in [
        ("bids", "snbb_run_bids.sh"),
        ("qsiprep", "snbb_run_qsiprep.sh"),
        ("freesurfer", "snbb_run_freesurfer.sh"),
    ]:
        with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
            submit_task(make_row(procedure=proc_name), cfg)
        cmd = mock_run.call_args[0][0]
        assert expected_script in cmd, f"{proc_name}: expected {expected_script} in cmd"


def test_submit_task_subprocess_flags(cfg):
    """subprocess.run called with capture_output=True, text=True, check=True."""
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg)
    _, kwargs = mock_run.call_args
    assert kwargs.get("capture_output") is True
    assert kwargs.get("text") is True
    assert kwargs.get("check") is True


# ---------------------------------------------------------------------------
# submit_task — return value
# ---------------------------------------------------------------------------

def test_submit_task_returns_job_id(cfg):
    with patch("subprocess.run", return_value=mock_sbatch("99999")):
        job_id = submit_task(make_row(), cfg)
    assert job_id == "99999"


def test_submit_task_parses_job_id_from_stdout(cfg):
    with patch("subprocess.run", return_value=mock_sbatch("54321")):
        job_id = submit_task(make_row(), cfg)
    assert job_id == "54321"


# ---------------------------------------------------------------------------
# submit_task — dry run
# ---------------------------------------------------------------------------

def test_dry_run_does_not_call_subprocess(cfg, capsys):
    with patch("subprocess.run") as mock_run:
        submit_task(make_row(), cfg, dry_run=True)
    mock_run.assert_not_called()


def test_dry_run_returns_none(cfg):
    with patch("subprocess.run"):
        result = submit_task(make_row(), cfg, dry_run=True)
    assert result is None


def test_dry_run_prints_command(cfg, capsys):
    submit_task(make_row(procedure="bids"), cfg, dry_run=True)
    out = capsys.readouterr().out
    assert "[DRY RUN]" in out
    assert "sbatch" in out
    assert "snbb_run_bids.sh" in out


# ---------------------------------------------------------------------------
# submit_manifest
# ---------------------------------------------------------------------------

def test_submit_manifest_submits_each_row(cfg):
    manifest = make_manifest(
        ("sub-0001", "ses-01", "bids"),
        ("sub-0002", "ses-01", "bids"),
    )
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_manifest(manifest, cfg)
    assert mock_run.call_count == 2


def test_submit_manifest_returns_state_dataframe(cfg):
    manifest = make_manifest(("sub-0001", "ses-01", "bids"))
    with patch("subprocess.run", return_value=mock_sbatch("111")):
        result = submit_manifest(manifest, cfg)
    assert isinstance(result, pd.DataFrame)
    for col in ("subject", "session", "procedure", "status", "submitted_at", "job_id"):
        assert col in result.columns


def test_submit_manifest_state_rows_are_pending(cfg):
    manifest = make_manifest(
        ("sub-0001", "ses-01", "bids"),
        ("sub-0001", "ses-01", "qsiprep"),
    )
    with patch("subprocess.run", return_value=mock_sbatch()):
        result = submit_manifest(manifest, cfg)
    assert (result["status"] == "pending").all()


def test_submit_manifest_job_ids_recorded(cfg):
    manifest = make_manifest(("sub-0001", "ses-01", "bids"))
    with patch("subprocess.run", return_value=mock_sbatch("777")):
        result = submit_manifest(manifest, cfg)
    assert result.iloc[0]["job_id"] == "777"


def test_submit_manifest_empty_manifest(cfg):
    manifest = pd.DataFrame(
        columns=["subject", "session", "procedure", "dicom_path", "priority"]
    )
    with patch("subprocess.run") as mock_run:
        result = submit_manifest(manifest, cfg)
    mock_run.assert_not_called()
    assert result.empty


def test_submit_manifest_dry_run_no_subprocess(cfg, capsys):
    manifest = make_manifest(
        ("sub-0001", "ses-01", "bids"),
        ("sub-0002", "ses-01", "bids"),
    )
    with patch("subprocess.run") as mock_run:
        result = submit_manifest(manifest, cfg, dry_run=True)
    mock_run.assert_not_called()
    out = capsys.readouterr().out
    assert out.count("[DRY RUN]") == 2


def test_submit_manifest_dry_run_job_ids_are_none(cfg):
    manifest = make_manifest(("sub-0001", "ses-01", "bids"))
    result = submit_manifest(manifest, cfg, dry_run=True)
    assert result.iloc[0]["job_id"] is None


def test_submit_manifest_one_row_per_task(cfg):
    manifest = make_manifest(
        ("sub-0001", "ses-01", "bids"),
        ("sub-0001", "ses-01", "qsiprep"),
        ("sub-0002", "ses-01", "bids"),
    )
    with patch("subprocess.run", return_value=mock_sbatch()):
        result = submit_manifest(manifest, cfg)
    assert len(result) == 3

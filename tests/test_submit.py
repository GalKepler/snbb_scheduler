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
        slurm_partition="debug",
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
    assert "--partition=debug" in cmd


def test_submit_task_account_flag(cfg):
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg)
    cmd = mock_run.call_args[0][0]
    assert "--account=snbb" in cmd


def test_submit_task_job_name_session_scoped(cfg):
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(procedure="bids"), cfg)
    cmd = mock_run.call_args[0][0]
    assert "--job-name=bids_sub-0001_ses-01" in cmd


def test_submit_task_job_name_subject_scoped(cfg):
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(procedure="freesurfer", session=""), cfg)
    cmd = mock_run.call_args[0][0]
    assert "--job-name=freesurfer_sub-0001" in cmd
    # session must NOT be passed as a script argument
    assert cmd[-1] == "snbb_run_freesurfer.sh" or cmd[-1] == "sub-0001"
    assert "ses-" not in cmd[-1]


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


def test_submit_task_passes_dicom_path_for_session_scoped(cfg):
    """dicom_path is appended as a 3rd script arg for session-scoped procedures."""
    dicom = Path("/data/dicom/session_dir")
    row = pd.Series({
        "subject": "sub-0001", "session": "ses-01",
        "procedure": "bids", "dicom_path": dicom, "priority": 0,
    })
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(row, cfg)
    cmd = mock_run.call_args[0][0]
    assert str(dicom) in cmd


def test_submit_task_no_dicom_path_when_none(cfg):
    """When dicom_path is None, no extra arg is appended for session-scoped procedures."""
    row = pd.Series({
        "subject": "sub-0001", "session": "ses-01",
        "procedure": "bids", "dicom_path": None, "priority": 0,
    })
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(row, cfg)
    cmd = mock_run.call_args[0][0]
    assert cmd[-1] == "ses-01"


def test_submit_task_no_dicom_path_for_subject_scoped(cfg):
    """dicom_path is never appended for subject-scoped procedures."""
    dicom = Path("/data/dicom/session_dir")
    row = pd.Series({
        "subject": "sub-0001", "session": "",
        "procedure": "freesurfer", "dicom_path": dicom, "priority": 0,
    })
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(row, cfg)
    cmd = mock_run.call_args[0][0]
    assert str(dicom) not in cmd


def test_submit_task_script_from_procedure_registry(cfg):
    """Each procedure uses its own script, not a hardcoded map."""
    for proc_name, expected_script in [
        ("bids", "snbb_run_bids.sh"),
        ("bids_post", "snbb_run_bids_post.sh"),
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


def test_submit_task_raises_on_unexpected_sbatch_output(cfg):
    """sbatch stdout that doesn't start with 'Submitted batch job' raises RuntimeError."""
    bad_mock = MagicMock()
    bad_mock.stdout = "Error: some sbatch problem\n"
    with patch("subprocess.run", return_value=bad_mock):
        with pytest.raises(RuntimeError, match="Unexpected sbatch output"):
            submit_task(make_row(), cfg)


def test_submit_task_mem_flag(tmp_path):
    """--mem flag included when slurm_mem is set."""
    cfg_mem = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_mem="32G",
    )
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg_mem)
    cmd = mock_run.call_args[0][0]
    assert "--mem=32G" in cmd


def test_submit_task_cpus_flag(tmp_path):
    """--cpus-per-task flag included when slurm_cpus_per_task is set."""
    cfg_cpus = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_cpus_per_task=8,
    )
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg_cpus)
    cmd = mock_run.call_args[0][0]
    assert "--cpus-per-task=8" in cmd


def test_submit_task_no_mem_when_none(cfg):
    """--mem flag absent when slurm_mem is None."""
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg)
    cmd = mock_run.call_args[0][0]
    assert not any(arg.startswith("--mem") for arg in cmd)


def test_submit_task_no_cpus_when_none(cfg):
    """--cpus-per-task flag absent when slurm_cpus_per_task is None."""
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg)
    cmd = mock_run.call_args[0][0]
    assert not any(arg.startswith("--cpus-per-task") for arg in cmd)


def test_submit_task_no_partition_when_empty(tmp_path):
    """When slurm_partition is empty, --partition flag is omitted from the command."""
    cfg_no_partition = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_partition="",
        slurm_account="snbb",
    )
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg_no_partition)
    cmd = mock_run.call_args[0][0]
    assert not any(arg.startswith("--partition") for arg in cmd)


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


# ---------------------------------------------------------------------------
# submit_task — slurm_log_dir / --output / --error flags
# ---------------------------------------------------------------------------

def test_submit_task_output_flag_when_log_dir_set(tmp_path):
    """--output flag added when slurm_log_dir is configured."""
    log_dir = tmp_path / "slurm_logs"
    cfg_log = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_log_dir=log_dir,
    )
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(procedure="bids"), cfg_log)
    cmd = mock_run.call_args[0][0]
    assert any(arg.startswith("--output=") for arg in cmd)


def test_submit_task_error_flag_when_log_dir_set(tmp_path):
    """--error flag added when slurm_log_dir is configured."""
    log_dir = tmp_path / "slurm_logs"
    cfg_log = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_log_dir=log_dir,
    )
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(procedure="bids"), cfg_log)
    cmd = mock_run.call_args[0][0]
    assert any(arg.startswith("--error=") for arg in cmd)


def test_submit_task_no_log_flags_when_log_dir_none(cfg):
    """--output and --error absent when slurm_log_dir is None."""
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(), cfg)
    cmd = mock_run.call_args[0][0]
    assert not any(arg.startswith("--output=") for arg in cmd)
    assert not any(arg.startswith("--error=") for arg in cmd)


def test_submit_task_log_dir_contains_procedure_subdir(tmp_path):
    """Log paths include procedure-specific subdirectory."""
    log_dir = tmp_path / "slurm_logs"
    cfg_log = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_log_dir=log_dir,
    )
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(procedure="bids"), cfg_log)
    cmd = mock_run.call_args[0][0]
    output_flag = next(a for a in cmd if a.startswith("--output="))
    assert "/bids/" in output_flag


def test_submit_task_log_dir_subdir_created(tmp_path):
    """Log subdirectory is created on disk before sbatch is called."""
    log_dir = tmp_path / "slurm_logs"
    cfg_log = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_log_dir=log_dir,
    )
    with patch("subprocess.run", return_value=mock_sbatch()):
        submit_task(make_row(procedure="bids"), cfg_log)
    assert (log_dir / "bids").is_dir()


# ---------------------------------------------------------------------------
# submit_task — audit logging
# ---------------------------------------------------------------------------

def test_submit_task_audit_submitted(cfg):
    from unittest.mock import MagicMock
    audit = MagicMock()
    with patch("subprocess.run", return_value=mock_sbatch("555")):
        submit_task(make_row(subject="sub-0001", session="ses-01", procedure="bids"), cfg, audit=audit)
    audit.log.assert_called_once_with(
        "submitted",
        subject="sub-0001",
        session="ses-01",
        procedure="bids",
        job_id="555",
    )


def test_submit_task_audit_dry_run(cfg):
    from unittest.mock import MagicMock
    audit = MagicMock()
    submit_task(make_row(subject="sub-0001", session="ses-01", procedure="bids"), cfg, dry_run=True, audit=audit)
    audit.log.assert_called_once()
    args, kwargs = audit.log.call_args
    assert args[0] == "dry_run"
    assert kwargs["subject"] == "sub-0001"
    assert "detail" in kwargs


def test_submit_task_no_audit_no_error(cfg):
    """audit=None still works — no AttributeError."""
    with patch("subprocess.run", return_value=mock_sbatch("1")):
        submit_task(make_row(), cfg, audit=None)


def test_submit_manifest_audit_passed_through(cfg):
    """submit_manifest passes audit to each submit_task call."""
    from unittest.mock import MagicMock
    audit = MagicMock()
    manifest = make_manifest(
        ("sub-0001", "ses-01", "bids"),
        ("sub-0002", "ses-01", "bids"),
    )
    with patch("subprocess.run", return_value=mock_sbatch("10")):
        submit_manifest(manifest, cfg, audit=audit)
    assert audit.log.call_count == 2


def test_submit_task_audit_error_on_called_process_error(cfg):
    """audit.log('error', ...) called when sbatch raises CalledProcessError."""
    import subprocess
    from unittest.mock import MagicMock
    audit = MagicMock()
    exc = subprocess.CalledProcessError(1, "sbatch")
    with patch("subprocess.run", side_effect=exc):
        with pytest.raises(subprocess.CalledProcessError):
            submit_task(make_row(), cfg, audit=audit)
    audit.log.assert_called_once()
    args, kwargs = audit.log.call_args
    assert args[0] == "error"


def test_submit_task_no_audit_on_called_process_error_no_crash(cfg):
    """audit=None doesn't crash when CalledProcessError is raised."""
    import subprocess
    exc = subprocess.CalledProcessError(1, "sbatch")
    with patch("subprocess.run", side_effect=exc):
        with pytest.raises(subprocess.CalledProcessError):
            submit_task(make_row(), cfg, audit=None)


def test_submit_task_log_filenames_contain_job_name(tmp_path):
    """Log file names embed the Slurm job name."""
    log_dir = tmp_path / "slurm_logs"
    cfg_log = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_log_dir=log_dir,
    )
    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        submit_task(make_row(subject="sub-0001", session="ses-01", procedure="bids"), cfg_log)
    cmd = mock_run.call_args[0][0]
    output_flag = next(a for a in cmd if a.startswith("--output="))
    assert "bids_sub-0001_ses-01" in output_flag

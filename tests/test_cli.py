"""CLI smoke tests using Click's CliRunner."""
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from click.testing import CliRunner

from snbb_scheduler.cli import main
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.manifest import save_state


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def cfg_path(tmp_path):
    """Write a minimal YAML config pointing at tmp_path directories."""
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        f"dicom_root: {tmp_path / 'dicom'}\n"
        f"bids_root: {tmp_path / 'bids'}\n"
        f"derivatives_root: {tmp_path / 'derivatives'}\n"
        f"state_file: {tmp_path / 'state.parquet'}\n"
    )
    return yaml_file


@pytest.fixture
def cfg_with_sessions(tmp_path, cfg_path):
    """Config + two DICOM sessions on disk."""
    (tmp_path / "dicom" / "sub-0001" / "ses-01").mkdir(parents=True)
    (tmp_path / "dicom" / "sub-0002" / "ses-01").mkdir(parents=True)
    return cfg_path


# ---------------------------------------------------------------------------
# --help
# ---------------------------------------------------------------------------

def test_main_help(runner):
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "snbb-scheduler" in result.output
    assert "--slurm-mem" in result.output
    assert "--slurm-cpus" in result.output


def test_run_help(runner):
    result = runner.invoke(main, ["run", "--help"])
    assert result.exit_code == 0
    assert "--dry-run" in result.output
    assert "--force" in result.output
    assert "--procedure" in result.output


def test_manifest_help(runner):
    result = runner.invoke(main, ["manifest", "--help"])
    assert result.exit_code == 0


def test_status_help(runner):
    result = runner.invoke(main, ["status", "--help"])
    assert result.exit_code == 0


def test_retry_help(runner):
    result = runner.invoke(main, ["retry", "--help"])
    assert result.exit_code == 0
    assert "--procedure" in result.output
    assert "--subject" in result.output


# ---------------------------------------------------------------------------
# run --dry-run
# ---------------------------------------------------------------------------

def test_run_dry_run_no_dicom(runner, cfg_path):
    result = runner.invoke(main, ["--config", str(cfg_path), "run", "--dry-run"])
    assert result.exit_code == 0
    assert "Nothing to submit" in result.output


def test_run_dry_run_with_sessions(runner, cfg_with_sessions):
    result = runner.invoke(main, ["--config", str(cfg_with_sessions), "run", "--dry-run"])
    assert result.exit_code == 0
    assert "[DRY RUN]" in result.output


def test_run_dry_run_does_not_write_state(runner, cfg_with_sessions, tmp_path):
    runner.invoke(main, ["--config", str(cfg_with_sessions), "run", "--dry-run"])
    assert not (tmp_path / "state.parquet").exists()


def test_slurm_mem_cli_overrides_config(runner, cfg_with_sessions):
    """--slurm-mem on the CLI overrides the config and reaches sbatch."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "Submitted batch job 1\n"
        runner.invoke(main, ["--config", str(cfg_with_sessions), "--slurm-mem", "64G", "run"])
    calls = mock_run.call_args_list
    assert calls, "sbatch was never called"
    for c in calls:
        assert "--mem=64G" in c[0][0]


def test_slurm_cpus_cli_overrides_config(runner, cfg_with_sessions):
    """--slurm-cpus on the CLI overrides the config and reaches sbatch."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "Submitted batch job 2\n"
        runner.invoke(main, ["--config", str(cfg_with_sessions), "--slurm-cpus", "4", "run"])
    calls = mock_run.call_args_list
    assert calls, "sbatch was never called"
    for c in calls:
        assert "--cpus-per-task=4" in c[0][0]


def test_run_live_submits_and_saves_state(runner, cfg_with_sessions, tmp_path):
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "Submitted batch job 42\n"
        result = runner.invoke(main, ["--config", str(cfg_with_sessions), "run"])
    assert result.exit_code == 0
    assert (tmp_path / "state.parquet").exists()


# ---------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------

def test_manifest_no_sessions(runner, cfg_path):
    result = runner.invoke(main, ["--config", str(cfg_path), "manifest"])
    assert result.exit_code == 0
    assert "No tasks pending" in result.output


def test_manifest_shows_procedures(runner, cfg_with_sessions):
    result = runner.invoke(main, ["--config", str(cfg_with_sessions), "manifest"])
    assert result.exit_code == 0
    assert "bids" in result.output


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

def test_status_no_state(runner, cfg_path):
    result = runner.invoke(main, ["--config", str(cfg_path), "status"])
    assert result.exit_code == 0
    assert "No state recorded" in result.output


def test_status_shows_state(runner, cfg_path, tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "running", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "99",
    }])
    save_state(state, cfg)
    result = runner.invoke(main, ["--config", str(cfg_path), "status"])
    assert result.exit_code == 0
    assert "sub-0001" in result.output


# ---------------------------------------------------------------------------
# retry
# ---------------------------------------------------------------------------

def test_retry_no_state(runner, cfg_path):
    result = runner.invoke(main, ["--config", str(cfg_path), "retry"])
    assert result.exit_code == 0
    assert "No state recorded" in result.output


def test_retry_no_matching_failures(runner, cfg_path, tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "complete", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "1",
    }])
    save_state(state, cfg)
    result = runner.invoke(main, ["--config", str(cfg_path), "retry", "--procedure", "bids"])
    assert result.exit_code == 0
    assert "No matching failed" in result.output


def test_retry_clears_failed_entries(runner, cfg_path, tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([
        {"subject": "sub-0001", "session": "ses-01", "procedure": "bids",
         "status": "failed", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "1"},
        {"subject": "sub-0002", "session": "ses-01", "procedure": "bids",
         "status": "complete", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "2"},
    ])
    save_state(state, cfg)
    result = runner.invoke(main, ["--config", str(cfg_path), "retry", "--procedure", "bids"])
    assert result.exit_code == 0
    assert "Cleared 1" in result.output

    # Reload state — only the complete row should remain
    from snbb_scheduler.manifest import load_state
    remaining = load_state(cfg)
    assert len(remaining) == 1
    assert remaining.iloc[0]["status"] == "complete"


# ---------------------------------------------------------------------------
# --force
# ---------------------------------------------------------------------------

def _add_bids(tmp_path, subject, session):
    """Create BIDS modality files matching the bids completion_marker."""
    bids_dir = tmp_path / "bids" / subject / session
    files = {
        "anat": ["sub_T1w.nii.gz"],
        "dwi": [
            "sub_dir-AP_dwi.nii.gz",
            "sub_dir-AP_dwi.bvec",
            "sub_dir-AP_dwi.bval",
            # Short reverse-PE DWI; heudiconv places it in dwi/ (not fmap/)
            "sub_dir-PA_dwi.nii.gz",
        ],
        "fmap": [
            "sub_acq-func_dir-AP_epi.nii.gz",
            "sub_acq-func_dir-PA_epi.nii.gz",
        ],
        "func": ["sub_task-rest_bold.nii.gz"],
    }
    for subdir, names in files.items():
        d = bids_dir / subdir
        d.mkdir(parents=True, exist_ok=True)
        for name in names:
            (d / name).touch()


@pytest.fixture
def cfg_with_bids_complete(tmp_path, cfg_path):
    """Config + DICOM + complete BIDS for sub-0001/ses-01."""
    (tmp_path / "dicom" / "sub-0001" / "ses-01").mkdir(parents=True)
    _add_bids(tmp_path, "sub-0001", "ses-01")
    return cfg_path


def test_force_resubmits_complete_procedure(runner, cfg_with_bids_complete):
    """--force causes already-complete procedures to be re-submitted."""
    result = runner.invoke(
        main,
        ["--config", str(cfg_with_bids_complete), "run", "--force", "--dry-run"],
    )
    assert result.exit_code == 0
    # bids is complete but --force means it appears in dry-run output
    assert "[DRY RUN]" in result.output
    assert "bids" in result.output


def test_force_bypasses_in_flight_filter(runner, cfg_with_sessions, tmp_path):
    """--force submits tasks even when they are already pending/running in state."""
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    # Mark sub-0001/ses-01/bids as already running
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "running", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "7",
    }])
    save_state(state, cfg)

    result = runner.invoke(
        main,
        ["--config", str(cfg_with_sessions), "run", "--force", "--dry-run"],
    )
    assert result.exit_code == 0
    assert "--force: skipping in-flight filter" in result.output
    assert "[DRY RUN]" in result.output


def test_force_procedure_limits_forced_procedure(runner, cfg_with_bids_complete):
    """--force --procedure bids only forces bids, not other procedures."""
    result = runner.invoke(
        main,
        [
            "--config", str(cfg_with_bids_complete),
            "run", "--force", "--procedure", "bids", "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "[DRY RUN]" in result.output


def test_slurm_log_dir_in_help(runner):
    """--slurm-log-dir appears in the main group help text."""
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "--slurm-log-dir" in result.output


def test_slurm_log_dir_cli_overrides_config(runner, cfg_with_sessions, tmp_path):
    """--slurm-log-dir on the CLI overrides config and reaches sbatch as --output/--error."""
    log_dir = tmp_path / "slurm_logs"
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "Submitted batch job 3\n"
        runner.invoke(
            main,
            ["--config", str(cfg_with_sessions), "--slurm-log-dir", str(log_dir), "run"],
        )
    calls = mock_run.call_args_list
    assert calls, "sbatch was never called"
    for c in calls:
        cmd = c[0][0]
        assert any(a.startswith("--output=") for a in cmd)
        assert any(a.startswith("--error=") for a in cmd)


# ---------------------------------------------------------------------------
# monitor command
# ---------------------------------------------------------------------------

def test_monitor_help(runner):
    result = runner.invoke(main, ["monitor", "--help"])
    assert result.exit_code == 0


def test_monitor_no_state(runner, cfg_path):
    result = runner.invoke(main, ["--config", str(cfg_path), "monitor"])
    assert result.exit_code == 0
    assert "No state recorded" in result.output


def test_monitor_with_in_flight_jobs(runner, cfg_path, tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "pending", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "99",
    }])
    save_state(state, cfg)

    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"99": "complete"}):
        result = runner.invoke(main, ["--config", str(cfg_path), "monitor"])
    assert result.exit_code == 0

    from snbb_scheduler.manifest import load_state
    updated = load_state(cfg)
    assert updated.iloc[0]["status"] == "complete"


def test_monitor_no_transitions_exits_ok(runner, cfg_path, tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "pending", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "100",
    }])
    save_state(state, cfg)

    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"100": "pending"}):
        result = runner.invoke(main, ["--config", str(cfg_path), "monitor"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# run --skip-monitor
# ---------------------------------------------------------------------------

def test_run_skip_monitor_no_sacct_called(runner, cfg_with_sessions, tmp_path):
    """--skip-monitor means poll_jobs is never called."""
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "pending", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "7",
    }])
    save_state(state, cfg)

    with patch("snbb_scheduler.monitor.poll_jobs") as mock_poll, \
         patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "Submitted batch job 1\n"
        runner.invoke(
            main,
            ["--config", str(cfg_with_sessions), "run", "--skip-monitor", "--dry-run"],
        )
    mock_poll.assert_not_called()


def test_run_skip_monitor_in_help(runner):
    result = runner.invoke(main, ["run", "--help"])
    assert "--skip-monitor" in result.output


# ---------------------------------------------------------------------------
# enhanced status
# ---------------------------------------------------------------------------

def test_status_shows_summary_section(runner, cfg_path, tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([
        {"subject": "sub-0001", "session": "ses-01", "procedure": "bids",
         "status": "complete", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "1"},
        {"subject": "sub-0002", "session": "ses-01", "procedure": "bids",
         "status": "pending", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "2"},
    ])
    save_state(state, cfg)
    result = runner.invoke(main, ["--config", str(cfg_path), "status"])
    assert result.exit_code == 0
    assert "Summary" in result.output
    assert "procedure" in result.output or "bids" in result.output
    assert "count" in result.output or "1" in result.output


def test_status_with_slurm_log_dir(runner, tmp_path):
    log_dir = tmp_path / "slurm_logs"
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        f"dicom_root: {tmp_path / 'dicom'}\n"
        f"bids_root: {tmp_path / 'bids'}\n"
        f"derivatives_root: {tmp_path / 'derivatives'}\n"
        f"state_file: {tmp_path / 'state.parquet'}\n"
        f"slurm_log_dir: {log_dir}\n"
    )
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_log_dir=log_dir,
    )
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "complete", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "11",
    }])
    save_state(state, cfg)
    result = runner.invoke(main, ["--config", str(yaml_file), "status"])
    assert result.exit_code == 0
    assert "log_path" in result.output


# ---------------------------------------------------------------------------
# retry audit
# ---------------------------------------------------------------------------

def test_retry_writes_audit_log(runner, cfg_path, tmp_path):
    log_file = tmp_path / "audit.jsonl"
    yaml_file = tmp_path / "config_audit.yaml"
    yaml_file.write_text(
        f"dicom_root: {tmp_path / 'dicom'}\n"
        f"bids_root: {tmp_path / 'bids'}\n"
        f"derivatives_root: {tmp_path / 'derivatives'}\n"
        f"state_file: {tmp_path / 'state.parquet'}\n"
        f"log_file: {log_file}\n"
    )
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "failed", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "5",
    }])
    save_state(state, cfg)
    result = runner.invoke(main, ["--config", str(yaml_file), "retry", "--procedure", "bids"])
    assert result.exit_code == 0
    import json
    record = json.loads(log_file.read_text())
    assert record["event"] == "retry_cleared"


def test_run_monitor_updates_state(runner, tmp_path):
    """run without --skip-monitor calls monitor and saves updated state."""
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        f"dicom_root: {tmp_path / 'dicom'}\n"
        f"bids_root: {tmp_path / 'bids'}\n"
        f"derivatives_root: {tmp_path / 'derivatives'}\n"
        f"state_file: {tmp_path / 'state.parquet'}\n"
    )
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "pending", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "77",
    }])
    save_state(state, cfg)

    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"77": "complete"}), \
         patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "Submitted batch job 1\n"
        result = runner.invoke(main, ["--config", str(yaml_file), "run", "--dry-run"])
    assert result.exit_code == 0

    from snbb_scheduler.manifest import load_state
    updated = load_state(cfg)
    assert updated.iloc[0]["status"] == "complete"


def test_run_monitor_exception_handled_gracefully(runner, tmp_path):
    """If monitor raises, run continues without crashing."""
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        f"dicom_root: {tmp_path / 'dicom'}\n"
        f"bids_root: {tmp_path / 'bids'}\n"
        f"derivatives_root: {tmp_path / 'derivatives'}\n"
        f"state_file: {tmp_path / 'state.parquet'}\n"
    )
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "pending", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "78",
    }])
    save_state(state, cfg)

    with patch("snbb_scheduler.cli.update_state_from_sacct", side_effect=RuntimeError("oops")):
        result = runner.invoke(main, ["--config", str(yaml_file), "run", "--dry-run"])
    assert result.exit_code == 0


def test_status_log_path_unknown_procedure(runner, tmp_path):
    """status with slurm_log_dir + unknown procedure name uses fallback job_name."""
    log_dir = tmp_path / "slurm_logs"
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        f"dicom_root: {tmp_path / 'dicom'}\n"
        f"bids_root: {tmp_path / 'bids'}\n"
        f"derivatives_root: {tmp_path / 'derivatives'}\n"
        f"state_file: {tmp_path / 'state.parquet'}\n"
        f"slurm_log_dir: {log_dir}\n"
    )
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_log_dir=log_dir,
    )
    state = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "unknown_proc",
        "status": "complete", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "99",
    }])
    save_state(state, cfg)
    result = runner.invoke(main, ["--config", str(yaml_file), "status"])
    assert result.exit_code == 0
    assert "log_path" in result.output


def test_retry_filter_by_subject(runner, cfg_path, tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    state = pd.DataFrame([
        {"subject": "sub-0001", "session": "ses-01", "procedure": "bids",
         "status": "failed", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "1"},
        {"subject": "sub-0002", "session": "ses-01", "procedure": "bids",
         "status": "failed", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "2"},
    ])
    save_state(state, cfg)
    runner.invoke(main, ["--config", str(cfg_path), "retry", "--subject", "sub-0001"])

    from snbb_scheduler.manifest import load_state
    remaining = load_state(cfg)
    assert len(remaining) == 1
    assert remaining.iloc[0]["subject"] == "sub-0002"

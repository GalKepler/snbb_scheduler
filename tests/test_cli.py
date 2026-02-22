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


def test_run_help(runner):
    result = runner.invoke(main, ["run", "--help"])
    assert result.exit_code == 0
    assert "--dry-run" in result.output


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

"""Tests for monitor.py — sacct polling and state refresh."""
import subprocess
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from snbb_scheduler.audit import AuditLogger
from snbb_scheduler.monitor import poll_jobs, update_state_from_sacct


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sacct_output(*lines: str) -> MagicMock:
    """Return a mock subprocess.run result with given sacct stdout lines."""
    m = MagicMock()
    m.stdout = "\n".join(lines) + "\n"
    return m


def _state(**kwargs) -> pd.DataFrame:
    """Build a minimal state DataFrame from keyword arg lists."""
    defaults = {
        "subject": ["sub-0001"],
        "session": ["ses-01"],
        "procedure": ["bids"],
        "status": ["pending"],
        "submitted_at": [pd.Timestamp("2024-01-01")],
        "job_id": ["12345"],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


# ---------------------------------------------------------------------------
# poll_jobs — basic parsing
# ---------------------------------------------------------------------------


def test_poll_jobs_empty_list_returns_empty():
    result = poll_jobs([])
    assert result == {}


def test_poll_jobs_completed_maps_to_complete():
    with patch("subprocess.run", return_value=_sacct_output("12345|COMPLETED")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "complete"}


def test_poll_jobs_failed_maps_to_failed():
    with patch("subprocess.run", return_value=_sacct_output("12345|FAILED")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "failed"}


def test_poll_jobs_timeout_maps_to_failed():
    with patch("subprocess.run", return_value=_sacct_output("12345|TIMEOUT")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "failed"}


def test_poll_jobs_cancelled_maps_to_failed():
    with patch("subprocess.run", return_value=_sacct_output("12345|CANCELLED")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "failed"}


def test_poll_jobs_out_of_memory_maps_to_failed():
    with patch("subprocess.run", return_value=_sacct_output("12345|OUT_OF_MEMORY")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "failed"}


def test_poll_jobs_node_fail_maps_to_failed():
    with patch("subprocess.run", return_value=_sacct_output("12345|NODE_FAIL")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "failed"}


def test_poll_jobs_pending_maps_to_pending():
    with patch("subprocess.run", return_value=_sacct_output("12345|PENDING")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "pending"}


def test_poll_jobs_running_maps_to_running():
    with patch("subprocess.run", return_value=_sacct_output("12345|RUNNING")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "running"}


def test_poll_jobs_multiple_jobs():
    with patch(
        "subprocess.run",
        return_value=_sacct_output("11111|COMPLETED", "22222|RUNNING", "33333|FAILED"),
    ):
        result = poll_jobs(["11111", "22222", "33333"])
    assert result == {"11111": "complete", "22222": "running", "33333": "failed"}


def test_poll_jobs_skips_sub_steps():
    """Job IDs containing '.' are sub-steps and must be skipped."""
    with patch(
        "subprocess.run",
        return_value=_sacct_output("12345|COMPLETED", "12345.batch|COMPLETED", "12345.0|COMPLETED"),
    ):
        result = poll_jobs(["12345"])
    assert result == {"12345": "complete"}
    assert "12345.batch" not in result
    assert "12345.0" not in result


def test_poll_jobs_unknown_state_ignored():
    """An unrecognised sacct state is silently dropped."""
    with patch("subprocess.run", return_value=_sacct_output("12345|REQUEUED")):
        result = poll_jobs(["12345"])
    assert "12345" not in result


def test_poll_jobs_empty_lines_ignored():
    """Empty lines in sacct output are skipped without error."""
    with patch("subprocess.run", return_value=_sacct_output("", "12345|COMPLETED", "")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "complete"}


def test_poll_jobs_sacct_failure_returns_empty():
    """If sacct exits non-zero, return an empty dict instead of raising."""
    with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "sacct")):
        result = poll_jobs(["12345"])
    assert result == {}


def test_poll_jobs_sacct_not_found_returns_empty():
    """If sacct is not installed, return empty dict."""
    with patch("subprocess.run", side_effect=FileNotFoundError("sacct not found")):
        result = poll_jobs(["12345"])
    assert result == {}


def test_poll_jobs_malformed_line_without_pipe_ignored():
    """Lines with no '|' separator are silently skipped."""
    with patch("subprocess.run", return_value=_sacct_output("nopipe", "12345|COMPLETED")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "complete"}


def test_poll_jobs_strips_cancelled_qualifier():
    """CANCELLED by user → 'failed', not ignored."""
    with patch(
        "subprocess.run",
        return_value=_sacct_output("12345|CANCELLED by 1001"),
    ):
        result = poll_jobs(["12345"])
    assert result == {"12345": "failed"}


def test_poll_jobs_calls_sacct_with_correct_args():
    with patch("subprocess.run", return_value=_sacct_output("12345|COMPLETED")) as mock_run:
        poll_jobs(["12345", "67890"])
    cmd = mock_run.call_args[0][0]
    assert "sacct" in cmd
    assert "-j" in cmd
    ids_idx = cmd.index("-j") + 1
    assert "12345" in cmd[ids_idx]
    assert "67890" in cmd[ids_idx]
    assert "--noheader" in cmd
    assert "--parsable2" in cmd


# ---------------------------------------------------------------------------
# update_state_from_sacct
# ---------------------------------------------------------------------------


def test_update_returns_unchanged_state_when_no_in_flight():
    state = _state(status=["complete"])
    result = update_state_from_sacct(state)
    assert (result["status"] == "complete").all()


def test_update_empty_state_returns_empty():
    empty = pd.DataFrame(columns=["subject", "session", "procedure", "status", "job_id"])
    result = update_state_from_sacct(empty)
    assert result.empty


def test_update_pending_to_complete():
    state = _state(status=["pending"], job_id=["99"])
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"99": "complete"}):
        result = update_state_from_sacct(state)
    assert result.iloc[0]["status"] == "complete"


def test_update_running_to_failed():
    state = _state(status=["running"], job_id=["55"])
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"55": "failed"}):
        result = update_state_from_sacct(state)
    assert result.iloc[0]["status"] == "failed"


def test_update_does_not_mutate_original():
    state = _state(status=["pending"], job_id=["99"])
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"99": "complete"}):
        result = update_state_from_sacct(state)
    assert state.iloc[0]["status"] == "pending"
    assert result.iloc[0]["status"] == "complete"


def test_update_only_polls_in_flight_jobs():
    """Only pending/running rows contribute job IDs to poll_jobs."""
    state = pd.DataFrame({
        "subject": ["sub-0001", "sub-0002"],
        "session": ["ses-01", "ses-01"],
        "procedure": ["bids", "bids"],
        "status": ["complete", "pending"],
        "submitted_at": [pd.Timestamp("2024-01-01")] * 2,
        "job_id": ["111", "222"],
    })
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"222": "complete"}) as mock_poll:
        update_state_from_sacct(state)
    called_ids = mock_poll.call_args[0][0]
    assert "222" in called_ids
    assert "111" not in called_ids


def test_update_no_change_when_status_unchanged():
    state = _state(status=["pending"], job_id=["99"])
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"99": "pending"}):
        result = update_state_from_sacct(state)
    assert result.iloc[0]["status"] == "pending"


def test_update_no_change_when_poll_returns_empty():
    state = _state(status=["pending"], job_id=["99"])
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={}):
        result = update_state_from_sacct(state)
    assert result.iloc[0]["status"] == "pending"


def test_update_logs_status_change_to_audit(tmp_path):
    state = _state(status=["pending"], job_id=["42"])
    log_path = tmp_path / "audit.jsonl"
    audit = AuditLogger(log_path)
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"42": "complete"}):
        update_state_from_sacct(state, audit=audit)
    import json
    entries = [json.loads(l) for l in log_path.read_text().splitlines()]
    assert any(
        e["event"] == "status_change"
        and e["old_status"] == "pending"
        and e["new_status"] == "complete"
        for e in entries
    )


def test_update_no_audit_when_audit_is_none():
    """update_state_from_sacct with audit=None must not raise."""
    state = _state(status=["pending"], job_id=["99"])
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"99": "complete"}):
        result = update_state_from_sacct(state, audit=None)
    assert result.iloc[0]["status"] == "complete"


def test_update_job_id_not_in_poll_result_stays_unchanged():
    """A pending row whose job_id sacct doesn't know about stays pending."""
    state = _state(status=["pending"], job_id=["999"])
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"888": "complete"}):
        result = update_state_from_sacct(state)
    # job 999 not in poll result → no change
    assert result.iloc[0]["status"] == "pending"


def test_update_skips_null_job_ids():
    """Rows with null job_id result in poll_jobs not being called at all."""
    state = pd.DataFrame({
        "subject": ["sub-0001"],
        "session": ["ses-01"],
        "procedure": ["bids"],
        "status": ["pending"],
        "submitted_at": [pd.Timestamp("2024-01-01")],
        "job_id": [None],
    })
    with patch("snbb_scheduler.monitor.poll_jobs") as mock_poll:
        result = update_state_from_sacct(state)
    # All job IDs are null → no poll needed, returns original state unchanged
    mock_poll.assert_not_called()
    assert result.iloc[0]["status"] == "pending"

"""Tests for monitor.py."""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from snbb_scheduler.monitor import poll_jobs, update_state_from_sacct


# ---------------------------------------------------------------------------
# poll_jobs
# ---------------------------------------------------------------------------

def _sacct_result(stdout: str):
    m = MagicMock()
    m.stdout = stdout
    return m


def test_poll_jobs_empty_list():
    assert poll_jobs([]) == {}


def test_poll_jobs_calls_sacct():
    with patch("subprocess.run", return_value=_sacct_result("12345|COMPLETED\n")) as mock_run:
        poll_jobs(["12345"])
    cmd = mock_run.call_args[0][0]
    assert "sacct" in cmd
    assert "12345" in " ".join(cmd)


def test_poll_jobs_parses_completed():
    with patch("subprocess.run", return_value=_sacct_result("12345|COMPLETED\n")):
        result = poll_jobs(["12345"])
    assert result == {"12345": "complete"}


def test_poll_jobs_state_pending():
    with patch("subprocess.run", return_value=_sacct_result("1|PENDING\n")):
        assert poll_jobs(["1"]) == {"1": "pending"}


def test_poll_jobs_state_running():
    with patch("subprocess.run", return_value=_sacct_result("2|RUNNING\n")):
        assert poll_jobs(["2"]) == {"2": "running"}


def test_poll_jobs_state_failed():
    with patch("subprocess.run", return_value=_sacct_result("3|FAILED\n")):
        assert poll_jobs(["3"]) == {"3": "failed"}


def test_poll_jobs_state_timeout():
    with patch("subprocess.run", return_value=_sacct_result("4|TIMEOUT\n")):
        assert poll_jobs(["4"]) == {"4": "failed"}


def test_poll_jobs_state_cancelled():
    with patch("subprocess.run", return_value=_sacct_result("5|CANCELLED\n")):
        assert poll_jobs(["5"]) == {"5": "failed"}


def test_poll_jobs_state_out_of_memory():
    with patch("subprocess.run", return_value=_sacct_result("6|OUT_OF_MEMORY\n")):
        assert poll_jobs(["6"]) == {"6": "failed"}


def test_poll_jobs_state_node_fail():
    with patch("subprocess.run", return_value=_sacct_result("7|NODE_FAIL\n")):
        assert poll_jobs(["7"]) == {"7": "failed"}


def test_poll_jobs_skips_substeps():
    stdout = "12345|COMPLETED\n12345.batch|COMPLETED\n"
    with patch("subprocess.run", return_value=_sacct_result(stdout)):
        result = poll_jobs(["12345"])
    assert "12345.batch" not in result
    assert result == {"12345": "complete"}


def test_poll_jobs_normalizes_cancelled_with_suffix():
    with patch("subprocess.run", return_value=_sacct_result("8|CANCELLED by user\n")):
        assert poll_jobs(["8"]) == {"8": "failed"}


def test_poll_jobs_ignores_unknown_state():
    with patch("subprocess.run", return_value=_sacct_result("9|REQUEUED\n")):
        assert poll_jobs(["9"]) == {}


def test_poll_jobs_returns_empty_on_called_process_error():
    import subprocess
    with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "sacct")):
        assert poll_jobs(["1"]) == {}


def test_poll_jobs_returns_empty_on_file_not_found():
    with patch("subprocess.run", side_effect=FileNotFoundError):
        assert poll_jobs(["1"]) == {}


# ---------------------------------------------------------------------------
# update_state_from_sacct
# ---------------------------------------------------------------------------

def make_state(*rows):
    """rows: list of (subject, session, procedure, status, job_id)"""
    data = []
    for subject, session, procedure, status, job_id in rows:
        data.append({
            "subject": subject,
            "session": session,
            "procedure": procedure,
            "status": status,
            "submitted_at": pd.Timestamp("2024-01-01"),
            "job_id": job_id,
        })
    return pd.DataFrame(data)


def test_update_state_empty_df():
    state = pd.DataFrame(
        columns=["subject", "session", "procedure", "status", "submitted_at", "job_id"]
    )
    result = update_state_from_sacct(state)
    assert result.empty


def test_update_state_no_in_flight():
    state = make_state(("sub-0001", "ses-01", "bids", "complete", "1"))
    with patch("snbb_scheduler.monitor.poll_jobs") as mock_poll:
        result = update_state_from_sacct(state)
    mock_poll.assert_not_called()
    assert result.iloc[0]["status"] == "complete"


def test_update_state_pending_to_complete():
    state = make_state(("sub-0001", "ses-01", "bids", "pending", "42"))
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"42": "complete"}):
        result = update_state_from_sacct(state)
    assert result.iloc[0]["status"] == "complete"


def test_update_state_pending_to_failed():
    state = make_state(("sub-0001", "ses-01", "bids", "pending", "43"))
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"43": "failed"}):
        result = update_state_from_sacct(state)
    assert result.iloc[0]["status"] == "failed"


def test_update_state_running_to_complete():
    state = make_state(("sub-0001", "ses-01", "bids", "running", "44"))
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"44": "complete"}):
        result = update_state_from_sacct(state)
    assert result.iloc[0]["status"] == "complete"


def test_update_state_complete_not_polled():
    state = make_state(("sub-0001", "ses-01", "bids", "complete", "45"))
    with patch("snbb_scheduler.monitor.poll_jobs") as mock_poll:
        update_state_from_sacct(state)
    mock_poll.assert_not_called()


def test_update_state_original_unchanged():
    state = make_state(("sub-0001", "ses-01", "bids", "pending", "46"))
    original_status = state.iloc[0]["status"]
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"46": "complete"}):
        result = update_state_from_sacct(state)
    assert state.iloc[0]["status"] == original_status
    assert result.iloc[0]["status"] == "complete"


def test_update_state_logs_transition():
    state = make_state(("sub-0001", "ses-01", "bids", "pending", "47"))
    audit = MagicMock()
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"47": "complete"}):
        update_state_from_sacct(state, audit=audit)
    audit.log.assert_called_once_with(
        "status_change",
        subject="sub-0001",
        session="ses-01",
        procedure="bids",
        job_id="47",
        old_status="pending",
        new_status="complete",
    )


def test_update_state_no_audit_when_unchanged():
    state = make_state(("sub-0001", "ses-01", "bids", "pending", "48"))
    audit = MagicMock()
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"48": "pending"}):
        update_state_from_sacct(state, audit=audit)
    audit.log.assert_not_called()


def test_poll_jobs_skips_empty_lines():
    stdout = "\n12345|COMPLETED\n\n"
    with patch("subprocess.run", return_value=_sacct_result(stdout)):
        result = poll_jobs(["12345"])
    assert result == {"12345": "complete"}


def test_poll_jobs_skips_malformed_lines():
    stdout = "not_a_valid_line\n12345|COMPLETED\n"
    with patch("subprocess.run", return_value=_sacct_result(stdout)):
        result = poll_jobs(["12345"])
    assert result == {"12345": "complete"}


def test_update_state_in_flight_no_job_ids():
    """In-flight jobs with None job_id: poll_jobs not called, state unchanged."""
    state = make_state(("sub-0001", "ses-01", "bids", "pending", None))
    with patch("snbb_scheduler.monitor.poll_jobs") as mock_poll:
        result = update_state_from_sacct(state)
    mock_poll.assert_not_called()
    assert result.iloc[0]["status"] == "pending"


def test_update_state_job_not_in_sacct_result():
    """If sacct doesn't return a result for a job, keep original status."""
    state = make_state(("sub-0001", "ses-01", "bids", "pending", "49"))
    # poll_jobs returns empty (e.g. job too old for sacct)
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={}):
        result = update_state_from_sacct(state)
    assert result.iloc[0]["status"] == "pending"


def test_update_state_job_id_missing_from_partial_sacct_result():
    """Job_id in state not returned by sacct (partial results): status unchanged."""
    state = make_state(
        ("sub-0001", "ses-01", "bids", "pending", "50"),
        ("sub-0002", "ses-01", "bids", "pending", "51"),
    )
    # sacct only knows about job 51, not 50
    with patch("snbb_scheduler.monitor.poll_jobs", return_value={"51": "complete"}):
        result = update_state_from_sacct(state)
    assert result.iloc[0]["status"] == "pending"
    assert result.iloc[1]["status"] == "complete"

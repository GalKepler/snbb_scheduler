"""Tests for CompletionCache (SQLite-backed completion status cache)."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from snbb_scheduler.cache import CompletionCache


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def cache(tmp_path: Path) -> CompletionCache:
    """Open a fresh cache in a tmp directory."""
    c = CompletionCache(tmp_path / "cache.db", ttl_hours=24)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Basic read/write
# ---------------------------------------------------------------------------


def test_get_missing_returns_none(cache: CompletionCache) -> None:
    assert cache.get("sub-0001", "ses-01", "bids") is None


def test_set_and_get_true(cache: CompletionCache) -> None:
    cache.set("sub-0001", "ses-01", "bids", True)
    assert cache.get("sub-0001", "ses-01", "bids") is True


def test_set_and_get_false(cache: CompletionCache) -> None:
    cache.set("sub-0001", "ses-01", "bids", False)
    assert cache.get("sub-0001", "ses-01", "bids") is False


def test_replace_updates_value(cache: CompletionCache) -> None:
    cache.set("sub-0001", "ses-01", "bids", False)
    cache.set("sub-0001", "ses-01", "bids", True)
    assert cache.get("sub-0001", "ses-01", "bids") is True


def test_different_keys_independent(cache: CompletionCache) -> None:
    cache.set("sub-0001", "ses-01", "bids", True)
    cache.set("sub-0002", "ses-01", "bids", False)
    assert cache.get("sub-0001", "ses-01", "bids") is True
    assert cache.get("sub-0002", "ses-01", "bids") is False


def test_subject_scoped_empty_session(cache: CompletionCache) -> None:
    """Subject-scoped procedures use empty string for session."""
    cache.set("sub-0001", "", "freesurfer", True)
    assert cache.get("sub-0001", "", "freesurfer") is True
    assert cache.get("sub-0001", "ses-01", "freesurfer") is None


# ---------------------------------------------------------------------------
# set_many
# ---------------------------------------------------------------------------


def test_set_many(cache: CompletionCache) -> None:
    rows = [
        ("sub-0001", "ses-01", "bids", True),
        ("sub-0001", "ses-02", "bids", False),
        ("sub-0002", "ses-01", "bids", True),
    ]
    cache.set_many(rows)
    assert cache.get("sub-0001", "ses-01", "bids") is True
    assert cache.get("sub-0001", "ses-02", "bids") is False
    assert cache.get("sub-0002", "ses-01", "bids") is True


# ---------------------------------------------------------------------------
# TTL / staleness
# ---------------------------------------------------------------------------


def test_ttl_zero_never_expires(tmp_path: Path) -> None:
    c = CompletionCache(tmp_path / "c.db", ttl_hours=0)
    c.set("sub-0001", "ses-01", "bids", True)
    # Even with zero sleep — TTL=0 means never expire
    assert c.get("sub-0001", "ses-01", "bids") is True
    c.close()


def test_stale_entry_returns_none(tmp_path: Path) -> None:
    """An entry written with a past timestamp should be treated as stale."""
    from datetime import datetime, timedelta, timezone

    c = CompletionCache(tmp_path / "c.db", ttl_hours=1)
    # Manually insert a stale entry (2 hours ago)
    stale_ts = (datetime.now(tz=timezone.utc) - timedelta(hours=2)).isoformat()
    c._con.execute(
        "INSERT OR REPLACE INTO completion_cache "
        "(subject, session, procedure, is_complete, checked_at) VALUES (?,?,?,?,?)",
        ("sub-0001", "ses-01", "bids", 1, stale_ts),
    )
    c._con.commit()
    assert c.get("sub-0001", "ses-01", "bids") is None
    c.close()


def test_is_fresh_true_for_recent_entry(cache: CompletionCache) -> None:
    cache.set("sub-0001", "ses-01", "bids", True)
    assert cache.is_fresh("sub-0001", "ses-01", "bids") is True


def test_is_fresh_false_for_missing(cache: CompletionCache) -> None:
    assert cache.is_fresh("sub-0001", "ses-01", "bids") is False


# ---------------------------------------------------------------------------
# get_all
# ---------------------------------------------------------------------------


def test_get_all_returns_all(cache: CompletionCache) -> None:
    cache.set_many([
        ("sub-0001", "ses-01", "bids", True),
        ("sub-0001", "ses-01", "qsiprep", False),
        ("sub-0002", "ses-01", "bids", True),
    ])
    results = cache.get_all()
    assert len(results) == 3


def test_get_all_filter_subject(cache: CompletionCache) -> None:
    cache.set_many([
        ("sub-0001", "ses-01", "bids", True),
        ("sub-0002", "ses-01", "bids", True),
    ])
    results = cache.get_all(subject="sub-0001")
    assert len(results) == 1
    assert results[0]["subject"] == "sub-0001"


def test_get_all_filter_procedure(cache: CompletionCache) -> None:
    cache.set_many([
        ("sub-0001", "ses-01", "bids", True),
        ("sub-0001", "ses-01", "qsiprep", False),
    ])
    results = cache.get_all(procedure="bids")
    assert len(results) == 1
    assert results[0]["procedure"] == "bids"


# ---------------------------------------------------------------------------
# clear
# ---------------------------------------------------------------------------


def test_clear_all(cache: CompletionCache) -> None:
    cache.set_many([
        ("sub-0001", "ses-01", "bids", True),
        ("sub-0002", "ses-01", "bids", True),
    ])
    n = cache.clear()
    assert n == 2
    assert cache.get_all() == []


def test_clear_by_subject(cache: CompletionCache) -> None:
    cache.set_many([
        ("sub-0001", "ses-01", "bids", True),
        ("sub-0002", "ses-01", "bids", True),
    ])
    n = cache.clear(subject="sub-0001")
    assert n == 1
    assert cache.get("sub-0001", "ses-01", "bids") is None
    assert cache.get("sub-0002", "ses-01", "bids") is True


def test_clear_by_procedure(cache: CompletionCache) -> None:
    cache.set_many([
        ("sub-0001", "ses-01", "bids", True),
        ("sub-0001", "ses-01", "qsiprep", True),
    ])
    cache.clear(procedure="bids")
    assert cache.get("sub-0001", "ses-01", "bids") is None
    assert cache.get("sub-0001", "ses-01", "qsiprep") is True


# ---------------------------------------------------------------------------
# status_matrix
# ---------------------------------------------------------------------------


def test_status_matrix(cache: CompletionCache) -> None:
    cache.set_many([
        ("sub-0001", "ses-01", "bids", True),
        ("sub-0001", "ses-01", "qsiprep", False),
    ])
    matrix = cache.status_matrix()
    statuses = {(r["subject"], r["procedure"]): r["status"] for r in matrix}
    assert statuses[("sub-0001", "bids")] == "complete"
    assert statuses[("sub-0001", "qsiprep")] == "incomplete"


def test_status_matrix_filter_procedures(cache: CompletionCache) -> None:
    cache.set_many([
        ("sub-0001", "ses-01", "bids", True),
        ("sub-0001", "ses-01", "qsiprep", False),
    ])
    matrix = cache.status_matrix(procedures=["bids"])
    assert len(matrix) == 1
    assert matrix[0]["procedure"] == "bids"


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


def test_context_manager(tmp_path: Path) -> None:
    with CompletionCache(tmp_path / "c.db") as c:
        c.set("sub-0001", "ses-01", "bids", True)
    # Connection closed — re-opening should still see the entry
    with CompletionCache(tmp_path / "c.db") as c:
        assert c.get("sub-0001", "ses-01", "bids") is True


# ---------------------------------------------------------------------------
# Persistence across open/close
# ---------------------------------------------------------------------------


def test_persists_across_reopens(tmp_path: Path) -> None:
    path = tmp_path / "c.db"
    c = CompletionCache(path)
    c.set("sub-0001", "ses-01", "bids", True)
    c.close()

    c2 = CompletionCache(path)
    assert c2.get("sub-0001", "ses-01", "bids") is True
    c2.close()

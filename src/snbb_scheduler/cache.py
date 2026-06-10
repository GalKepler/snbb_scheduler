from __future__ import annotations

__all__ = ["CompletionCache"]

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


_DDL = """
CREATE TABLE IF NOT EXISTS completion_cache (
    subject     TEXT NOT NULL,
    session     TEXT NOT NULL,
    procedure   TEXT NOT NULL,
    is_complete INTEGER NOT NULL,
    checked_at  TEXT NOT NULL,
    PRIMARY KEY (subject, session, procedure)
)
"""


class CompletionCache:
    """SQLite-backed cache of per-(subject, session, procedure) completion status.

    The cache is safe to open from multiple processes — SQLite provides
    serialised writes via WAL mode. Reads are always fast O(1) index lookups.

    Parameters
    ----------
    path:
        Path to the SQLite database file. Created on first use.
    ttl_hours:
        Entries older than this many hours are considered stale and will be
        re-verified on the next explicit ``set()`` call. A value of 0 means
        entries never expire (useful for read-only queries after a scan).
    """

    def __init__(self, path: Path, ttl_hours: int = 24) -> None:
        self._path = Path(path)
        self._ttl = timedelta(hours=ttl_hours)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(str(self._path), check_same_thread=False)
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute(_DDL)
        self._con.commit()

    # ------------------------------------------------------------------ #
    # Write                                                                #
    # ------------------------------------------------------------------ #

    def set(self, subject: str, session: str, procedure: str, is_complete: bool) -> None:
        """Insert or replace a completion entry with the current timestamp."""
        now = datetime.now(tz=timezone.utc).isoformat()
        self._con.execute(
            "INSERT OR REPLACE INTO completion_cache "
            "(subject, session, procedure, is_complete, checked_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (subject, session, procedure, int(is_complete), now),
        )
        self._con.commit()

    def set_many(self, rows: list[tuple[str, str, str, bool]]) -> None:
        """Bulk-insert completion entries. Each row: (subject, session, procedure, is_complete)."""
        now = datetime.now(tz=timezone.utc).isoformat()
        self._con.executemany(
            "INSERT OR REPLACE INTO completion_cache "
            "(subject, session, procedure, is_complete, checked_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [(s, ses, p, int(c), now) for s, ses, p, c in rows],
        )
        self._con.commit()

    # ------------------------------------------------------------------ #
    # Read                                                                 #
    # ------------------------------------------------------------------ #

    def get(
        self, subject: str, session: str, procedure: str
    ) -> bool | None:
        """Return cached completion status, or None if missing/stale."""
        row = self._con.execute(
            "SELECT is_complete, checked_at FROM completion_cache "
            "WHERE subject=? AND session=? AND procedure=?",
            (subject, session, procedure),
        ).fetchone()
        if row is None:
            return None
        if self._ttl.total_seconds() > 0 and self._is_stale(row[1]):
            return None
        return bool(row[0])

    def is_fresh(self, subject: str, session: str, procedure: str) -> bool:
        """Return True only if a non-stale entry exists (regardless of value)."""
        row = self._con.execute(
            "SELECT checked_at FROM completion_cache "
            "WHERE subject=? AND session=? AND procedure=?",
            (subject, session, procedure),
        ).fetchone()
        if row is None:
            return False
        return not self._is_stale(row[0])

    def get_all(
        self,
        subject: str | None = None,
        procedure: str | None = None,
        fresh_only: bool = True,
    ) -> list[dict]:
        """Return all cached entries, optionally filtered.

        Parameters
        ----------
        subject:
            Restrict to a single subject.
        procedure:
            Restrict to a single procedure.
        fresh_only:
            When True (default), exclude stale entries.

        Returns
        -------
        List of dicts with keys: subject, session, procedure, is_complete, checked_at.
        """
        clauses: list[str] = []
        params: list[str] = []
        if subject is not None:
            clauses.append("subject=?")
            params.append(subject)
        if procedure is not None:
            clauses.append("procedure=?")
            params.append(procedure)

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self._con.execute(
            f"SELECT subject, session, procedure, is_complete, checked_at "
            f"FROM completion_cache {where} ORDER BY subject, session, procedure",
            params,
        ).fetchall()

        result = []
        for s, ses, p, ic, ca in rows:
            if fresh_only and self._ttl.total_seconds() > 0 and self._is_stale(ca):
                continue
            result.append({
                "subject": s,
                "session": ses,
                "procedure": p,
                "is_complete": bool(ic),
                "checked_at": ca,
            })
        return result

    # ------------------------------------------------------------------ #
    # Invalidation                                                         #
    # ------------------------------------------------------------------ #

    def clear(
        self,
        subject: str | None = None,
        session: str | None = None,
        procedure: str | None = None,
    ) -> int:
        """Delete matching cache entries. Returns count of deleted rows."""
        clauses: list[str] = []
        params: list[str] = []
        if subject is not None:
            clauses.append("subject=?")
            params.append(subject)
        if session is not None:
            clauses.append("session=?")
            params.append(session)
        if procedure is not None:
            clauses.append("procedure=?")
            params.append(procedure)

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        cur = self._con.execute(f"DELETE FROM completion_cache {where}", params)
        self._con.commit()
        return cur.rowcount

    # ------------------------------------------------------------------ #
    # Status export                                                        #
    # ------------------------------------------------------------------ #

    def status_matrix(
        self,
        subjects: list[str] | None = None,
        procedures: list[str] | None = None,
    ) -> list[dict]:
        """Return a flat list of status dicts, one per (subject, session, procedure).

        Suitable for building a pivot table in pandas.
        Each dict has: subject, session, procedure, status
        where status is 'complete' or 'incomplete'.
        """
        entries = self.get_all(fresh_only=False)
        result = []
        for e in entries:
            if subjects is not None and e["subject"] not in subjects:
                continue
            if procedures is not None and e["procedure"] not in procedures:
                continue
            result.append({
                "subject": e["subject"],
                "session": e["session"],
                "procedure": e["procedure"],
                "status": "complete" if e["is_complete"] else "incomplete",
            })
        return result

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> "CompletionCache":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _is_stale(self, checked_at: str) -> bool:
        if self._ttl.total_seconds() <= 0:
            return False
        try:
            ts = datetime.fromisoformat(checked_at)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return (datetime.now(tz=timezone.utc) - ts) > self._ttl
        except ValueError:
            return True

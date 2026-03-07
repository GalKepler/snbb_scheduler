from __future__ import annotations

__all__ = [
    "DicomAuditResult",
    "ProcedureAuditResult",
    "SessionAuditResult",
    "ProcedureSummary",
    "AuditReport",
    "audit_dicom",
    "audit_session",
    "audit_procedure",
    "run_full_audit",
]

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from snbb_scheduler.checks import FileCheckResult, check_detailed
from snbb_scheduler.config import Procedure, SchedulerConfig
from snbb_scheduler.log_analyzer import LogFinding, analyze_task_logs
from snbb_scheduler.manifest import load_state
from snbb_scheduler.rules import _completion_kwargs
from snbb_scheduler.sessions import discover_sessions

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class DicomAuditResult:
    """Audit result for DICOM source data for one session."""

    subject: str
    session: str
    dicom_path: str | None
    exists: bool
    file_count: int
    has_expected_structure: bool  # subdirectories present
    is_suspicious: bool  # file_count < threshold
    detail: str


@dataclass
class ProcedureAuditResult:
    """Audit result for one procedure applied to one session."""

    procedure: str
    subject: str
    session: str
    status: str  # complete/incomplete/missing/failed/not_started
    file_checks: list[FileCheckResult] = field(default_factory=list)
    completeness_ratio: float = 0.0  # 0.0-1.0
    state_status: str | None = None  # from parquet: pending/running/complete/failed/None
    is_stale: bool = False
    job_age_hours: float | None = None
    log_findings: list[LogFinding] = field(default_factory=list)


@dataclass
class SessionAuditResult:
    """Full audit result for one (subject, session) pair."""

    subject: str
    session: str
    dicom: DicomAuditResult
    procedures: dict[str, ProcedureAuditResult] = field(default_factory=dict)
    health_score: float = 0.0  # 0.0-1.0


@dataclass
class ProcedureSummary:
    """Aggregate statistics for one procedure across all sessions."""

    procedure: str
    total_sessions: int
    complete: int
    incomplete: int
    failed: int
    not_started: int
    stale: int
    common_errors: list[tuple[str, int]] = field(default_factory=list)


@dataclass
class AuditReport:
    """Top-level audit report."""

    timestamp: str
    config_summary: dict
    session_results: list[SessionAuditResult] = field(default_factory=list)
    procedure_summaries: list[ProcedureSummary] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Phase 2: DICOM source validation
# ---------------------------------------------------------------------------


def audit_dicom(
    subject: str,
    session: str,
    sessions_df: pd.DataFrame,
    config: SchedulerConfig,
) -> DicomAuditResult:
    """Validate DICOM source data for one session.

    Parameters
    ----------
    subject, session:
        Identifiers for the session to audit.
    sessions_df:
        DataFrame from ``discover_sessions()``.
    config:
        Scheduler config (used for threshold via ``config.audit``).

    Returns
    -------
    DicomAuditResult
    """
    # Find DICOM path from sessions DataFrame
    mask = (sessions_df["subject"] == subject) & (sessions_df["session"] == session)
    matched = sessions_df[mask]

    dicom_path_str: str | None = None
    if not matched.empty and "dicom_path" in matched.columns:
        raw = matched.iloc[0]["dicom_path"]
        dicom_path_str = str(raw) if raw is not None else None

    if dicom_path_str is None:
        # Fallback: construct from dicom_root
        dicom_path_str = str(config.dicom_root / subject / session)

    dicom_path = Path(dicom_path_str)

    if not dicom_path.exists():
        return DicomAuditResult(
            subject=subject,
            session=session,
            dicom_path=dicom_path_str,
            exists=False,
            file_count=0,
            has_expected_structure=False,
            is_suspicious=True,
            detail="DICOM directory does not exist",
        )

    file_count = sum(1 for f in dicom_path.rglob("*") if f.is_file())
    has_subdirs = any(d.is_dir() for d in dicom_path.iterdir())
    threshold = config.audit.dicom_min_files
    is_suspicious = file_count < threshold

    detail_parts = [f"{file_count} file(s)"]
    if not has_subdirs:
        detail_parts.append("no subdirectories")
    if is_suspicious:
        detail_parts.append(f"below threshold of {threshold}")

    return DicomAuditResult(
        subject=subject,
        session=session,
        dicom_path=dicom_path_str,
        exists=True,
        file_count=file_count,
        has_expected_structure=has_subdirs,
        is_suspicious=is_suspicious,
        detail=", ".join(detail_parts),
    )


# ---------------------------------------------------------------------------
# Phase 4: Core audit engine
# ---------------------------------------------------------------------------


def _audit_one_procedure(
    proc: Procedure,
    subject: str,
    session: str,
    config: SchedulerConfig,
    state: pd.DataFrame,
) -> ProcedureAuditResult:
    """Audit a single procedure for one session."""
    root = config.get_procedure_root(proc)
    output_path = root / subject if proc.scope == "subject" else root / subject / session

    # Build completion kwargs (same logic as rules.py / manifest.py)
    _row = pd.Series({"subject": subject, "session": session or ""})
    kwargs = _completion_kwargs(proc, _row, config)

    file_checks = check_detailed(proc, output_path, **kwargs)
    total = len(file_checks)
    found_count = sum(1 for fc in file_checks if fc.found)
    completeness_ratio = found_count / total if total > 0 else 0.0

    if total == 0:
        fs_status = "missing"
    elif found_count == total:
        fs_status = "complete"
    elif found_count == 0:
        fs_status = "incomplete"
    else:
        fs_status = "incomplete"

    # Look up state
    state_status: str | None = None
    is_stale = False
    job_age_hours: float | None = None

    if not state.empty:
        smask = (state["subject"] == subject) & (state["procedure"] == proc.name)
        if session:
            smask &= state["session"] == session
        matched = state[smask]
        if not matched.empty:
            row = matched.iloc[-1]  # most recent
            state_status = str(row["status"])
            submitted_at = row.get("submitted_at")
            if submitted_at is not None and pd.notna(submitted_at):
                try:
                    if hasattr(submitted_at, "to_pydatetime"):
                        submitted_dt = submitted_at.to_pydatetime()
                    else:
                        submitted_dt = pd.Timestamp(submitted_at).to_pydatetime()
                    now = datetime.now(timezone.utc)
                    if submitted_dt.tzinfo is None:
                        from datetime import timezone as tz
                        submitted_dt = submitted_dt.replace(tzinfo=tz.utc)
                    age = now - submitted_dt
                    job_age_hours = age.total_seconds() / 3600.0
                    threshold_hours = config.audit.stale_job_threshold_hours
                    if state_status in ("pending", "running") and job_age_hours > threshold_hours:
                        is_stale = True
                except Exception:  # noqa: BLE001
                    pass

    # Determine combined status
    if state_status == "failed":
        status = "failed"
    elif state_status == "complete" or fs_status == "complete":
        status = "complete"
    elif state_status in ("pending", "running"):
        status = state_status
    elif fs_status == "incomplete":
        status = "incomplete"
    else:
        status = "not_started"

    # Analyze logs
    log_findings = analyze_task_logs(subject, session, proc.name, config, state)

    return ProcedureAuditResult(
        procedure=proc.name,
        subject=subject,
        session=session,
        status=status,
        file_checks=file_checks,
        completeness_ratio=completeness_ratio,
        state_status=state_status,
        is_stale=is_stale,
        job_age_hours=job_age_hours,
        log_findings=log_findings,
    )


def audit_session(
    subject: str,
    session: str,
    config: SchedulerConfig,
    sessions_df: pd.DataFrame,
    state: pd.DataFrame,
) -> SessionAuditResult:
    """Full audit of one session: DICOM + all procedures + logs.

    Parameters
    ----------
    subject, session:
        Session identifiers.
    config:
        Scheduler configuration.
    sessions_df:
        DataFrame from ``discover_sessions()``.
    state:
        State DataFrame from ``load_state()``.
    """
    dicom_result = audit_dicom(subject, session, sessions_df, config)

    proc_results: dict[str, ProcedureAuditResult] = {}
    for proc in config.procedures:
        # Subject-scoped procedures apply to all sessions but only need auditing once per subject.
        # When called per-session we still audit to provide status context, but callers
        # should be aware results are duplicated across sessions for subject-scoped procs.
        proc_results[proc.name] = _audit_one_procedure(proc, subject, session, config, state)

    # Health score: fraction of complete procedures
    complete_count = sum(1 for r in proc_results.values() if r.status == "complete")
    health_score = complete_count / len(proc_results) if proc_results else 0.0

    return SessionAuditResult(
        subject=subject,
        session=session,
        dicom=dicom_result,
        procedures=proc_results,
        health_score=health_score,
    )


def audit_procedure(
    proc_name: str,
    config: SchedulerConfig,
    sessions_df: pd.DataFrame,
    state: pd.DataFrame,
) -> ProcedureSummary:
    """Aggregate stats for one procedure across all sessions.

    Parameters
    ----------
    proc_name:
        Name of the procedure to summarize.
    config, sessions_df, state:
        Standard inputs.
    """
    proc = config.get_procedure(proc_name)

    # For subject-scoped procedures, de-duplicate by subject
    if proc.scope == "subject":
        groups = sessions_df[["subject"]].drop_duplicates().to_dict("records")
        items = [(row["subject"], "") for row in groups]
    else:
        items = [
            (row["subject"], row["session"])
            for _, row in sessions_df.iterrows()
        ]

    total = len(items)
    complete = incomplete = failed = not_started = stale = 0
    error_counts: dict[str, int] = {}

    for subject, session in items:
        result = _audit_one_procedure(proc, subject, session, config, state)
        if result.status == "complete":
            complete += 1
        elif result.status == "failed":
            failed += 1
        elif result.status == "not_started":
            not_started += 1
        else:
            incomplete += 1
        if result.is_stale:
            stale += 1
        for finding in result.log_findings:
            error_counts[finding.pattern_name] = error_counts.get(finding.pattern_name, 0) + 1

    common_errors = sorted(error_counts.items(), key=lambda x: -x[1])[:5]

    return ProcedureSummary(
        procedure=proc_name,
        total_sessions=total,
        complete=complete,
        incomplete=incomplete,
        failed=failed,
        not_started=not_started,
        stale=stale,
        common_errors=common_errors,
    )


def run_full_audit(config: SchedulerConfig) -> AuditReport:
    """Orchestrate a full audit: discover sessions, load state, audit everything.

    Parameters
    ----------
    config:
        Scheduler configuration.

    Returns
    -------
    AuditReport
    """
    sessions_df = discover_sessions(config)
    state = load_state(config)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    config_summary = {
        "dicom_root": str(config.dicom_root),
        "bids_root": str(config.bids_root),
        "derivatives_root": str(config.derivatives_root),
        "procedures": [p.name for p in config.procedures],
        "state_file": str(config.state_file),
    }

    # Audit each session
    session_results: list[SessionAuditResult] = []
    seen_sessions: set[tuple[str, str]] = set()

    for _, row in sessions_df.iterrows():
        subject = row["subject"]
        session = row["session"]
        key = (subject, session)
        if key in seen_sessions:
            continue
        seen_sessions.add(key)
        result = audit_session(subject, session, config, sessions_df, state)
        session_results.append(result)

    # Procedure summaries
    procedure_summaries = [
        audit_procedure(proc.name, config, sessions_df, state)
        for proc in config.procedures
    ]

    return AuditReport(
        timestamp=timestamp,
        config_summary=config_summary,
        session_results=session_results,
        procedure_summaries=procedure_summaries,
    )

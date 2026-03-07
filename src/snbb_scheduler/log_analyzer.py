from __future__ import annotations

__all__ = [
    "LogPattern",
    "LogFinding",
    "DEFAULT_LOG_PATTERNS",
    "analyze_log_file",
    "find_logs_for_task",
    "analyze_task_logs",
]

import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

    from snbb_scheduler.config import SchedulerConfig


@dataclass
class LogPattern:
    """A named regex pattern used to scan Slurm log files."""

    name: str
    regex: str
    severity: str  # "error" | "warning"
    description: str


@dataclass
class LogFinding:
    """A single match of a LogPattern in a log file."""

    pattern_name: str
    severity: str
    line_number: int
    line_text: str
    log_file: str


DEFAULT_LOG_PATTERNS: list[LogPattern] = [
    LogPattern(
        name="oom",
        regex=r"(?i)(out.of.memory|oom.kill|killed.process|memory.limit.exceeded|slurmstepd.*Killed)",
        severity="error",
        description="Out-of-memory kill",
    ),
    LogPattern(
        name="timeout",
        regex=r"(?i)(DUE TO TIME LIMIT|timed out|TIMEOUT|exceeded.*time)",
        severity="error",
        description="Job exceeded time limit",
    ),
    LogPattern(
        name="container_error",
        regex=r"(?i)(apptainer.*error|singularity.*error|container.*failed|FATAL.*apptainer|FATAL.*singularity)",
        severity="error",
        description="Container runtime error",
    ),
    LogPattern(
        name="missing_file",
        regex=r"(?i)(No such file or directory|FileNotFoundError|cannot open.*no such)",
        severity="error",
        description="Missing file or directory",
    ),
    LogPattern(
        name="permission_denied",
        regex=r"(?i)(Permission denied|Access denied|Operation not permitted)",
        severity="error",
        description="File permission error",
    ),
    LogPattern(
        name="disk_full",
        regex=r"(?i)(No space left on device|disk.*full|quota.*exceeded|write.*failed.*no space)",
        severity="error",
        description="Disk full or quota exceeded",
    ),
    LogPattern(
        name="segfault",
        regex=r"(?i)(Segmentation fault|segfault|signal 11|core dumped)",
        severity="error",
        description="Segmentation fault",
    ),
    LogPattern(
        name="python_traceback",
        regex=r"^Traceback \(most recent call last\)",
        severity="error",
        description="Python exception traceback",
    ),
    LogPattern(
        name="freesurfer_error",
        regex=r"(?i)(recon-all.*ERROR|mri_convert.*error|ERROR: recon-all|mris_.*failed)",
        severity="error",
        description="FreeSurfer processing error",
    ),
    LogPattern(
        name="qsiprep_error",
        regex=r"(?i)(qsiprep.*error|nipype.*error|RuntimeError.*qsiprep)",
        severity="error",
        description="QSIPrep processing error",
    ),
    LogPattern(
        name="qsiprep_warning",
        regex=r"(?i)(qsiprep.*warning|UserWarning.*qsiprep|nipype.*warning)",
        severity="warning",
        description="QSIPrep processing warning",
    ),
    LogPattern(
        name="slurm_node_fail",
        regex=r"(?i)(node.*fail|slurmstepd.*error|job.*node.*down)",
        severity="error",
        description="Slurm node failure",
    ),
    LogPattern(
        name="cuda_error",
        regex=r"(?i)(CUDA.*error|cudaError|GPU.*error|RuntimeError.*CUDA)",
        severity="error",
        description="CUDA / GPU error",
    ),
]


def analyze_log_file(
    log_path: Path,
    patterns: list[LogPattern] | None = None,
) -> list[LogFinding]:
    """Scan a single log file for known error patterns.

    Parameters
    ----------
    log_path:
        Path to the ``.out`` or ``.err`` log file.
    patterns:
        List of patterns to search for.  Defaults to ``DEFAULT_LOG_PATTERNS``.

    Returns
    -------
    list[LogFinding]
        One entry per matched line (may include multiple per pattern).
    """
    if patterns is None:
        patterns = DEFAULT_LOG_PATTERNS

    compiled = [(p, re.compile(p.regex)) for p in patterns]
    findings: list[LogFinding] = []

    try:
        text = log_path.read_text(errors="replace")
    except OSError:
        return findings

    for lineno, line in enumerate(text.splitlines(), start=1):
        for pattern, regex in compiled:
            if regex.search(line):
                findings.append(
                    LogFinding(
                        pattern_name=pattern.name,
                        severity=pattern.severity,
                        line_number=lineno,
                        line_text=line.rstrip(),
                        log_file=str(log_path),
                    )
                )

    return findings


def find_logs_for_task(
    subject: str,
    session: str,
    procedure: str,
    config: "SchedulerConfig",
    state: "pd.DataFrame",
) -> list[Path]:
    """Locate Slurm log files for a given task.

    Uses the same naming convention as ``submit.py``:
    ``<slurm_log_dir>/<procedure>/<job_name>_<job_id>.{out,err}``

    Falls back to glob-based discovery when job_id is not available.

    Parameters
    ----------
    subject, session, procedure:
        Task identifiers.
    config:
        Scheduler configuration (needs ``slurm_log_dir``).
    state:
        State DataFrame to look up ``job_id``.

    Returns
    -------
    list[Path]
        Existing log files (may be empty).
    """
    if config.slurm_log_dir is None:
        return []

    log_dir = config.slurm_log_dir / procedure
    if not log_dir.exists():
        return []

    # Try to find via job_id from state
    job_ids: list[str] = []
    if not state.empty:
        mask = (
            (state["subject"] == subject)
            & (state["procedure"] == procedure)
        )
        if session:
            mask &= state["session"] == session
        matched = state[mask]["job_id"].dropna().astype(str).tolist()
        job_ids = [j for j in matched if j and j != "nan"]

    found: list[Path] = []
    if job_ids:
        for job_id in job_ids:
            for suffix in ("out", "err"):
                # glob for files ending in _{job_id}.suffix
                for f in log_dir.glob(f"*_{job_id}.{suffix}"):
                    if f not in found:
                        found.append(f)
    else:
        # Fallback: glob by subject+procedure
        key = f"{subject}_{session}" if session else subject
        for suffix in ("out", "err"):
            for f in log_dir.glob(f"*{key}*.{suffix}"):
                if f not in found:
                    found.append(f)

    return found


def analyze_task_logs(
    subject: str,
    session: str,
    procedure: str,
    config: "SchedulerConfig",
    state: "pd.DataFrame",
    patterns: list[LogPattern] | None = None,
) -> list[LogFinding]:
    """Locate and analyze all Slurm logs for a task.

    Convenience wrapper combining :func:`find_logs_for_task` and
    :func:`analyze_log_file`.
    """
    log_files = find_logs_for_task(subject, session, procedure, config, state)
    findings: list[LogFinding] = []
    for log_file in log_files:
        findings.extend(analyze_log_file(log_file, patterns=patterns))
    return findings

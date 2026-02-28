from __future__ import annotations

__all__ = ["Rule", "build_rules"]

from typing import Callable

import pandas as pd

from snbb_scheduler.checks import is_complete
from snbb_scheduler.config import Procedure, SchedulerConfig

# Type alias for a rule function
Rule = Callable[[pd.Series], bool]


def build_rules(
    config: SchedulerConfig,
    sessions_df: pd.DataFrame | None = None,
    force: bool = False,
    force_procedures: list[str] | None = None,
) -> dict[str, Rule]:
    """Generate a rule function for every procedure in config.

    Each rule returns ``True`` when all of the following hold:

    1. DICOM data exists for the session (``dicom_exists``).
    2. All upstream procedures listed in ``proc.depends_on`` are complete.
    3. This procedure's own output is not yet complete.
       (Skipped when *force* is ``True`` and the procedure is in
       *force_procedures*, or when *force* is ``True`` and
       *force_procedures* is ``None``.)

    Parameters
    ----------
    config:
        Scheduler configuration containing the procedure registry.
    sessions_df:
        Full sessions DataFrame produced by :func:`~snbb_scheduler.sessions.discover_sessions`.
        Required for correct evaluation of **cross-scope dependencies** —
        that is, a *subject*-scoped procedure that depends on a
        *session*-scoped one.  When ``None``, cross-scope dependency
        checking is skipped (safe for all currently-defined procedures
        that do not have cross-scope dependencies).
    force:
        When ``True``, skip the self-completion check for matching procedures.
    force_procedures:
        If given, limit forced re-submission to this procedure name list.
    """
    return {
        proc.name: _make_rule(
            proc, config,
            sessions_df=sessions_df,
            force=force,
            force_procedures=force_procedures,
        )
        for proc in config.procedures
    }


def _completion_kwargs(proc: Procedure, row: pd.Series, config: SchedulerConfig) -> dict:
    """Return extra keyword arguments for ``is_complete`` based on procedure name.

    Some specialised checks require additional context (e.g. the BIDS root
    and subject label) that cannot be derived from the output path alone.
    This helper centralises that mapping so that ``_make_rule`` stays clean.

    The same mapping is used by :func:`~snbb_scheduler.manifest.reconcile_with_filesystem`
    to avoid duplicating the per-procedure kwarg logic.
    """
    subject = row["subject"]
    if proc.name in ("freesurfer", "qsiprep"):
        return {"bids_root": config.bids_root, "subject": subject}
    if proc.name == "qsirecon":
        return {"derivatives_root": config.derivatives_root, "subject": subject}
    if proc.name == "fastsurfer":
        return {
            "bids_root": config.bids_root,
            "derivatives_root": config.derivatives_root,
            "subject": subject,
        }
    return {}


def _make_rule(
    proc: Procedure,
    config: SchedulerConfig,
    sessions_df: pd.DataFrame | None = None,
    force: bool = False,
    force_procedures: list[str] | None = None,
) -> Rule:
    """Create a rule closure that decides whether *proc* needs to run for a session.

    The returned callable accepts a session row (``pd.Series``) and returns
    ``True`` when **all** of the following hold:

    1. DICOM data exists for the session (``dicom_exists`` is ``True``).
    2. Every procedure in ``proc.depends_on`` is already complete on disk.
    3. This procedure's own output is **not** yet complete on disk.

    Cross-scope dependencies (``fastsurfer`` only)
    -----------------------------------------------
    ``fastsurfer`` is subject-scoped and depends on ``bids_post``
    (session-scoped).  A simple per-row check is insufficient — the
    FastSurfer job should only start once **all** sessions of the subject
    have ``bids_post`` complete.

    If *sessions_df* is provided, the rule iterates every session row for
    the current subject and requires ``bids_post`` to be complete in all
    of them.  The script itself decides whether to run cross-sectional or
    longitudinal based on the session count it discovers.

    Other subject-scoped procedures (e.g. ``qsiprep``, ``freesurfer``) use
    the standard per-row dependency check even when their dependency is
    session-scoped; this preserves the original one-session-at-a-time
    behaviour for those tools.
    """
    # Pre-classify dependencies as same-scope or cross-scope once, outside
    # the inner closure, to avoid repeated work on every rule evaluation.
    #
    # Cross-scope logic applies to ``fastsurfer``, which must wait until
    # ALL sessions of the subject have their session-scoped dependency
    # (``bids_post``) complete before the subject-scoped run can start.
    # Other subject-scoped procedures (e.g. ``qsiprep``, ``freesurfer``)
    # check the current session row's dependency path, which is the original
    # per-row behaviour.
    cross_scope_deps: list[str] = []
    same_scope_deps: list[str] = []
    for dep_name in proc.depends_on:
        dep_proc = config.get_procedure(dep_name)
        if proc.name == "fastsurfer" and dep_proc.scope == "session":
            cross_scope_deps.append(dep_name)
        else:
            same_scope_deps.append(dep_name)

    def rule(row: pd.Series) -> bool:
        if not row["dicom_exists"]:
            return False

        # ── Same-scope dependencies (existing logic) ──────────────────────
        for dep_name in same_scope_deps:
            dep_proc = config.get_procedure(dep_name)
            dep_kwargs = _completion_kwargs(dep_proc, row, config)
            if not is_complete(dep_proc, row[f"{dep_name}_path"], **dep_kwargs):
                return False

        # ── Cross-scope dependencies ──────────────────────────────────────
        # A subject-scoped procedure depends on a session-scoped one.
        # All sessions of the subject must have the dependency complete.
        if cross_scope_deps and sessions_df is not None:
            subject = row["subject"]
            subject_rows = sessions_df[sessions_df["subject"] == subject]
            for dep_name in cross_scope_deps:
                dep_proc = config.get_procedure(dep_name)
                for _, srow in subject_rows.iterrows():
                    if not srow.get("dicom_exists", False):
                        continue
                    dep_kw = _completion_kwargs(dep_proc, srow, config)
                    if not is_complete(dep_proc, srow[f"{dep_name}_path"], **dep_kw):
                        return False  # any incomplete session → not ready

        # ── Self-completion check ─────────────────────────────────────────
        should_force = force and (force_procedures is None or proc.name in force_procedures)
        if should_force:
            return True
        self_kwargs = _completion_kwargs(proc, row, config)
        return not is_complete(proc, row[f"{proc.name}_path"], **self_kwargs)

    rule.__name__ = f"needs_{proc.name}"
    return rule

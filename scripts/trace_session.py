#!/usr/bin/env python3
"""Trace a single subject/session through the snbb_scheduler pipeline.

Shows, for each procedure:
  - Output path
  - Completion marker(s) being checked
  - Whether each marker resolves to a hit on disk
  - Final is_complete() verdict

Usage:
    python scripts/trace_session.py --subject sub-0001 --session ses-01
    python scripts/trace_session.py --subject sub-0001 --session ses-01 --config /path/to/config.yaml
"""

from __future__ import annotations

import argparse
from pathlib import Path

from snbb_scheduler.checks import check_detailed, is_complete, _SPECIALIZED_CHECKS
from snbb_scheduler.config import DEFAULT_PROCEDURES, SchedulerConfig
from snbb_scheduler.rules import _completion_kwargs


# ── ANSI colour helpers ──────────────────────────────────────────────────────

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

TICK = f"{GREEN}✓{RESET}"
CROSS = f"{RED}✗{RESET}"


def _fmt_bool(v: bool) -> str:
    return TICK if v else CROSS


# ── Path resolution ──────────────────────────────────────────────────────────


def _output_path(proc, config: SchedulerConfig, subject: str, session: str) -> Path:
    """Resolve the filesystem output path for a procedure."""
    root = config.get_procedure_root(proc)
    if proc.scope == "subject":
        return root / subject
    # session-scoped: bids/bids_post/defacing land in bids_root/subject/session
    # qsiprep / qsirecon land in derivatives_root/tool/subject/session
    if not proc.output_dir:
        return root / subject / session
    return root / subject / session


# ── Dicom check ──────────────────────────────────────────────────────────────


def _dicom_exists(config: SchedulerConfig, subject: str, session: str) -> tuple[bool, Path]:
    dicom_dir = config.dicom_root / subject / session
    return dicom_dir.exists(), dicom_dir


# ── Main trace ───────────────────────────────────────────────────────────────


def trace(subject: str, session: str, config: SchedulerConfig) -> None:
    sep = "─" * 70

    print(f"\n{BOLD}Pipeline trace{RESET}  subject={subject}  session={session}\n{sep}")

    # DICOM gate
    dicom_ok, dicom_path = _dicom_exists(config, subject, session)
    print(f"\n{BOLD}[DICOM]{RESET}  {dicom_path}")
    print(f"  exists : {_fmt_bool(dicom_ok)}")
    if not dicom_ok:
        print(f"\n{YELLOW}  No DICOM data found — all rules will return False.{RESET}")

    print()

    for proc in config.procedures:
        output_path = _output_path(proc, config, subject, session)
        kwargs = _completion_kwargs(proc, _make_row(subject, session, config), config)

        complete = is_complete(proc, output_path, **kwargs)
        details = check_detailed(proc, output_path, **kwargs)

        scope_tag = f"[{proc.scope}]" if proc.scope == "subject" else "[session]"
        marker_desc = _describe_marker(proc)
        specialized = " (specialized check)" if proc.name in _SPECIALIZED_CHECKS else ""

        print(f"{BOLD}{proc.name}{RESET} {DIM}{scope_tag}{RESET}")
        print(f"  output path : {output_path}")
        print(f"  path exists : {_fmt_bool(output_path.exists())}")
        print(f"  marker      : {DIM}{marker_desc}{specialized}{RESET}")
        if proc.depends_on:
            print(f"  depends on  : {', '.join(proc.depends_on)}")

        print(f"  checks:")
        for r in details:
            hit = TICK if r.found else CROSS
            print(f"    {hit}  {r.pattern}")
            for f in r.matched_files[:3]:
                print(f"         {DIM}{f}{RESET}")
            if len(r.matched_files) > 3:
                print(f"         {DIM}… +{len(r.matched_files) - 3} more{RESET}")

        verdict_colour = GREEN if complete else RED
        print(f"  is_complete : {verdict_colour}{BOLD}{complete}{RESET}")
        print()

    print(sep)


def _describe_marker(proc) -> str:
    m = proc.completion_marker
    if m is None:
        return "None  (non-empty directory  —or—  specialized check)"
    if isinstance(m, list):
        return f"list of {len(m)} patterns"
    return repr(m)


def _make_row(subject: str, session: str, config: SchedulerConfig):
    """Build a minimal pd.Series that _completion_kwargs can read."""
    import pandas as pd

    row_data = {"subject": subject, "session": session}
    for proc in config.procedures:
        output_path = _output_path(proc, config, subject, session)
        row_data[f"{proc.name}_path"] = output_path
    return pd.Series(row_data)


# ── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--subject", required=True, help="BIDS subject label, e.g. sub-0001")
    parser.add_argument("--session", required=True, help="BIDS session label, e.g. ses-01")
    parser.add_argument("--config", default=None, help="Path to snbb_config.yaml (optional)")
    args = parser.parse_args()

    if args.config:
        config = SchedulerConfig.from_yaml(args.config)
    else:
        config = SchedulerConfig()

    trace(args.subject, args.session, config)


if __name__ == "__main__":
    main()

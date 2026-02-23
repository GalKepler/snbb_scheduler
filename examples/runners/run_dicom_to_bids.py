#!/usr/bin/env python3
"""DICOM to BIDS conversion via HeudiConv.

Converts DICOM files to BIDS format using HeudiConv within the VoxelOps
framework. Reads a linked_sessions CSV, sanitizes participant/session IDs,
and runs conversions in parallel across sessions.

Usage
-----
    python dicom_to_bids.py \\
        --csv ~/Downloads/linked_sessions.csv \\
        --output-dir /media/storage/yalab-dev/qsiprep_test/BIDS \\
        --heuristic /path/to/heuristic.py \\
        --workers 4
"""

from __future__ import annotations

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import shutil
from typing import Union

import pandas as pd

from voxelops import HeudiconvDefaults, HeudiconvInputs, run_procedure
import json

from voxelops.procedures.orchestrator import _get_default_log_dir

logger = logging.getLogger(__name__)

DOCKER_IMAGE = ""

# ---------------------------------------------------------------------------
# Sanitizers
# ---------------------------------------------------------------------------


def sanitize_subject_code(subject_code: str) -> str:
    """Remove special characters and zero-pad to 4 digits."""
    return subject_code.replace("-", "").replace("_", "").replace(" ", "").zfill(4)


def sanitize_session_id(session_id: Union[str, int, float]) -> str:
    """Convert to string, clean, and zero-pad to 12 digits."""
    if isinstance(session_id, float):
        if pd.isna(session_id):
            return ""
        session_str = str(int(session_id))
    else:
        session_str = str(session_id)
    return session_str.replace("-", "").replace("_", "").replace(" ", "").zfill(12)


# ---------------------------------------------------------------------------
# Session loading
# ---------------------------------------------------------------------------


def load_sessions(csv_path: Union[str, Path]) -> pd.DataFrame:
    """Load and sanitize a linked_sessions CSV file.

    Expects columns ``SubjectCode``, ``ScanID``, and ``dicom_path``.
    Returns a deduplicated DataFrame with ``subject_code``, ``session_id``,
    and ``dicom_path`` columns. Rows with a missing ``dicom_path`` are dropped.

    Parameters
    ----------
    csv_path:
        Path to the linked_sessions.csv file.
    """
    df = pd.read_csv(csv_path)
    df["subject_code"] = df["SubjectCode"].apply(sanitize_subject_code)
    df["session_id"] = df["ScanID"].apply(sanitize_session_id)
    df = df.dropna(subset=["dicom_path"]).reset_index(drop=True)
    return df.drop_duplicates(subset=["subject_code", "session_id"]).reset_index(
        drop=True
    )


def _get_last_execution_log(
    inputs: HeudiconvInputs, log_dir: Path | None = None
) -> bool:
    """Return the path to the last execution log for a participant/session.

    Parameters
    ----------
    inputs : HeudiconvInputs
        The inputs for the heudiconv procedure.
    log_dir : Path
        Directory containing execution logs.
    log_dir : Path
        Directory containing execution logs.

    Returns
    -------
    Path | None
        Path to the last execution log, or None if not found.
    """
    if (log_dir is None) or (not log_dir.exists()):
        log_dir = _get_default_log_dir(inputs)

    prefix = f"heudiconv_sub-{inputs.participant}"
    if inputs.session is not None:
        prefix += f"_ses-{inputs.session}"

    log_files = list(log_dir.glob(f"{prefix}_*.json"))
    if not log_files:
        return False
    last_executed = sorted(log_files, key=lambda f: f.stat().st_mtime)[-1]

    # get "success" field from the log
    with open(last_executed) as f:
        log_data = json.load(f)
        if log_data.get("success"):
            return True
        else:
            return False


# ---------------------------------------------------------------------------
# Parallel runner
# ---------------------------------------------------------------------------


def run_parallel(
    sessions: pd.DataFrame,
    output_dir: Path,
    config: HeudiconvDefaults,
    max_workers: int = 4,
    log_dir: Path | None = None,
) -> list[dict]:
    """Convert all sessions in parallel using ThreadPoolExecutor.

    Each Docker invocation spawns a subprocess, so threads are appropriate
    here — the GIL is released while waiting for the child process.

    Parameters
    ----------
    sessions:
        DataFrame with ``subject_code``, ``session_id``, and ``dicom_path``.
    output_dir:
        BIDS output directory.
    config:
        Shared HeudiconvDefaults applied to every session.
    max_workers:
        Number of sessions to process concurrently.

    Returns
    -------
    list[dict]
        One result dict per session, with keys ``subject_code``,
        ``session_id``, ``success``, ``duration_human``, ``output``,
        and ``error``.
    """

    def _convert(row: pd.Series, log_dir: Path) -> dict:
        subject = row["subject_code"]
        session = row["session_id"]
        target_dir = Path(output_dir) / f"sub-{subject}" / f"ses-{session}"
        if target_dir.exists() and config.overwrite:
            logger.warning(
                "Output already exists for sub-%s ses-%s, but overwrite is enabled. Deleting existing output.",
                subject,
                session,
            )
            try:
                shutil.rmtree(target_dir)
                heudiconv_hidden_dir = (
                    Path(output_dir) / ".heudiconv" / subject / f"ses-{session}"
                )
                if heudiconv_hidden_dir.exists():
                    shutil.rmtree(heudiconv_hidden_dir)
            except Exception as exc:
                logger.error(
                    "Failed to delete existing output for sub-%s ses-%s: %s",
                    subject,
                    session,
                    exc,
                )
                return {
                    "subject_code": subject,
                    "session_id": session,
                    "success": False,
                    "duration_human": None,
                    "output": None,
                    "error": f"Failed to delete existing output: {exc}",
                }
        logger.info("Starting  sub-%s ses-%s", subject, session)
        inputs = HeudiconvInputs(
            dicom_dir=Path(row["dicom_path"]),
            participant=subject,
            session=session,
            output_dir=output_dir,
        )
        try:
            executed_successfully = _get_last_execution_log(inputs, log_dir)
            if executed_successfully:
                logger.info(
                    "Skipping sub-%s ses-%s (already executed successfully)",
                    subject,
                    session,
                )
                return {
                    "subject_code": subject,
                    "session_id": session,
                    "success": True,
                    "duration_human": None,
                    "output": None,
                    "error": "Skipped (already executed successfully)",
                }
            result = run_procedure(
                procedure="heudiconv", inputs=inputs, config=config, log_dir=log_dir
            )
            bids_dir = None
            if result.execution:
                outputs = result.execution.get("expected_outputs")
                bids_dir = str(getattr(outputs, "bids_dir", "") or "")
            return {
                "subject_code": subject,
                "session_id": session,
                "success": result.success,
                "duration_human": result.execution.get("duration_human")
                if result.execution
                else None,
                "output": bids_dir,
                "error": result.get_failure_reason(),
            }
        except Exception as exc:
            logger.exception("Conversion failed for sub-%s ses-%s", subject, session)
            return {
                "subject_code": subject,
                "session_id": session,
                "success": False,
                "duration_human": None,
                "output": None,
                "error": str(exc),
            }

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_convert, row, log_dir): (
                row["subject_code"],
                row["session_id"],
            )
            for _, row in sessions.iterrows()
        }
        for future in as_completed(futures):
            sub, ses = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                logger.error("Unexpected error for sub-%s ses-%s: %s", sub, ses, exc)
                result = {
                    "subject_code": sub,
                    "session_id": ses,
                    "success": False,
                    "duration_human": None,
                    "output": None,
                    "error": str(exc),
                }
            results.append(result)
            status = "OK" if result["success"] else "FAILED"
            logger.info(
                "[%s] sub-%s ses-%s  duration=%s  error=%s",
                status,
                result["subject_code"],
                result["session_id"],
                result.get("duration_human"),
                result.get("error"),
            )

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert DICOM files to BIDS format using HeudiConv.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--csv",
        required=True,
        type=Path,
        help="Path to linked_sessions.csv (columns: SubjectCode, ScanID, dicom_path)",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="BIDS output directory",
    )
    parser.add_argument(
        "--heuristic",
        required=True,
        type=Path,
        help="Path to HeudiConv heuristic.py",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of sessions to process concurrently (one Docker process each)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing BIDS outputs",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        help="Directory to save logs (one per session)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    parser.add_argument(
        "--participants",
        nargs="+",
        metavar="LABEL",
        help="Only convert these participants (raw codes without 'sub-' prefix). "
             "Default: all rows in the CSV.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    config = HeudiconvDefaults(
        heuristic=Path(args.heuristic).resolve(),
        overwrite=args.overwrite,
    )

    sessions = load_sessions(args.csv)
    logger.info("Loaded %d session(s) to process", len(sessions))

    if args.participants:
        sessions = sessions[sessions["subject_code"].isin(args.participants)].reset_index(drop=True)
        logger.info("Filtered to %d session(s) for participants: %s",
                    len(sessions), args.participants)

    results = run_parallel(
        sessions=sessions,
        output_dir=args.output_dir,
        config=config,
        max_workers=args.workers,
        log_dir=Path(args.log_dir).resolve() if args.log_dir else None,
    )

    n_ok = sum(r["success"] for r in results)
    n_fail = len(results) - n_ok
    logger.info("Done: %d succeeded, %d failed", n_ok, n_fail)

    if n_fail:
        logger.warning("Failed sessions:")
        for r in results:
            if not r["success"]:
                logger.warning(
                    "  sub-%s ses-%s: %s",
                    r["subject_code"],
                    r["session_id"],
                    r["error"],
                )


if __name__ == "__main__":
    main()

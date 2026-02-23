#!/usr/bin/env python3
"""QSIPrep diffusion MRI preprocessing CLI.

Runs QSIPrep on one or more participants within the VoxelOps framework.
Participants can be supplied directly or via a CSV file, and runs are
executed in parallel across participants.

Usage
-----
    python run_qsiprep.py \\
        --bids-dir /media/storage/yalab-dev/BIDS \\
        --output-dir /media/storage/yalab-dev/derivatives \\
        --participants 0001 0002 0003 \\
        --bids-filters /path/to/bids_filters.json \\
        --fs-license /path/to/license.txt \\
        --workers 4

    # Or supply participants via CSV (column: participant):
    python run_qsiprep.py \\
        --bids-dir /media/storage/yalab-dev/BIDS \\
        --output-dir /media/storage/yalab-dev/derivatives \\
        --csv participants.csv \\
        --bids-filters /path/to/bids_filters.json
"""

from __future__ import annotations

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from voxelops import QSIPrepDefaults, QSIPrepInputs, run_procedure
from voxelops.runners._base import _get_default_log_dir

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Participant loading
# ---------------------------------------------------------------------------

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


def load_participants_from_csv(
    csv_path: Path, output_dir: Path, force: bool
) -> list[str]:
    """Load participant labels from a CSV file.

    Expects a ``participant`` column (without 'sub-' prefix).

    Parameters
    ----------
    csv_path : Path
        Path to CSV file with a ``participant`` column.

    Returns
    -------
    list[str]
        Deduplicated list of participant labels.
    """
    df = load_sessions(csv_path)

    # if force, return all participants in the csv
    if force:
        return sorted(df["subject_code"].unique())
    # collect all subjects that either don't exist in the output directory or that have more session in the csv than in the output directory
    existing_subjects = set()
    for subject in df["subject_code"].unique():
        subject_dir = output_dir / f"sub-{subject}"
        # if it doesnt exist, or was created over 24 hours ago
        if not subject_dir.exists() or (
            subject_dir.stat().st_mtime < (pd.Timestamp.now().timestamp() - 24 * 3600)
        ):
            existing_subjects.add(subject)
        else:
            for session in set(
                df[df["subject_code"] == subject]["session_id"].unique()
            ):
                session_dir = subject_dir / f"ses-{session}"
                if not session_dir.exists():
                    existing_subjects.add(subject)
                    break
    return sorted(existing_subjects)


# ---------------------------------------------------------------------------
# Last-execution check
# ---------------------------------------------------------------------------


def _get_last_execution_log(inputs: QSIPrepInputs, log_dir: Path | None = None) -> bool:
    """Return True if the last execution log shows success.

    Parameters
    ----------
    inputs : QSIPrepInputs
        The inputs for the qsiprep procedure.
    log_dir : Path, optional
        Directory containing execution logs.

    Returns
    -------
    bool
        True if the last log indicates a successful run, False otherwise.
    """
    if log_dir is None or not log_dir.exists():
        log_dir = _get_default_log_dir(inputs)

    prefix = f"qsiprep_sub-{inputs.participant}"
    log_files = list(log_dir.glob(f"{prefix}_*.json"))
    if not log_files:
        return False

    last_executed = sorted(log_files, key=lambda f: f.stat().st_mtime)[-1]
    with open(last_executed) as f:
        log_data = json.load(f)
        return bool(log_data.get("success"))


# ---------------------------------------------------------------------------
# Parallel runner
# ---------------------------------------------------------------------------


def run_parallel(
    participants: list[str],
    bids_dir: Path,
    output_dir: Path,
    config: QSIPrepDefaults,
    bids_filters: Path | None = None,
    work_dir: Path | None = None,
    max_workers: int = 4,
    log_dir: Path | None = None,
) -> list[dict]:
    """Run QSIPrep for all participants in parallel using ThreadPoolExecutor.

    Each Docker invocation spawns a subprocess, so threads are appropriate
    here — the GIL is released while waiting for the child process.

    Parameters
    ----------
    participants : list[str]
        Participant labels (without 'sub-' prefix).
    bids_dir : Path
        BIDS dataset directory.
    output_dir : Path
        Derivatives output directory.
    config : QSIPrepDefaults
        Shared QSIPrepDefaults applied to every participant.
    bids_filters : Path, optional
        Path to BIDS filters JSON file.
    work_dir : Path, optional
        Working directory for QSIPrep intermediate files.
    max_workers : int
        Number of participants to process concurrently.
    log_dir : Path, optional
        Directory to write execution logs.

    Returns
    -------
    list[dict]
        One result dict per participant, with keys ``participant``,
        ``success``, ``duration_human``, ``output``, and ``error``.
    """

    def _preprocess(participant: str) -> dict:
        logger.info("Starting sub-%s", participant)
        inputs = QSIPrepInputs(
            bids_dir=bids_dir,
            participant=participant,
            output_dir=output_dir,
            work_dir=work_dir,
            bids_filters=bids_filters,
        )
        try:
            if _get_last_execution_log(inputs, log_dir):
                logger.info(
                    "Skipping sub-%s (already executed successfully)", participant
                )
                return {
                    "participant": participant,
                    "success": True,
                    "duration_human": None,
                    "output": None,
                    "error": "Skipped (already executed successfully)",
                }
            result = run_procedure(
                procedure="qsiprep",
                inputs=inputs,
                config=config,
                log_dir=log_dir,
            )
            qsiprep_dir = None
            if result.execution:
                outputs = result.execution.get("expected_outputs")
                qsiprep_dir = str(getattr(outputs, "qsiprep_dir", "") or "")
            return {
                "participant": participant,
                "success": result.success,
                "duration_human": result.execution.get("duration_human")
                if result.execution
                else None,
                "output": qsiprep_dir,
                "error": result.get_failure_reason(),
            }
        except Exception as exc:
            logger.exception("Preprocessing failed for sub-%s", participant)
            return {
                "participant": participant,
                "success": False,
                "duration_human": None,
                "output": None,
                "error": str(exc),
            }

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_preprocess, p): p for p in participants}
        for future in as_completed(futures):
            participant = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                logger.error("Unexpected error for sub-%s: %s", participant, exc)
                result = {
                    "participant": participant,
                    "success": False,
                    "duration_human": None,
                    "output": None,
                    "error": str(exc),
                }
            results.append(result)
            status = "OK" if result["success"] else "FAILED"
            logger.info(
                "[%s] sub-%s  duration=%s  error=%s",
                status,
                result["participant"],
                result.get("duration_human"),
                result.get("error"),
            )

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run QSIPrep diffusion MRI preprocessing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Input
    parser.add_argument(
        "--bids-dir",
        required=True,
        type=Path,
        help="BIDS dataset directory",
    )

    # Participant selection (mutually exclusive)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--participants",
        nargs="+",
        metavar="LABEL",
        help="One or more participant labels (without 'sub-' prefix)",
    )
    group.add_argument(
        "--csv",
        type=Path,
        metavar="CSV",
        help="CSV file with a 'participant' column",
    )

    # Output
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Derivatives output directory",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Working directory for QSIPrep intermediates",
    )

    # QSIPrep options
    parser.add_argument(
        "--bids-filters",
        type=Path,
        help="Path to BIDS filters JSON file",
    )
    parser.add_argument(
        "--fs-license",
        type=Path,
        help="Path to FreeSurfer license file",
    )
    parser.add_argument(
        "--output-resolution",
        type=float,
        default=1.6,
        help="Output resolution in mm",
    )
    parser.add_argument(
        "--nprocs",
        type=int,
        default=8,
        help="Number of parallel processes per QSIPrep run",
    )
    parser.add_argument(
        "--mem-mb",
        type=int,
        default=16000,
        help="Memory limit in MB per QSIPrep run",
    )
    parser.add_argument(
        "--skip-bids-validation",
        action="store_true",
        help="Skip BIDS validation inside QSIPrep",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run even if outputs already exist",
    )
    parser.add_argument(
        "--docker-image",
        default="pennlinc/qsiprep:latest",
        help="QSIPrep Docker image",
    )
    # Parallelism / logging
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of participants to process concurrently",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        help="Directory to save logs (one per participant)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )

    return parser


def main() -> None:
    args = _build_parser().parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Resolve participants
    if args.csv:
        participants = load_participants_from_csv(args.csv, args.output_dir, args.force)
    else:
        participants = args.participants

    logger.info("Loaded %d participant(s) to process", len(participants))

    config = QSIPrepDefaults(
        nprocs=args.nprocs,
        mem_mb=args.mem_mb,
        output_resolution=args.output_resolution,
        skip_bids_validation=args.skip_bids_validation,
        fs_license=args.fs_license,
        docker_image=args.docker_image,
        force=args.force,
    )

    results = run_parallel(
        participants=participants,
        bids_dir=Path(args.bids_dir).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        config=config,
        bids_filters=Path(args.bids_filters).resolve() if args.bids_filters else None,
        work_dir=Path(args.work_dir).resolve() if args.work_dir else None,
        max_workers=args.workers,
        log_dir=Path(args.log_dir).resolve() if args.log_dir else None,
    )

    n_ok = sum(r["success"] for r in results)
    n_fail = len(results) - n_ok
    logger.info("Done: %d succeeded, %d failed", n_ok, n_fail)

    if n_fail:
        logger.warning("Failed participants:")
        for r in results:
            if not r["success"]:
                logger.warning(
                    "  sub-%s: %s",
                    r["participant"],
                    r["error"],
                )


if __name__ == "__main__":
    main()

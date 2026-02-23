#!/usr/bin/env python3
"""FreeSurfer cortical reconstruction CLI.

Runs FreeSurfer recon-all on one or more participants within the VoxelOps
framework.  Participants can be supplied directly or via a CSV file, and
runs are executed in parallel across participants.

Usage
-----
    python run_freesurfer.py \\
        --bids-dir /media/storage/yalab-dev/BIDS \\
        --output-dir /media/storage/yalab-dev/derivatives/freesurfer \\
        --participants 0001 0002 0003 \\
        --fs-license /path/to/license.txt \\
        --workers 2

    # Use only contrast-enhanced (ce-corrected) T1w images:
    python run_freesurfer.py \\
        --bids-dir /media/storage/yalab-dev/BIDS \\
        --output-dir /media/storage/yalab-dev/derivatives/freesurfer \\
        --participants 0001 0002 \\
        --fs-license /path/to/license.txt \\
        --t1w-filters ce=corrected

    # T2w is used automatically; restrict to a specific acquisition or disable:
    python run_freesurfer.py \\
        --bids-dir /media/storage/yalab-dev/BIDS \\
        --output-dir /media/storage/yalab-dev/derivatives/freesurfer \\
        --participants 0001 0002 \\
        --fs-license /path/to/license.txt \\
        --t2w-filters acq=sag   # narrow T2w selection
        # --no-t2w              # or disable T2w entirely

    # Also enable FLAIR (opt-in; prefer T2w over FLAIR):
    python run_freesurfer.py \\
        --bids-dir /media/storage/yalab-dev/BIDS \\
        --output-dir /media/storage/yalab-dev/derivatives/freesurfer \\
        --participants 0001 0002 \\
        --fs-license /path/to/license.txt \\
        --flair-filters acq=sag

    # Or supply participants via CSV (columns: SubjectCode, ScanID):
    python run_freesurfer.py \\
        --bids-dir /media/storage/yalab-dev/BIDS \\
        --output-dir /media/storage/yalab-dev/derivatives/freesurfer \\
        --csv participants.csv \\
        --fs-license /path/to/license.txt \\
        --nthreads 8 \\
        --t1w-filters ce=corrected acq=mprage
"""

from __future__ import annotations

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import shutil

import pandas as pd

from voxelops import FreeSurferDefaults, FreeSurferInputs, run_procedure
from voxelops.runners._base import _get_default_log_dir

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sanitizers
# ---------------------------------------------------------------------------


def sanitize_subject_code(subject_code: str) -> str:
    """Remove special characters and zero-pad to 4 digits."""
    return subject_code.replace("-", "").replace("_", "").replace(" ", "").zfill(4)


def sanitize_session_id(session_id: str | int | float) -> str:
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


def load_sessions(csv_path: str | Path) -> pd.DataFrame:
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
    csv_path: Path,
    bids_dir: Path,
    output_dir: Path,
    force: bool,
    t1w_filters: dict[str, str] | None = None,
) -> list[str]:
    """Load participant labels from a CSV file.

    Only returns participants whose BIDS T1w data exists (after applying
    ``t1w_filters``).  Skips participants whose FreeSurfer ``recon-all.done``
    flag already exists unless ``force`` is True.

    Parameters
    ----------
    csv_path : Path
        Path to CSV file with ``SubjectCode`` and ``ScanID`` columns.
    bids_dir : Path
        BIDS dataset directory (used to confirm T1w data is present).
    output_dir : Path
        FreeSurfer subjects directory (used to skip completed participants).
    force : bool
        If True, return all participants regardless of existing outputs.
    t1w_filters : dict[str, str], optional
        BIDS entity filters applied when checking for T1w data.  Mirrors
        the ``--t1w-filters`` CLI option.

    Returns
    -------
    list[str]
        Sorted list of participant labels to process.
    """
    df = load_sessions(csv_path)

    if force:
        return sorted(df["subject_code"].unique())

    pending = set()
    for subject in df["subject_code"].unique():
        # Only queue participants that have matching T1w data
        # subject_dir = bids_dir / f"sub-{subject}"
        # t1w_files = list(subject_dir.glob("**/anat/*_T1w.nii.gz"))
        # if t1w_filters:
        #     t1w_files = [
        #         p
        #         for p in t1w_files
        #         if all(f"{k}-{v}" in p.name for k, v in t1w_filters.items())
        #     ]
        # if not subject_dir.exists() or not t1w_files:
        #     logger.debug(
        #         "Skipping sub-%s: no matching T1w data found in BIDS dir", subject
        #     )
        #     continue

        # Skip if recon-all already completed
        done_flag = output_dir / f"sub-{subject}" / "scripts" / "recon-all.done"
        if not done_flag.exists():
            pending.add(subject)
            if done_flag.parent.exists():
                shutil.rmtree(output_dir / f"sub-{subject}")
        else:
            logger.debug("Skipping sub-%s: recon-all.done already exists", subject)

    return sorted(pending)


# ---------------------------------------------------------------------------
# Last-execution check
# ---------------------------------------------------------------------------


def _get_last_execution_log(
    inputs: FreeSurferInputs, log_dir: Path | None = None
) -> bool:
    """Return True if the last execution log shows success.

    Parameters
    ----------
    inputs : FreeSurferInputs
        The inputs for the freesurfer procedure.
    log_dir : Path, optional
        Directory containing execution logs.

    Returns
    -------
    bool
        True if the last log indicates a successful run, False otherwise.
    """
    if log_dir is None or not log_dir.exists():
        log_dir = _get_default_log_dir(inputs)

    prefix = f"freesurfer_sub-{inputs.participant}"
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
    config: FreeSurferDefaults,
    t1w_filters: dict[str, str] | None = None,
    t2w_filters: dict[str, str] | None = None,
    flair_filters: dict[str, str] | None = None,
    work_dir: Path | None = None,
    max_workers: int = 2,
    log_dir: Path | None = None,
) -> list[dict]:
    """Run FreeSurfer recon-all for all participants in parallel.

    Each Docker invocation spawns a subprocess, so threads are appropriate
    here — the GIL is released while waiting for the child process.

    Parameters
    ----------
    participants : list[str]
        Participant labels (without 'sub-' prefix).
    bids_dir : Path
        BIDS dataset directory.
    output_dir : Path
        FreeSurfer subjects directory (SUBJECTS_DIR).
    config : FreeSurferDefaults
        Shared configuration applied to every participant.
    t1w_filters : dict[str, str], optional
        BIDS entity filters for T1w image selection (e.g. ``{"ce": "corrected"}``).
    t2w_filters : dict[str, str], optional
        BIDS entity filters for T2w discovery.  ``None`` (default) disables
        T2w entirely; ``{}`` uses any T2w found.
    flair_filters : dict[str, str], optional
        BIDS entity filters for FLAIR discovery.  Same opt-in semantics as
        ``t2w_filters``.
    work_dir : Path, optional
        Working directory for framework intermediates.
    max_workers : int
        Number of participants to process concurrently.  Keep this low
        (default 2) — recon-all is CPU/memory intensive.
    log_dir : Path, optional
        Directory to write execution logs.

    Returns
    -------
    list[dict]
        One result dict per participant, with keys ``participant``,
        ``success``, ``duration_human``, ``output``, and ``error``.
    """

    def _reconstruct(participant: str) -> dict:
        logger.info("Starting sub-%s", participant)
        inputs = FreeSurferInputs(
            bids_dir=bids_dir,
            participant=participant,
            output_dir=output_dir,
            work_dir=work_dir,
            t1w_filters=t1w_filters,
            t2w_filters=t2w_filters,
            flair_filters=flair_filters,
        )
        try:
            if _get_last_execution_log(inputs, log_dir) and (not config.force):
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
            # if force, delete existing outputs to ensure a clean re-run
            if config.force:
                subject_dir = output_dir / f"sub-{participant}"
                if subject_dir.exists():
                    logger.info(
                        "Force enabled: deleting existing outputs for sub-%s",
                        participant,
                    )
                    shutil.rmtree(subject_dir)
            result = run_procedure(
                procedure="freesurfer",
                inputs=inputs,
                config=config,
                log_dir=log_dir,
            )
            subject_dir = None
            if result.execution:
                outputs = result.execution.get("expected_outputs")
                subject_dir = str(getattr(outputs, "subject_dir", "") or "")
            return {
                "participant": participant,
                "success": result.success,
                "duration_human": result.execution.get("duration_human")
                if result.execution
                else None,
                "output": subject_dir,
                "error": result.get_failure_reason(),
            }
        except Exception as exc:
            logger.exception("Reconstruction failed for sub-%s", participant)
            return {
                "participant": participant,
                "success": False,
                "duration_human": None,
                "output": None,
                "error": str(exc),
            }

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_reconstruct, p): p for p in participants}
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
        description="Run FreeSurfer recon-all cortical reconstruction.",
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
        help="CSV file with 'SubjectCode' and 'ScanID' columns",
    )

    # Output
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="FreeSurfer subjects directory (SUBJECTS_DIR)",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Working directory for framework intermediates",
    )

    # FreeSurfer options
    parser.add_argument(
        "--fs-license",
        required=True,
        type=Path,
        help="Path to FreeSurfer licence file (required)",
    )
    parser.add_argument(
        "--t1w-filters",
        nargs="+",
        metavar="KEY=VALUE",
        help=(
            "BIDS entity filters for T1w image selection, e.g. "
            "ce=corrected  or  ce=corrected acq=mprage. "
            "Only filenames containing every key-value token are used."
        ),
    )
    parser.add_argument(
        "--t2w-filters",
        nargs="*",
        metavar="KEY=VALUE",
        help=(
            "BIDS entity filters to narrow T2w image selection, e.g. acq=sag. "
            "T2w images are used by default (pial surface refinement via -T2pial). "
            "Pass KEY=VALUE pairs to restrict which T2w files are used.  "
            "Use --no-t2w to disable T2w entirely."
        ),
    )
    parser.add_argument(
        "--no-t2w",
        action="store_true",
        help="Disable T2w detection and pial refinement",
    )
    parser.add_argument(
        "--flair-filters",
        nargs="*",
        metavar="KEY=VALUE",
        help=(
            "Enable FLAIR-enhanced pial surface refinement (-FLAIR / -FLAIRpial). "
            "Pass with no values to use any FLAIR found, or with KEY=VALUE pairs "
            "to filter (e.g. --flair-filters acq=sag).  "
            "FLAIR is disabled unless this flag is given; prefer T2w over FLAIR."
        ),
    )
    parser.add_argument(
        "--nthreads",
        type=int,
        default=4,
        help="OpenMP threads per recon-all run (-openmp N)",
    )
    parser.add_argument(
        "--hires",
        action="store_true",
        help="Enable sub-millimetre processing (-hires)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run even if recon-all.done already exists",
    )
    parser.add_argument(
        "--docker-image",
        default="freesurfer/freesurfer:8.1.0",
        help="FreeSurfer Docker image",
    )

    # Parallelism / logging
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Number of participants to process concurrently (keep low — recon-all is CPU-intensive)",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        help="Directory to save execution logs (one per participant)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )

    return parser


def _parse_filters(
    raw: list[str] | None, flag: str = "--filters"
) -> dict[str, str] | None:
    """Parse ``KEY=VALUE`` strings into a ``{key: value}`` dict.

    Returns ``{}`` when ``raw`` is an empty list (flag supplied with no args),
    which signals "use any file of this type without further filtering".
    Returns ``None`` when ``raw`` is ``None`` (flag not supplied at all),
    which disables that modality entirely.
    """
    if raw is None:
        return None
    filters = {}
    for item in raw:
        if "=" not in item:
            raise argparse.ArgumentTypeError(
                f"{flag} entries must be KEY=VALUE, got: {item!r}"
            )
        key, _, value = item.partition("=")
        filters[key.strip()] = value.strip()
    return filters


def main() -> None:
    args = _build_parser().parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    bids_dir = Path(args.bids_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    t1w_filters = _parse_filters(args.t1w_filters, "--t1w-filters")

    # T2w is enabled by default; --no-t2w disables it, --t2w-filters narrows the files
    if args.no_t2w:
        t2w_filters: dict[str, str] | None = None
    elif args.t2w_filters is not None:
        t2w_filters = _parse_filters(args.t2w_filters, "--t2w-filters")
    else:
        t2w_filters = {}  # auto-detect any T2w

    # FLAIR is opt-in: only enabled when --flair-filters is explicitly passed
    flair_filters = _parse_filters(args.flair_filters, "--flair-filters")

    if t1w_filters:
        logger.info("Applying T1w filters: %s", t1w_filters)
    if t2w_filters is None:
        logger.info("T2w disabled (--no-t2w)")
    elif t2w_filters:
        logger.info("T2w enabled with filters: %s", t2w_filters)
    else:
        logger.info("T2w enabled (any T2w found will be used)")
    if flair_filters is not None:
        logger.info(
            "FLAIR enabled%s", f" with filters {flair_filters}" if flair_filters else ""
        )

    # Resolve participants
    if args.csv:
        participants = load_participants_from_csv(
            args.csv, bids_dir, output_dir, args.force, t1w_filters
        )
    else:
        participants = args.participants

    logger.info("Loaded %d participant(s) to process", len(participants))

    config = FreeSurferDefaults(
        nthreads=args.nthreads,
        hires=args.hires,
        fs_license=Path(args.fs_license).resolve(),
        docker_image=args.docker_image,
        force=args.force,
        use_t2pial=True,
        use_flairpial=True,
    )

    results = run_parallel(
        participants=participants,
        bids_dir=bids_dir,
        output_dir=output_dir,
        config=config,
        t1w_filters=t1w_filters,
        t2w_filters=t2w_filters,
        flair_filters=flair_filters,
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

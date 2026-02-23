#!/usr/bin/env python3
"""QSIRecon diffusion reconstruction and connectivity CLI.

Runs QSIRecon on one or more participants (optionally scoped to a specific
session) within the VoxelOps framework.  Runs are executed in parallel across
participant/session pairs using a thread pool.

Usage
-----
Single participant, specific session::

    python run_qsirecon.py \\
        --qsiprep-dir /media/storage/yalab-dev/derivatives/qsiprep \\
        --output-dir  /media/storage/yalab-dev/derivatives/qsirecon \\
        --participants CLMC10 \\
        --session 202407110849 \\
        --recon-spec /path/to/mrtrix_tractography.yaml \\
        --recon-spec-aux-files /media/storage/yalab-dev/derivatives/responses \\
        --datasets atlases=/media/storage/yalab-dev/voxelops/Schaefer2018Tian2020_atlases \\
        --atlases 4S156Parcels Schaefer2018N100n7Tian2020S1 \\
        --fs-license /path/to/license.txt \\
        --fs-subjects-dir /media/storage/yalab-dev/derivatives/freesurfer \\
        --nprocs 20 --mem-mb 32000

Multiple participants, all sessions (no --session)::

    python run_qsirecon.py \\
        --qsiprep-dir /media/storage/yalab-dev/derivatives/qsiprep \\
        --output-dir  /media/storage/yalab-dev/derivatives/qsirecon \\
        --participants 0001 0002 0003 \\
        --recon-spec /path/to/mrtrix_tractography.yaml \\
        --workers 4

Via CSV (SubjectCode + ScanID → per subject/session pairs)::

    python run_qsirecon.py \\
        --qsiprep-dir /media/storage/yalab-dev/derivatives/qsiprep \\
        --output-dir  /media/storage/yalab-dev/derivatives/qsirecon \\
        --csv participants.csv \\
        --recon-spec /path/to/mrtrix_tractography.yaml
"""

from __future__ import annotations

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Union

import pandas as pd

from voxelops import QSIReconDefaults, QSIReconInputs, run_procedure
from voxelops.runners._base import _get_default_log_dir

logger = logging.getLogger(__name__)


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


def load_pairs_from_csv(
    csv_path: Path, qsiprep_dir: Path, output_dir: Path, force: bool
) -> list[tuple[str, str]]:
    """Load (participant, session) pairs from a CSV file.

    Only returns pairs whose QSIPrep subject directory exists.  Pairs whose
    QSIRecon session directory already exists are skipped unless ``force``
    is True.

    Parameters
    ----------
    csv_path : Path
        Path to CSV file with ``SubjectCode`` and ``ScanID`` columns.
    qsiprep_dir : Path
        QSIPrep derivatives directory (used to confirm preprocessing is done).
    output_dir : Path
        QSIRecon output directory (used to skip already-processed pairs).
    force : bool
        If True, return all pairs regardless of existing outputs.

    Returns
    -------
    list[tuple[str, str]]
        Sorted list of ``(participant, session)`` pairs to process.
    """
    df = load_sessions(csv_path)
    pending: list[tuple[str, str]] = []

    for _, row in df.iterrows():
        subject = row["subject_code"]
        session = row["session_id"]

        if not (qsiprep_dir / f"sub-{subject}").exists():
            logger.debug("Skipping sub-%s: no QSIPrep output found", subject)
            continue

        if force:
            pending.append((subject, session))
            continue

        session_dir = output_dir / f"sub-{subject}" / f"ses-{session}"
        if not session_dir.exists():
            pending.append((subject, session))
        else:
            logger.debug(
                "Skipping sub-%s ses-%s: QSIRecon output already exists",
                subject,
                session,
            )

    return sorted(pending)


# ---------------------------------------------------------------------------
# Last-execution check
# ---------------------------------------------------------------------------


def _get_last_execution_log(
    inputs: QSIReconInputs, log_dir: Path | None = None
) -> bool:
    """Return True if the last execution log for this subject/session shows success.

    Parameters
    ----------
    inputs : QSIReconInputs
        The inputs for the qsirecon procedure.
    log_dir : Path, optional
        Directory containing execution logs.

    Returns
    -------
    bool
        True if the last log indicates a successful run, False otherwise.
    """
    if log_dir is None or not log_dir.exists():
        log_dir = _get_default_log_dir(inputs)

    session_part = f"_ses-{inputs.session}" if inputs.session else ""
    prefix = f"qsirecon_sub-{inputs.participant}{session_part}"
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
    pairs: list[tuple[str, str | None]],
    qsiprep_dir: Path,
    output_dir: Path,
    config: QSIReconDefaults,
    recon_spec: Path | None = None,
    recon_spec_aux_files: Path | None = None,
    datasets: dict[str, Path] | None = None,
    atlases: list[str] | None = None,
    work_dir: Path | None = None,
    max_workers: int = 4,
    log_dir: Path | None = None,
) -> list[dict]:
    """Run QSIRecon for all (participant, session) pairs in parallel.

    Each Docker invocation spawns a subprocess, so threads are appropriate
    here — the GIL is released while waiting for the child process.

    Parameters
    ----------
    pairs : list[tuple[str, str | None]]
        ``(participant, session)`` pairs to process.  Set session to
        ``None`` to omit ``--session-id`` (processes all sessions).
    qsiprep_dir : Path
        QSIPrep derivatives directory.
    output_dir : Path
        QSIRecon output directory.
    config : QSIReconDefaults
        Shared configuration applied to every run.  Contains Docker image,
        resource limits, FreeSurfer license, and subjects directory.
    recon_spec : Path, optional
        Path to the reconstruction spec YAML file.
    recon_spec_aux_files : Path, optional
        Directory with auxiliary files referenced by the recon spec (e.g.
        MRtrix3 response functions).  Mounted into the container using the
        directory's own basename (e.g. ``responses/`` → ``/responses``).
    datasets : dict[str, Path], optional
        Mapping of dataset names to local paths (e.g. atlases).
    atlases : list[str], optional
        Atlas names to pass via ``--atlases``.  Falls back to
        ``QSIReconInputs`` defaults when None.
    work_dir : Path, optional
        Working directory for QSIRecon intermediate files.
    max_workers : int
        Number of pairs to process concurrently.
    log_dir : Path, optional
        Directory to write per-run JSON execution logs.

    Returns
    -------
    list[dict]
        One result dict per pair, with keys ``participant``, ``session``,
        ``success``, ``duration_human``, ``output``, and ``error``.
    """

    def _reconstruct(participant: str, session: str | None) -> dict:
        label = f"sub-{participant}" + (f" ses-{session}" if session else "")
        logger.info("Starting %s", label)

        inputs_kwargs: dict = dict(
            qsiprep_dir=qsiprep_dir,
            participant=participant,
            session=session,
            output_dir=output_dir,
            work_dir=work_dir,
            recon_spec=recon_spec,
            recon_spec_aux_files=recon_spec_aux_files,
            datasets=datasets,
        )
        if atlases is not None:
            inputs_kwargs["atlases"] = atlases

        inputs = QSIReconInputs(**inputs_kwargs)
        try:
            if _get_last_execution_log(inputs, log_dir):
                logger.info("Skipping %s (already executed successfully)", label)
                return {
                    "participant": participant,
                    "session": session,
                    "success": True,
                    "duration_human": None,
                    "output": None,
                    "error": "Skipped (already executed successfully)",
                }
            result = run_procedure(
                procedure="qsirecon",
                inputs=inputs,
                config=config,
                log_dir=log_dir,
            )
            qsirecon_dir = None
            if result.execution:
                outputs = result.execution.get("expected_outputs")
                qsirecon_dir = str(getattr(outputs, "qsirecon_dir", "") or "")
            return {
                "participant": participant,
                "session": session,
                "success": result.success,
                "duration_human": result.execution.get("duration_human")
                if result.execution
                else None,
                "output": qsirecon_dir,
                "error": result.get_failure_reason(),
            }
        except Exception as exc:
            logger.exception("Reconstruction failed for %s", label)
            return {
                "participant": participant,
                "session": session,
                "success": False,
                "duration_human": None,
                "output": None,
                "error": str(exc),
            }

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_reconstruct, participant, session): (participant, session)
            for participant, session in pairs
        }
        for future in as_completed(futures):
            participant, session = futures[future]
            label = f"sub-{participant}" + (f" ses-{session}" if session else "")
            try:
                result = future.result()
            except Exception as exc:
                logger.error("Unexpected error for %s: %s", label, exc)
                result = {
                    "participant": participant,
                    "session": session,
                    "success": False,
                    "duration_human": None,
                    "output": None,
                    "error": str(exc),
                }
            results.append(result)
            status = "OK" if result["success"] else "FAILED"
            logger.info(
                "[%s] %s  duration=%s  error=%s",
                status,
                label,
                result.get("duration_human"),
                result.get("error"),
            )

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run QSIRecon diffusion reconstruction and connectivity.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Input
    parser.add_argument(
        "--qsiprep-dir",
        required=True,
        type=Path,
        help="QSIPrep derivatives directory",
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
        help=(
            "CSV file with 'SubjectCode' and 'ScanID' columns. "
            "Each row becomes one (participant, session) run."
        ),
    )

    # Session (only meaningful with --participants)
    parser.add_argument(
        "--session",
        metavar="SESSION_ID",
        default=None,
        help=(
            "Session label (without 'ses-' prefix) to process. "
            "Passed as --session-id to qsirecon. "
            "When omitted all sessions in the QSIPrep output are processed "
            "together in one invocation. "
            "Ignored when --csv is used (session comes from the CSV)."
        ),
    )

    # Output
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="QSIRecon output directory",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Working directory for QSIRecon intermediates",
    )

    # QSIRecon options
    parser.add_argument(
        "--recon-spec",
        type=Path,
        help="Path to reconstruction spec YAML file",
    )
    parser.add_argument(
        "--recon-spec-aux-files",
        type=Path,
        metavar="DIR",
        help=(
            "Directory containing auxiliary files referenced by the recon spec "
            "(e.g. MRtrix3 response functions). "
            "Mounted into the container at /<dirname> "
            "(e.g. .../responses → /responses)."
        ),
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        metavar="NAME=PATH",
        help="Extra datasets to mount, e.g. atlases=/path/to/atlases",
    )
    parser.add_argument(
        "--atlases",
        nargs="+",
        metavar="ATLAS",
        help="Atlas names to pass via --atlases (overrides QSIReconInputs defaults)",
    )
    parser.add_argument(
        "--fs-license",
        type=Path,
        help="Path to FreeSurfer license file",
    )
    parser.add_argument(
        "--fs-subjects-dir",
        type=Path,
        metavar="DIR",
        help=(
            "FreeSurfer subjects directory. "
            "Mounted at /fs_subjects_dir and passed as --fs-subjects-dir."
        ),
    )
    parser.add_argument(
        "--nprocs",
        type=int,
        default=8,
        help="Number of parallel processes per QSIRecon run",
    )
    parser.add_argument(
        "--mem-mb",
        type=int,
        default=16000,
        help="Memory limit in MB per QSIRecon run",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run even if outputs already exist",
    )
    parser.add_argument(
        "--docker-image",
        default="pennlinc/qsirecon:1.2.0",
        help="QSIRecon Docker image",
    )

    # Parallelism / logging
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of participant/session pairs to process concurrently",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        help="Directory to save logs (one JSON per run)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )

    return parser


def _parse_datasets(raw: list[str] | None) -> dict[str, Path] | None:
    """Parse ``NAME=PATH`` strings into a ``{name: Path}`` dict."""
    if not raw:
        return None
    datasets = {}
    for item in raw:
        if "=" not in item:
            raise argparse.ArgumentTypeError(
                f"--datasets entries must be NAME=PATH, got: {item!r}"
            )
        name, _, path = item.partition("=")
        datasets[name.strip()] = Path(path.strip())
    return datasets


def main() -> None:
    args = _build_parser().parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    qsiprep_dir = Path(args.qsiprep_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    # Build (participant, session) pairs
    if args.csv:
        pairs = load_pairs_from_csv(args.csv, qsiprep_dir, output_dir, args.force)
    else:
        # Single session for all listed participants (None = no --session-id)
        pairs = [(p, args.session) for p in args.participants]

    logger.info("Loaded %d pair(s) to process", len(pairs))

    datasets = _parse_datasets(args.datasets)

    config = QSIReconDefaults(
        nprocs=args.nprocs,
        mem_mb=args.mem_mb,
        fs_license=args.fs_license,
        fs_subjects_dir=args.fs_subjects_dir,
        docker_image=args.docker_image,
        force=args.force,
    )

    results = run_parallel(
        pairs=pairs,
        qsiprep_dir=qsiprep_dir,
        output_dir=output_dir,
        config=config,
        recon_spec=Path(args.recon_spec).resolve() if args.recon_spec else None,
        recon_spec_aux_files=(
            Path(args.recon_spec_aux_files).resolve()
            if args.recon_spec_aux_files
            else None
        ),
        datasets={k: v.resolve() for k, v in datasets.items()} if datasets else None,
        atlases=args.atlases,
        work_dir=Path(args.work_dir).resolve() if args.work_dir else None,
        max_workers=args.workers,
        log_dir=Path(args.log_dir).resolve() if args.log_dir else None,
    )

    n_ok = sum(r["success"] for r in results)
    n_fail = len(results) - n_ok
    logger.info("Done: %d succeeded, %d failed", n_ok, n_fail)

    if n_fail:
        logger.warning("Failed runs:")
        for r in results:
            if not r["success"]:
                session_label = f" ses-{r['session']}" if r["session"] else ""
                logger.warning(
                    "  sub-%s%s: %s",
                    r["participant"],
                    session_label,
                    r["error"],
                )


if __name__ == "__main__":
    main()

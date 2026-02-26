from __future__ import annotations

"""snbb_scheduler.freesurfer — T1w/T2w collection and FreeSurfer command building.

This module is the single source of truth for which images are selected for a
FreeSurfer ``recon-all`` run.  Both the job-submission script
(``scripts/snbb_recon_all_helper.py``) and the completion check
(:func:`snbb_scheduler.checks._count_available_t1w`) import
:func:`collect_images` from here so that the same filtering rules apply to
execution and status evaluation.

Selection rules for T1w images:

1. Glob ``<bids_dir>/<subject>/ses-*/anat/*_T1w.nii.gz`` across all sessions.
2. Exclude files whose basename contains ``defaced``.
3. If any ``rec-norm`` variant exists, keep only those; otherwise keep all
   remaining files.

The same two-step filter is applied to T2w images independently.

Supports two run modes
----------------------
**Apptainer mode** (recommended, ``--sif`` provided)::

    python3 snbb_recon_all_helper.py \\
        --bids-dir   /data/snbb/bids \\
        --output-dir /data/snbb/derivatives/freesurfer \\
        --subject    sub-0001 \\
        --threads    8 \\
        --sif        /data/containers/freesurfer.sif \\
        --fs-license /data/freesurfer/license.txt

**Native mode** (``--sif`` omitted, FreeSurfer must be on PATH)::

    python3 snbb_recon_all_helper.py \\
        --bids-dir   /data/snbb/bids \\
        --output-dir /data/snbb/derivatives/freesurfer \\
        --subject    sub-0001 \\
        --threads    8
"""

__all__ = ["collect_images"]

import argparse
import subprocess
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Image collection
# ---------------------------------------------------------------------------

def collect_images(bids_dir: Path, subject: str) -> tuple[list[Path], list[Path]]:
    """Return ``(t1w_files, t2w_files)`` for all sessions of *subject*.

    Globs ``*_T1w.nii.gz`` and ``*_T2w.nii.gz`` across all ``ses-*``
    subdirectories.  The selection rules are applied identically to both
    modalities:

    1. Remove files whose name contains ``defaced``.
    2. If any ``rec-norm`` variant survives, keep only those; otherwise keep
       all remaining files.
    """
    t1w = sorted(bids_dir.glob(f"{subject}/ses-*/anat/*_T1w.nii.gz"))
    t1w = [f for f in t1w if "defaced" not in f.name]
    t1w_rec = [f for f in t1w if "rec-norm" in f.name]
    if t1w_rec:
        t1w = t1w_rec

    t2w = sorted(bids_dir.glob(f"{subject}/ses-*/anat/*_T2w.nii.gz"))
    t2w = [f for f in t2w if "defaced" not in f.name]
    t2w_rec = [f for f in t2w if "rec-norm" in f.name]
    if t2w_rec:
        t2w = t2w_rec

    return t1w, t2w


# ---------------------------------------------------------------------------
# Command builders
# ---------------------------------------------------------------------------

def _remap(path: Path, host_root: Path, container_root: str) -> str:
    """Replace *host_root* prefix with *container_root* in *path*."""
    return container_root + "/" + path.relative_to(host_root).as_posix()


def build_native_command(
    subject: str,
    output_dir: Path,
    t1w_files: list[Path],
    t2w_files: list[Path],
    threads: int,
) -> list[str]:
    """Build a native ``recon-all`` command."""
    cmd = [
        "recon-all",
        "-subject",
        subject,
        "-sd",
        str(output_dir),
        "-all",
        "-parallel",
        "-openmp",
        str(threads),
    ]
    for t1 in t1w_files:
        cmd += ["-i", str(t1)]
    if t2w_files:
        cmd += ["-T2", str(t2w_files[0]), "-T2pial"]
    return cmd


def build_apptainer_command(
    sif: Path,
    fs_license: Path,
    bids_dir: Path,
    output_dir: Path,
    subject: str,
    t1w_files: list[Path],
    t2w_files: list[Path],
    threads: int,
) -> list[str]:
    """Build an ``apptainer run`` command that wraps ``recon-all``.

    Inside the container:
      - ``/data``   ← bids_dir (read-only)
      - ``/output`` ← output_dir (read-write)
      - ``/opt/fs_license.txt`` ← fs_license (read-only)
    """
    cmd = [
        "apptainer",
        "run",
        "--cleanenv",
        "--env",
        "FS_LICENSE=/opt/fs_license.txt",
        "--bind",
        f"{bids_dir}:/data:ro",
        "--bind",
        f"{output_dir}:/output",
        "--bind",
        f"{fs_license}:/opt/fs_license.txt:ro",
        str(sif),
        "recon-all",
        "-subject",
        subject,
        "-sd",
        "/output",
        "-all",
        "-parallel",
        "-openmp",
        str(threads),
    ]
    for t1 in t1w_files:
        cmd += ["-i", _remap(t1, bids_dir, "/data")]
    if t2w_files:
        cmd += ["-T2", _remap(t2w_files[0], bids_dir, "/data"), "-T2pial"]
    return cmd


# ---------------------------------------------------------------------------
# CLI entry point (used by scripts/snbb_recon_all_helper.py)
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Glob BIDS T1w/T2w images and run FreeSurfer recon-all."
    )
    parser.add_argument(
        "--bids-dir", required=True, type=Path, help="BIDS root directory."
    )
    parser.add_argument(
        "--output-dir", required=True, type=Path, help="FreeSurfer subjects dir."
    )
    parser.add_argument(
        "--subject", required=True, help="Subject label, e.g. sub-0001."
    )
    parser.add_argument(
        "--threads", type=int, default=8, help="Number of parallel threads."
    )
    parser.add_argument(
        "--sif",
        type=Path,
        default=None,
        help="Path to FreeSurfer Apptainer SIF. When set, runs inside the container.",
    )
    parser.add_argument(
        "--fs-license",
        type=Path,
        default=None,
        help="FreeSurfer license file (required when --sif is set).",
    )
    args = parser.parse_args(argv)

    if args.sif is not None and args.fs_license is None:
        print("ERROR: --fs-license is required when --sif is set.", file=sys.stderr)
        return 1

    t1w_files, t2w_files = collect_images(args.bids_dir, args.subject)

    if not t1w_files:
        print(
            f"ERROR: No T1w images found for {args.subject} under {args.bids_dir}",
            file=sys.stderr,
        )
        return 1

    print(f"Found {len(t1w_files)} T1w image(s) and {len(t2w_files)} T2w image(s).")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.sif is not None:
        cmd = build_apptainer_command(
            sif=args.sif,
            fs_license=args.fs_license,
            bids_dir=args.bids_dir,
            output_dir=args.output_dir,
            subject=args.subject,
            t1w_files=t1w_files,
            t2w_files=t2w_files,
            threads=args.threads,
        )
    else:
        cmd = build_native_command(
            subject=args.subject,
            output_dir=args.output_dir,
            t1w_files=t1w_files,
            t2w_files=t2w_files,
            threads=args.threads,
        )

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())

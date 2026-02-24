#!/usr/bin/env python3
"""snbb_recon_all_helper.py — Collect T1w/T2w images and run FreeSurfer recon-all.

Usage::

    python3 snbb_recon_all_helper.py \
        --bids-dir /data/snbb/bids \
        --output-dir /data/snbb/derivatives/freesurfer \
        --subject sub-0001 \
        --threads 8

The script globs all T1w (and optionally T2w) NIfTI files across all sessions
for the given subject, then constructs and executes a ``recon-all`` command.
At least one T1w image is required; T2w images are optional.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def collect_images(bids_dir: Path, subject: str) -> tuple[list[Path], list[Path]]:
    """Return (t1w_files, t2w_files) for all sessions of *subject*."""
    t1w = sorted(bids_dir.glob(f"{subject}/ses-*/anat/*_T1w.nii.gz"))
    t2w = sorted(bids_dir.glob(f"{subject}/ses-*/anat/*_T2w.nii.gz"))
    return t1w, t2w


def build_command(
    subject: str,
    output_dir: Path,
    t1w_files: list[Path],
    t2w_files: list[Path],
    threads: int,
) -> list[str]:
    """Build the recon-all command list."""
    cmd = ["recon-all", "-all", "-s", subject, "-sd", str(output_dir)]
    for t1 in t1w_files:
        cmd += ["-i", str(t1)]
    if t2w_files:
        cmd += ["-T2", str(t2w_files[0]), "-T2pial"]
    cmd += ["-threads", str(threads)]
    return cmd


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Glob BIDS T1w/T2w images and run FreeSurfer recon-all."
    )
    parser.add_argument("--bids-dir", required=True, type=Path, help="BIDS root directory.")
    parser.add_argument("--output-dir", required=True, type=Path, help="FreeSurfer subjects dir.")
    parser.add_argument("--subject", required=True, help="Subject label, e.g. sub-0001.")
    parser.add_argument("--threads", type=int, default=8, help="Number of threads for recon-all.")
    args = parser.parse_args(argv)

    t1w_files, t2w_files = collect_images(args.bids_dir, args.subject)

    if not t1w_files:
        print(
            f"ERROR: No T1w images found for {args.subject} under {args.bids_dir}",
            file=sys.stderr,
        )
        return 1

    print(f"Found {len(t1w_files)} T1w image(s) and {len(t2w_files)} T2w image(s).")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    cmd = build_command(args.subject, args.output_dir, t1w_files, t2w_files, args.threads)
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())

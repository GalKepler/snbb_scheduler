from __future__ import annotations

"""snbb_scheduler.freesurfer — T1w/T2w collection and FreeSurfer command building.

This module is the single source of truth for which images are selected for a
FreeSurfer ``recon-all`` run and for orchestrating the longitudinal pipeline.

FreeSurfer longitudinal pipeline
---------------------------------
A single ``freesurfer`` procedure (subject-scoped) handles both single-session
and multi-session subjects:

**Single-session subjects** — cross-sectional only::

    recon-all -s <subject> -i <T1w> [-T2 <T2w> -T2pial] -sd <SUBJECTS_DIR> -all

**Multi-session subjects** — full 3-step longitudinal pipeline:

1. Cross-sectional for each session::

    recon-all -s <subject>_<session> -i <T1w> [-T2 <T2w> -T2pial] -sd <SUBJECTS_DIR> -all

2. Template (base) — unbiased within-subject average::

    recon-all -base <subject> -tp <subject>_<ses1> -tp <subject>_<ses2> -sd <SUBJECTS_DIR> -all

3. Longitudinal refinement for each session::

    recon-all -long <subject>_<session> <subject> -sd <SUBJECTS_DIR> -all

T1w/T2w selection rules
------------------------
**Across-session collection** (:func:`collect_images`):
  Globs all sessions and returns all qualifying files as a flat list.
  Used by :func:`~snbb_scheduler.checks._count_available_t1w`.

**Per-session collection** (:func:`collect_session_t1w`, :func:`collect_session_t2w`):
  One file per session with two-step filtering:

  1. Exclude files whose basename contains ``defaced``.
  2. Prefer ``rec-norm`` variants when they exist.
  3. Return the first (sorted) surviving file, or ``None`` if none found.

Output layout
--------------
Single-session::

    freesurfer/<subject>/scripts/recon-all.done

Multi-session::

    freesurfer/<subject>_<session>/scripts/recon-all.done    (cross-sectional)
    freesurfer/<subject>/scripts/recon-all.done               (template)
    freesurfer/<subject>_<session>.long.<subject>/scripts/recon-all.done  (longitudinal)

QSIRecon integration
---------------------
QSIRecon's ``--fs-subjects-dir`` receives ``derivatives/freesurfer`` and looks
up ``<subject>/mri/aparc+aseg.mgz``.  For single-session subjects this is the
cross-sectional output; for multi-session subjects this is the template
directory, which contains a full FreeSurfer reconstruction suitable for HSVS.
"""

__all__ = [
    "collect_images",
    "collect_session_t1w",
    "collect_session_t2w",
    "collect_all_session_images",
    "build_cross_sectional_command",
    "build_template_command",
    "build_longitudinal_command",
    "build_cross_sectional_apptainer_command",
    "build_template_apptainer_command",
    "build_longitudinal_apptainer_command",
]

import argparse
import subprocess
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Image collection — across all sessions (original API)
# ---------------------------------------------------------------------------


def collect_images(bids_dir: Path, subject: str) -> tuple[list[Path], list[Path]]:
    """Return ``(t1w_files, t2w_files)`` for all sessions of *subject*.

    Globs ``*_T1w.nii.gz`` and ``*_T2w.nii.gz`` across all ``ses-*``
    subdirectories.  The selection rules are applied identically to both
    modalities:

    1. Remove files whose name contains ``defaced``.
    2. If any ``rec-norm`` variant survives, keep only those; otherwise keep
       all remaining files.

    Used by :func:`~snbb_scheduler.checks._count_available_t1w` and as a
    fallback for the completion check.
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
# Image collection — per session (for longitudinal pipeline)
# ---------------------------------------------------------------------------


def collect_session_t1w(bids_dir: Path, subject: str, session: str) -> Path | None:
    """Return the single T1w NIfTI to use for a cross-sectional run.

    Applies the two-step filter:

    1. Exclude files whose basename contains ``defaced``.
    2. Prefer ``rec-norm`` variants when they exist.
    3. Return the first (sorted) surviving file, or ``None`` if none found.

    Parameters
    ----------
    bids_dir:
        BIDS root directory.
    subject:
        BIDS subject label, e.g. ``sub-0001``.
    session:
        BIDS session label, e.g. ``ses-01``.
    """
    candidates = sorted(bids_dir.glob(f"{subject}/{session}/anat/*_T1w.nii.gz"))
    candidates = [f for f in candidates if "defaced" not in f.name]
    rec_norm = [f for f in candidates if "rec-norm" in f.name]
    if rec_norm:
        candidates = rec_norm
    return candidates[0] if candidates else None


def collect_session_t2w(bids_dir: Path, subject: str, session: str) -> Path | None:
    """Return the single T2w NIfTI to use for a cross-sectional run.

    Same two-step filter as :func:`collect_session_t1w`, applied to T2w files.

    Parameters
    ----------
    bids_dir:
        BIDS root directory.
    subject:
        BIDS subject label, e.g. ``sub-0001``.
    session:
        BIDS session label, e.g. ``ses-01``.
    """
    candidates = sorted(bids_dir.glob(f"{subject}/{session}/anat/*_T2w.nii.gz"))
    candidates = [f for f in candidates if "defaced" not in f.name]
    rec_norm = [f for f in candidates if "rec-norm" in f.name]
    if rec_norm:
        candidates = rec_norm
    return candidates[0] if candidates else None


def collect_all_session_images(
    bids_dir: Path, subject: str
) -> dict[str, tuple[Path, Path | None]]:
    """Return a mapping of session label → ``(T1w, T2w_or_None)`` for all valid sessions.

    Iterates every ``ses-*`` subdirectory under ``<bids_dir>/<subject>`` and
    calls :func:`collect_session_t1w` and :func:`collect_session_t2w` for each.
    Sessions without a suitable T1w image are omitted.

    Parameters
    ----------
    bids_dir:
        BIDS root directory.
    subject:
        BIDS subject label, e.g. ``sub-0001``.

    Returns
    -------
    dict[str, tuple[Path, Path | None]]
        Ordered mapping of ``session_label → (t1w_path, t2w_path_or_None)``.
    """
    subject_dir = bids_dir / subject
    if not subject_dir.exists():
        return {}
    result: dict[str, tuple[Path, Path | None]] = {}
    for ses_dir in sorted(subject_dir.iterdir()):
        if not ses_dir.is_dir() or not ses_dir.name.startswith("ses-"):
            continue
        t1w = collect_session_t1w(bids_dir, subject, ses_dir.name)
        if t1w is None:
            continue
        t2w = collect_session_t2w(bids_dir, subject, ses_dir.name)
        result[ses_dir.name] = (t1w, t2w)
    return result


# ---------------------------------------------------------------------------
# Path remapping helper (container paths)
# ---------------------------------------------------------------------------


def _remap(path: Path, host_root: Path, container_root: str) -> str:
    """Replace *host_root* prefix with *container_root* in *path*."""
    return container_root + "/" + path.relative_to(host_root).as_posix()


# ---------------------------------------------------------------------------
# Native command builders
# ---------------------------------------------------------------------------


def build_cross_sectional_command(
    subject_id: str,
    output_dir: Path,
    t1w: Path,
    t2w: Path | None,
    threads: int,
) -> list[str]:
    """Build a native ``recon-all`` cross-sectional command.

    Parameters
    ----------
    subject_id:
        FreeSurfer subject ID (``<subject>`` for single-session,
        ``<subject>_<session>`` for multi-session).
    output_dir:
        FreeSurfer SUBJECTS_DIR.
    t1w:
        Path to the T1w NIfTI.
    t2w:
        Optional path to the T2w NIfTI.
    threads:
        Number of parallel threads.
    """
    cmd = [
        "recon-all",
        "-subject",
        subject_id,
        "-sd",
        str(output_dir),
        "-i",
        str(t1w),
        "-all",
        "-parallel",
        "-openmp",
        str(threads),
    ]
    if t2w is not None:
        cmd += ["-T2", str(t2w), "-T2pial"]
    return cmd


def build_template_command(
    subject: str,
    sessions: list[str],
    output_dir: Path,
    threads: int,
) -> list[str]:
    """Build a native ``recon-all -base`` template command.

    Parameters
    ----------
    subject:
        BIDS subject label, e.g. ``sub-0001`` (used as the template ID).
    sessions:
        List of session labels whose cross-sectional IDs are timepoints,
        e.g. ``["ses-01", "ses-02"]``.
    output_dir:
        FreeSurfer SUBJECTS_DIR.
    threads:
        Number of parallel threads.
    """
    cmd = [
        "recon-all",
        "-base",
        subject,
        "-sd",
        str(output_dir),
        "-all",
        "-parallel",
        "-openmp",
        str(threads),
    ]
    for ses in sessions:
        cmd += ["-tp", f"{subject}_{ses}"]
    return cmd


def build_longitudinal_command(
    subject: str,
    session: str,
    output_dir: Path,
    threads: int,
) -> list[str]:
    """Build a native ``recon-all -long`` longitudinal command.

    Parameters
    ----------
    subject:
        BIDS subject label, e.g. ``sub-0001`` (also the template ID).
    session:
        BIDS session label, e.g. ``ses-01``.
    output_dir:
        FreeSurfer SUBJECTS_DIR.
    threads:
        Number of parallel threads.
    """
    return [
        "recon-all",
        "-long",
        f"{subject}_{session}",
        subject,
        "-sd",
        str(output_dir),
        "-all",
        "-parallel",
        "-openmp",
        str(threads),
    ]


# ---------------------------------------------------------------------------
# Apptainer command builders
# ---------------------------------------------------------------------------


def _base_apptainer_cmd(
    sif: Path,
    fs_license: Path,
    bids_dir: Path,
    output_dir: Path,
) -> list[str]:
    """Return the common ``apptainer run`` preamble with bind mounts."""
    return [
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
    ]


def build_cross_sectional_apptainer_command(
    sif: Path,
    fs_license: Path,
    bids_dir: Path,
    output_dir: Path,
    subject_id: str,
    t1w: Path,
    t2w: Path | None,
    threads: int,
) -> list[str]:
    """Build an Apptainer ``recon-all`` cross-sectional command.

    The container binds:
    - ``/data``   ← *bids_dir* (read-only)
    - ``/output`` ← *output_dir* (read-write)
    - ``/opt/fs_license.txt`` ← *fs_license* (read-only)
    """
    cmd = _base_apptainer_cmd(sif, fs_license, bids_dir, output_dir)
    cmd += [
        "recon-all",
        "-subject",
        subject_id,
        "-sd",
        "/output",
        "-i",
        _remap(t1w, bids_dir, "/data"),
        "-all",
        "-parallel",
        "-openmp",
        str(threads),
    ]
    if t2w is not None:
        cmd += ["-T2", _remap(t2w, bids_dir, "/data"), "-T2pial"]
    return cmd


def build_template_apptainer_command(
    sif: Path,
    fs_license: Path,
    bids_dir: Path,
    output_dir: Path,
    subject: str,
    sessions: list[str],
    threads: int,
) -> list[str]:
    """Build an Apptainer ``recon-all -base`` template command."""
    cmd = _base_apptainer_cmd(sif, fs_license, bids_dir, output_dir)
    cmd += [
        "recon-all",
        "-base",
        subject,
        "-sd",
        "/output",
        "-all",
        "-parallel",
        "-openmp",
        str(threads),
    ]
    for ses in sessions:
        cmd += ["-tp", f"{subject}_{ses}"]
    return cmd


def build_longitudinal_apptainer_command(
    sif: Path,
    fs_license: Path,
    bids_dir: Path,
    output_dir: Path,
    subject: str,
    session: str,
    threads: int,
) -> list[str]:
    """Build an Apptainer ``recon-all -long`` longitudinal command."""
    cmd = _base_apptainer_cmd(sif, fs_license, bids_dir, output_dir)
    cmd += [
        "recon-all",
        "-long",
        f"{subject}_{session}",
        subject,
        "-sd",
        "/output",
        "-all",
        "-parallel",
        "-openmp",
        str(threads),
    ]
    return cmd


# Legacy aliases kept for backward compatibility
def build_native_command(
    subject: str,
    output_dir: Path,
    t1w_files: list[Path],
    t2w_files: list[Path],
    threads: int,
) -> list[str]:
    """Build a native ``recon-all`` command (legacy across-session API).

    Passes all T1w files as separate ``-i`` inputs.  Use
    :func:`build_cross_sectional_command` for the longitudinal pipeline.
    """
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
    """Build an Apptainer ``recon-all`` command (legacy across-session API).

    Passes all T1w files as separate ``-i`` inputs.  Use
    :func:`build_cross_sectional_apptainer_command` for the longitudinal pipeline.
    """
    cmd = _base_apptainer_cmd(sif, fs_license, bids_dir, output_dir)
    cmd += [
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


def _done(subjects_dir: Path, subject_id: str) -> bool:
    """Return True if ``recon-all.done`` exists for *subject_id*."""
    return (subjects_dir / subject_id / "scripts" / "recon-all.done").exists()


def _run(cmd: list[str], label: str) -> int:
    """Run *cmd* and return its exit code, printing *label* before executing."""
    print(f"[freesurfer] {label}")
    print(f"[freesurfer] Running: {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(
            f"[freesurfer] ERROR: {label} failed with exit code {result.returncode}",
            file=sys.stderr,
        )
    return result.returncode


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the FreeSurfer longitudinal helper.

    Orchestrates the full longitudinal pipeline for multi-session subjects,
    or a plain cross-sectional run for single-session subjects.  Already-
    completed steps (``recon-all.done`` exists) are skipped automatically.

    Usage example::

        python3 snbb_recon_all_helper.py \\
            --bids-dir /data/snbb/bids \\
            --output-dir /data/snbb/derivatives/freesurfer \\
            --subject sub-0001 \\
            --threads 8 \\
            --sif /containers/freesurfer.sif \\
            --fs-license /misc/freesurfer/license.txt
    """
    parser = argparse.ArgumentParser(
        description="FreeSurfer longitudinal helper — cross-sectional or 3-step pipeline."
    )
    parser.add_argument(
        "--bids-dir", required=True, type=Path, help="BIDS root directory."
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="FreeSurfer SUBJECTS_DIR (derivatives/freesurfer).",
    )
    parser.add_argument(
        "--subject", required=True, help="BIDS subject label, e.g. sub-0001."
    )
    parser.add_argument(
        "--threads", type=int, default=8, help="Number of parallel threads (default: 8)."
    )
    parser.add_argument(
        "--sif",
        type=Path,
        default=None,
        help="Path to FreeSurfer Apptainer SIF.  When set, runs inside the container.",
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

    sessions_images = collect_all_session_images(args.bids_dir, args.subject)

    if not sessions_images:
        print(
            f"ERROR: No sessions with a T1w image found for {args.subject} "
            f"under {args.bids_dir}",
            file=sys.stderr,
        )
        return 1

    sessions = list(sessions_images.keys())
    subjects_dir = args.output_dir
    subjects_dir.mkdir(parents=True, exist_ok=True)
    use_apptainer = args.sif is not None

    # ── Single-session: plain cross-sectional run ─────────────────────────
    if len(sessions) == 1:
        ses = sessions[0]
        t1w, t2w = sessions_images[ses]
        subject_id = args.subject  # output lands at <subject>/

        print(f"[freesurfer] Single session ({ses}): running cross-sectional FreeSurfer.")

        if _done(subjects_dir, subject_id):
            print(f"[freesurfer] {subject_id} already complete — skipping.")
            return 0

        if use_apptainer:
            cmd = build_cross_sectional_apptainer_command(
                sif=args.sif,
                fs_license=args.fs_license,
                bids_dir=args.bids_dir,
                output_dir=subjects_dir,
                subject_id=subject_id,
                t1w=t1w,
                t2w=t2w,
                threads=args.threads,
            )
        else:
            cmd = build_cross_sectional_command(
                subject_id=subject_id,
                output_dir=subjects_dir,
                t1w=t1w,
                t2w=t2w,
                threads=args.threads,
            )

        return _run(cmd, f"cross-sectional {subject_id}")

    # ── Multi-session: full 3-step longitudinal pipeline ─────────────────

    print(
        f"[freesurfer] Multi-session ({sessions}): running longitudinal FreeSurfer pipeline."
    )

    # Step 1 — Cross-sectional for each session
    for ses, (t1w, t2w) in sessions_images.items():
        subject_id = f"{args.subject}_{ses}"
        if _done(subjects_dir, subject_id):
            print(f"[freesurfer] Step 1 ({subject_id}): already complete — skipping.")
            continue

        if use_apptainer:
            cmd = build_cross_sectional_apptainer_command(
                sif=args.sif,
                fs_license=args.fs_license,
                bids_dir=args.bids_dir,
                output_dir=subjects_dir,
                subject_id=subject_id,
                t1w=t1w,
                t2w=t2w,
                threads=args.threads,
            )
        else:
            cmd = build_cross_sectional_command(
                subject_id=subject_id,
                output_dir=subjects_dir,
                t1w=t1w,
                t2w=t2w,
                threads=args.threads,
            )

        rc = _run(cmd, f"step 1 cross-sectional {subject_id}")
        if rc != 0:
            return rc

    # Step 2 — Template
    if _done(subjects_dir, args.subject):
        print(f"[freesurfer] Step 2 (template {args.subject}): already complete — skipping.")
    else:
        if use_apptainer:
            cmd = build_template_apptainer_command(
                sif=args.sif,
                fs_license=args.fs_license,
                bids_dir=args.bids_dir,
                output_dir=subjects_dir,
                subject=args.subject,
                sessions=sessions,
                threads=args.threads,
            )
        else:
            cmd = build_template_command(
                subject=args.subject,
                sessions=sessions,
                output_dir=subjects_dir,
                threads=args.threads,
            )

        rc = _run(cmd, f"step 2 template {args.subject}")
        if rc != 0:
            return rc

    # Step 3 — Longitudinal for each session
    for ses in sessions:
        long_id = f"{args.subject}_{ses}.long.{args.subject}"
        if _done(subjects_dir, long_id):
            print(f"[freesurfer] Step 3 ({long_id}): already complete — skipping.")
            continue

        if use_apptainer:
            cmd = build_longitudinal_apptainer_command(
                sif=args.sif,
                fs_license=args.fs_license,
                bids_dir=args.bids_dir,
                output_dir=subjects_dir,
                subject=args.subject,
                session=ses,
                threads=args.threads,
            )
        else:
            cmd = build_longitudinal_command(
                subject=args.subject,
                session=ses,
                output_dir=subjects_dir,
                threads=args.threads,
            )

        rc = _run(cmd, f"step 3 longitudinal {long_id}")
        if rc != 0:
            return rc

    print(f"[freesurfer] All steps complete for {args.subject}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

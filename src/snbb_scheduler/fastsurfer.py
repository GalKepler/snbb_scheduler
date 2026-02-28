from __future__ import annotations

"""snbb_scheduler.fastsurfer — T1w collection and command building for
FastSurfer longitudinal processing.

FastSurfer longitudinal pipeline overview
-----------------------------------------
The longitudinal pipeline runs in three stages that must execute in order:

1. **Cross-sectional** (``fastsurfer_cross``, per-session):
   Each timepoint is processed independently with FastSurfer::

       fastsurfer --sid sub-0001_ses-01 --sd <SUBJECTS_DIR> \\
           --t1 <T1w.nii.gz> --threads 8

   Output: ``<SUBJECTS_DIR>/sub-0001_ses-01/``

2. **Template creation** (``fastsurfer_template``, per-subject):
   A within-subject unbiased anatomical template is built from all
   cross-sectional results using FreeSurfer's ``recon-all -base``.
   Requires at least **two** completed cross-sectional timepoints::

       recon-all -base sub-0001 \\
           -tp sub-0001_ses-01 -tp sub-0001_ses-02 \\
           -sd <SUBJECTS_DIR> -all

   Output: ``<SUBJECTS_DIR>/sub-0001/``

3. **Longitudinal** (``fastsurfer_long``, per-session):
   Each timepoint is reprocessed longitudinally, using the template as a
   prior for cortical surface reconstruction::

       recon-all -long sub-0001_ses-01 sub-0001 \\
           -sd <SUBJECTS_DIR> -all

   Output: ``<SUBJECTS_DIR>/sub-0001_ses-01.long.sub-0001/``

T1w selection rules (cross-sectional)
--------------------------------------
Only one T1w image is passed per session (unlike FreeSurfer cross-sectional
which pools all sessions).  The selection follows the same two-step filter
used by :mod:`snbb_scheduler.freesurfer`:

1. Exclude files whose basename contains ``defaced``.
2. If any ``rec-norm`` variant survives, keep only those; otherwise keep all.
3. Return the first (alphabetically sorted) surviving image.

SUBJECTS_DIR naming
-------------------
All three stages share a single ``SUBJECTS_DIR``
(``<derivatives_root>/fastsurfer/``).  The subdirectory names encode stage
and session:

* Cross-sectional : ``<subject>_<session>``  (e.g. ``sub-0001_ses-01``)
* Template        : ``<subject>``            (e.g. ``sub-0001``)
* Longitudinal    : ``<subject>_<session>.long.<subject>``
                    (e.g. ``sub-0001_ses-01.long.sub-0001``)
"""

__all__ = [
    "collect_session_t1w",
    "fastsurfer_sid",
    "fastsurfer_long_sid",
    "build_cross_apptainer_command",
    "build_template_apptainer_command",
    "build_long_apptainer_command",
]

import argparse
import subprocess
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------


def fastsurfer_sid(subject: str, session: str) -> str:
    """Return the FastSurfer subject-ID for a cross-sectional run.

    Parameters
    ----------
    subject:
        BIDS subject label, e.g. ``sub-0001``.
    session:
        BIDS session label, e.g. ``ses-01``.

    Returns
    -------
    str
        Combined identifier ``"{subject}_{session}"``,
        e.g. ``"sub-0001_ses-01"``.
    """
    return f"{subject}_{session}"


def fastsurfer_long_sid(subject: str, session: str) -> str:
    """Return the longitudinal output directory name for a session.

    Parameters
    ----------
    subject:
        BIDS subject label, e.g. ``sub-0001``.
    session:
        BIDS session label, e.g. ``ses-01``.

    Returns
    -------
    str
        Identifier ``"{subject}_{session}.long.{subject}"``,
        e.g. ``"sub-0001_ses-01.long.sub-0001"``.
    """
    return f"{subject}_{session}.long.{subject}"


# ---------------------------------------------------------------------------
# T1w collection
# ---------------------------------------------------------------------------


def collect_session_t1w(bids_dir: Path, subject: str, session: str) -> Path | None:
    """Return the single T1w NIfTI to use for a cross-sectional FastSurfer run.

    Applies the same two-step filter used by FreeSurfer image collection:

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

    Returns
    -------
    Path | None
        Absolute path to the chosen T1w NIfTI, or ``None`` if the session
        has no suitable T1w image.
    """
    candidates = sorted(
        bids_dir.glob(f"{subject}/{session}/anat/*_T1w.nii.gz")
    )
    candidates = [f for f in candidates if "defaced" not in f.name]

    rec_norm = [f for f in candidates if "rec-norm" in f.name]
    if rec_norm:
        candidates = rec_norm

    return candidates[0] if candidates else None


# ---------------------------------------------------------------------------
# Path remapping helper (container paths)
# ---------------------------------------------------------------------------


def _remap(path: Path, host_root: Path, container_root: str) -> str:
    """Rewrite *path* by replacing *host_root* with *container_root*.

    Used when building Apptainer bind-mount commands so that host filesystem
    paths are translated to their in-container equivalents.
    """
    return container_root + "/" + path.relative_to(host_root).as_posix()


# ---------------------------------------------------------------------------
# Command builders
# ---------------------------------------------------------------------------


def build_cross_apptainer_command(
    sif: Path,
    fs_license: Path,
    bids_dir: Path,
    output_dir: Path,
    subject: str,
    session: str,
    t1w_file: Path,
    threads: int,
) -> list[str]:
    """Build an Apptainer command for a single cross-sectional FastSurfer run.

    The container is invoked with:

    * ``/data``   ← *bids_dir* (read-only)
    * ``/output`` ← *output_dir* (read-write)
    * ``/opt/fs_license.txt`` ← *fs_license* (read-only)

    Parameters
    ----------
    sif:
        Path to the FastSurfer Apptainer SIF image.
    fs_license:
        FreeSurfer license file (required even for FastSurfer).
    bids_dir:
        BIDS root directory (bound read-only into the container at ``/data``).
    output_dir:
        FastSurfer SUBJECTS_DIR (bound read-write at ``/output``).
    subject:
        BIDS subject label, e.g. ``sub-0001``.
    session:
        BIDS session label, e.g. ``ses-01``.
    t1w_file:
        Absolute path to the T1w NIfTI for this session.
    threads:
        Number of parallel threads to pass to FastSurfer.

    Returns
    -------
    list[str]
        Complete ``apptainer run`` command suitable for :func:`subprocess.run`.
    """
    sid = fastsurfer_sid(subject, session)
    t1w_container = _remap(t1w_file, bids_dir, "/data")
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
        "--sid",
        sid,
        "--sd",
        "/output",
        "--t1",
        t1w_container,
        "--threads",
        str(threads),
        "--3T",
    ]


def build_template_apptainer_command(
    sif: Path,
    fs_license: Path,
    output_dir: Path,
    subject: str,
    session_ids: list[str],
    threads: int,
) -> list[str]:
    """Build an Apptainer command for within-subject template creation.

    Wraps FreeSurfer's ``recon-all -base`` inside the FastSurfer container.
    The template step does not read BIDS input directly — it uses the
    cross-sectional outputs that already reside under *output_dir*.

    Parameters
    ----------
    sif:
        Path to the FastSurfer Apptainer SIF image.
    fs_license:
        FreeSurfer license file.
    output_dir:
        FastSurfer SUBJECTS_DIR containing the cross-sectional results.
    subject:
        BIDS subject label, e.g. ``sub-0001``.
    session_ids:
        Ordered list of session labels whose cross-sectional runs are
        complete, e.g. ``["ses-01", "ses-02"]``.
    threads:
        Number of threads to pass to ``recon-all``.

    Returns
    -------
    list[str]
        Complete ``apptainer run`` command.
    """
    cmd = [
        "apptainer",
        "run",
        "--cleanenv",
        "--env",
        "FS_LICENSE=/opt/fs_license.txt",
        "--bind",
        f"{output_dir}:/output",
        "--bind",
        f"{fs_license}:/opt/fs_license.txt:ro",
        str(sif),
        # Inside the container run FreeSurfer's recon-all -base
        "recon-all",
        "-base",
        subject,
        "-sd",
        "/output",
    ]
    for ses in session_ids:
        cmd += ["-tp", fastsurfer_sid(subject, ses)]
    cmd += ["-all", "-parallel", "-openmp", str(threads)]
    return cmd


def build_long_apptainer_command(
    sif: Path,
    fs_license: Path,
    output_dir: Path,
    subject: str,
    session: str,
    threads: int,
) -> list[str]:
    """Build an Apptainer command for longitudinal FreeSurfer refinement.

    Wraps FreeSurfer's ``recon-all -long`` inside the FastSurfer container.

    Parameters
    ----------
    sif:
        Path to the FastSurfer Apptainer SIF image.
    fs_license:
        FreeSurfer license file.
    output_dir:
        FastSurfer SUBJECTS_DIR.
    subject:
        BIDS subject label, e.g. ``sub-0001``.
    session:
        BIDS session label, e.g. ``ses-01``.
    threads:
        Number of threads to pass to ``recon-all``.

    Returns
    -------
    list[str]
        Complete ``apptainer run`` command.
    """
    sid = fastsurfer_sid(subject, session)
    return [
        "apptainer",
        "run",
        "--cleanenv",
        "--env",
        "FS_LICENSE=/opt/fs_license.txt",
        "--bind",
        f"{output_dir}:/output",
        "--bind",
        f"{fs_license}:/opt/fs_license.txt:ro",
        str(sif),
        "recon-all",
        "-long",
        sid,
        subject,
        "-sd",
        "/output",
        "-all",
        "-parallel",
        "-openmp",
        str(threads),
    ]


# ---------------------------------------------------------------------------
# CLI entry point (used by scripts/snbb_fastsurfer_helper.py)
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for all three FastSurfer longitudinal stages.

    Usage examples::

        # Cross-sectional
        python3 snbb_fastsurfer_helper.py cross \\
            --bids-dir /data/bids --output-dir /data/derivatives/fastsurfer \\
            --subject sub-0001 --session ses-01 \\
            --sif /containers/fastsurfer.sif --fs-license /misc/fs/license.txt

        # Template
        python3 snbb_fastsurfer_helper.py template \\
            --output-dir /data/derivatives/fastsurfer \\
            --subject sub-0001 --sessions ses-01 ses-02 \\
            --sif /containers/fastsurfer.sif --fs-license /misc/fs/license.txt

        # Longitudinal
        python3 snbb_fastsurfer_helper.py long \\
            --output-dir /data/derivatives/fastsurfer \\
            --subject sub-0001 --session ses-01 \\
            --sif /containers/fastsurfer.sif --fs-license /misc/fs/license.txt
    """
    parser = argparse.ArgumentParser(
        description="FastSurfer longitudinal pipeline helper.",
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    # ── shared arguments ──────────────────────────────────────────────────
    def _add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--output-dir", required=True, type=Path,
                       help="FastSurfer SUBJECTS_DIR (derivatives/fastsurfer).")
        p.add_argument("--subject", required=True,
                       help="BIDS subject label, e.g. sub-0001.")
        p.add_argument("--sif", required=True, type=Path,
                       help="Path to FastSurfer Apptainer SIF image.")
        p.add_argument("--fs-license", required=True, type=Path,
                       help="FreeSurfer license file.")
        p.add_argument("--threads", type=int, default=8,
                       help="Number of parallel threads (default: 8).")

    # ── cross-sectional ───────────────────────────────────────────────────
    p_cross = sub.add_parser("cross", help="Run cross-sectional FastSurfer.")
    _add_common(p_cross)
    p_cross.add_argument("--bids-dir", required=True, type=Path,
                          help="BIDS root directory.")
    p_cross.add_argument("--session", required=True,
                          help="BIDS session label, e.g. ses-01.")

    # ── template ─────────────────────────────────────────────────────────
    p_tmpl = sub.add_parser("template", help="Create within-subject template.")
    _add_common(p_tmpl)
    p_tmpl.add_argument("--sessions", required=True, nargs="+",
                         help="Session labels whose cross-sectional runs are complete.")

    # ── longitudinal ──────────────────────────────────────────────────────
    p_long = sub.add_parser("long", help="Run longitudinal refinement.")
    _add_common(p_long)
    p_long.add_argument("--session", required=True,
                         help="BIDS session label to process longitudinally.")

    args = parser.parse_args(argv)

    # ── dispatch ──────────────────────────────────────────────────────────
    if args.mode == "cross":
        t1w = collect_session_t1w(args.bids_dir, args.subject, args.session)
        if t1w is None:
            print(
                f"ERROR: No T1w image found for {args.subject} {args.session} "
                f"under {args.bids_dir}",
                file=sys.stderr,
            )
            return 1
        print(f"Using T1w: {t1w}")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        cmd = build_cross_apptainer_command(
            sif=args.sif,
            fs_license=args.fs_license,
            bids_dir=args.bids_dir,
            output_dir=args.output_dir,
            subject=args.subject,
            session=args.session,
            t1w_file=t1w,
            threads=args.threads,
        )

    elif args.mode == "template":
        print(f"Building template for {args.subject} from sessions: {args.sessions}")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        cmd = build_template_apptainer_command(
            sif=args.sif,
            fs_license=args.fs_license,
            output_dir=args.output_dir,
            subject=args.subject,
            session_ids=args.sessions,
            threads=args.threads,
        )

    else:  # long
        print(f"Running longitudinal for {args.subject} {args.session}")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        cmd = build_long_apptainer_command(
            sif=args.sif,
            fs_license=args.fs_license,
            output_dir=args.output_dir,
            subject=args.subject,
            session=args.session,
            threads=args.threads,
        )

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())

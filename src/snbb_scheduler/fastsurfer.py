from __future__ import annotations

"""snbb_scheduler.fastsurfer — T1w collection and command building for
FastSurfer processing.

FastSurfer pipeline overview
-----------------------------
A single ``fastsurfer`` procedure (subject-scoped) covers both the
cross-sectional and longitudinal cases:

* **1 session** — runs ``run_fastsurfer.sh`` (cross-sectional) via the
  existing :func:`build_cross_apptainer_command` builder.

* **2+ sessions** — runs ``long_fastsurfer.sh`` (full longitudinal pipeline)
  via :func:`build_long_fastsurfer_command`.  ``long_fastsurfer.sh`` handles
  the cross-sectional pre-processing, template creation, and longitudinal
  refinement internally in a single invocation.

T1w selection rules
--------------------
One T1w image is selected per session.  The selection follows the same
two-step filter used by :mod:`snbb_scheduler.freesurfer`:

1. Exclude files whose basename contains ``defaced``.
2. Prefer ``rec-norm`` variants when they exist.
3. Return the first (sorted) surviving image, or ``None`` if none found.

SUBJECTS_DIR naming
--------------------
Each subject's outputs are nested under ``<derivatives_root>/fastsurfer/<subject>/``,
which is bound to ``/output`` inside the container.  Within that subject directory:

* Cross-sectional : ``<session>``  (e.g. ``ses-01``)
* Longitudinal    : ``<session>.long.<subject>``
                    (e.g. ``ses-01.long.sub-0001``)
* Template        : ``<subject>``  (e.g. ``sub-0001``)

Full example layout::

    fastsurfer/
    └── sub-0001/
        ├── ses-01                      (cross-sectional intermediate)
        ├── ses-01.long.sub-0001        (longitudinal final)
        └── sub-0001                    (template)
"""

__all__ = [
    "collect_session_t1w",
    "collect_all_session_t1ws",
    "fastsurfer_sid",
    "fastsurfer_long_sid",
    "build_cross_apptainer_command",
    "build_long_fastsurfer_command",
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

    With the nested per-subject output structure the subject directory is
    already the ``SUBJECTS_DIR``, so only the bare session label is needed.

    Parameters
    ----------
    subject:
        BIDS subject label, e.g. ``sub-0001`` (unused; kept for API symmetry).
    session:
        BIDS session label, e.g. ``ses-01``.

    Returns
    -------
    str
        The session label, e.g. ``"ses-01"``.
    """
    return session


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
        Identifier ``"{session}.long.{subject}"``,
        e.g. ``"ses-01.long.sub-0001"``.
    """
    return f"{session}.long.{subject}"


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


def collect_all_session_t1ws(bids_dir: Path, subject: str) -> dict[str, Path]:
    """Return a mapping of session label → T1w path for all valid sessions.

    Iterates every ``ses-*`` subdirectory under ``<bids_dir>/<subject>`` and
    calls :func:`collect_session_t1w` for each.  Sessions without a suitable
    T1w image are omitted.

    Parameters
    ----------
    bids_dir:
        BIDS root directory.
    subject:
        BIDS subject label, e.g. ``sub-0001``.

    Returns
    -------
    dict[str, Path]
        Ordered mapping of ``session_label → absolute T1w path``,
        e.g. ``{"ses-01": Path(...), "ses-02": Path(...)}``.
    """
    subject_dir = bids_dir / subject
    if not subject_dir.exists():
        return {}
    result: dict[str, Path] = {}
    for ses_dir in sorted(subject_dir.iterdir()):
        if not ses_dir.is_dir() or not ses_dir.name.startswith("ses-"):
            continue
        t1w = collect_session_t1w(bids_dir, subject, ses_dir.name)
        if t1w is not None:
            result[ses_dir.name] = t1w
    return result


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
        f"{output_dir}/{subject}:/output",
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


def build_long_fastsurfer_command(
    sif: Path,
    fs_license: Path,
    bids_dir: Path,
    output_dir: Path,
    subject: str,
    sessions_t1ws: dict[str, Path],
    threads: int,
) -> list[str]:
    """Build an Apptainer command for the FastSurfer longitudinal pipeline.

    Invokes ``long_fastsurfer.sh`` inside the FastSurfer container, which
    handles cross-sectional pre-processing, template creation, and
    longitudinal refinement in a single call.

    The container is invoked with:

    * ``/data``   ← *bids_dir* (read-only)
    * ``/output`` ← *output_dir* (read-write)
    * ``/opt/fs_license.txt`` ← *fs_license* (read-only)

    Parameters
    ----------
    sif:
        Path to the FastSurfer Apptainer SIF image.
    fs_license:
        FreeSurfer license file.
    bids_dir:
        BIDS root directory (bound read-only at ``/data``).
    output_dir:
        FastSurfer SUBJECTS_DIR (bound read-write at ``/output``).
    subject:
        BIDS subject label, e.g. ``sub-0001``.
    sessions_t1ws:
        Ordered mapping of session label → T1w path,
        e.g. ``{"ses-01": Path(...), "ses-02": Path(...)}``.
    threads:
        Number of parallel threads.

    Returns
    -------
    list[str]
        Complete ``apptainer run`` command suitable for :func:`subprocess.run`.
    """
    cmd = [
        "apptainer",
        "exec",
        "--cleanenv",
        "--env",
        "FS_LICENSE=/opt/fs_license.txt",
        "--bind",
        f"{bids_dir}:/data:ro",
        "--bind",
        f"{output_dir}/{subject}:/output",
        "--bind",
        f"{fs_license}:/opt/fs_license.txt:ro",
        str(sif),
        "/fastsurfer/long_fastsurfer.sh",
        "--tid",
        subject,
        "--sd",
        "/output",
        "--3T",
        "--parallel_surf",
        "--threads",
        str(threads),
    ]
    # Append all T1w paths remapped to container space
    cmd += ["--t1s"] + [_remap(t1w, bids_dir, "/data") for t1w in sessions_t1ws.values()]
    # Append all timepoint IDs (bare session labels; subject dir is already the SUBJECTS_DIR)
    cmd += ["--tpids"] + list(sessions_t1ws.keys())
    return cmd


# ---------------------------------------------------------------------------
# CLI entry point (used by scripts/snbb_fastsurfer_helper.py)
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the unified FastSurfer helper.

    Auto-discovers BIDS sessions for the subject and branches:

    * **1 session** → cross-sectional ``run_fastsurfer.sh``
    * **2+ sessions** → longitudinal ``long_fastsurfer.sh``

    Usage example::

        python3 snbb_fastsurfer_helper.py \\
            --bids-dir /data/bids --output-dir /data/derivatives/fastsurfer \\
            --subject sub-0001 \\
            --sif /containers/fastsurfer.sif --fs-license /misc/fs/license.txt
    """
    parser = argparse.ArgumentParser(
        description="FastSurfer helper — cross-sectional or longitudinal.",
    )
    parser.add_argument("--bids-dir", required=True, type=Path,
                        help="BIDS root directory.")
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="FastSurfer SUBJECTS_DIR (derivatives/fastsurfer).")
    parser.add_argument("--subject", required=True,
                        help="BIDS subject label, e.g. sub-0001.")
    parser.add_argument("--sif", required=True, type=Path,
                        help="Path to FastSurfer Apptainer SIF image.")
    parser.add_argument("--fs-license", required=True, type=Path,
                        help="FreeSurfer license file.")
    parser.add_argument("--threads", type=int, default=8,
                        help="Number of parallel threads (default: 8).")

    args = parser.parse_args(argv)

    sessions_t1ws = collect_all_session_t1ws(args.bids_dir, args.subject)
    if not sessions_t1ws:
        print(
            f"ERROR: No sessions with a T1w image found for {args.subject} "
            f"under {args.bids_dir}",
            file=sys.stderr,
        )
        return 1

    (args.output_dir / args.subject).mkdir(parents=True, exist_ok=True)

    if len(sessions_t1ws) == 1:
        session, t1w = next(iter(sessions_t1ws.items()))
        print(f"Single session ({session}): running cross-sectional FastSurfer.")
        cmd = build_cross_apptainer_command(
            sif=args.sif,
            fs_license=args.fs_license,
            bids_dir=args.bids_dir,
            output_dir=args.output_dir,
            subject=args.subject,
            session=session,
            t1w_file=t1w,
            threads=args.threads,
        )
    else:
        sessions = list(sessions_t1ws.keys())
        print(f"Multiple sessions ({sessions}): running longitudinal FastSurfer.")
        cmd = build_long_fastsurfer_command(
            sif=args.sif,
            fs_license=args.fs_license,
            bids_dir=args.bids_dir,
            output_dir=args.output_dir,
            subject=args.subject,
            sessions_t1ws=sessions_t1ws,
            threads=args.threads,
        )

    print(f"Running: {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())

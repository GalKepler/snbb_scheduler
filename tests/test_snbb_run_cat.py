"""T1w selection in scripts/snbb_run_cat.sh — runs the real script on a fake
BIDS session with a stub ``apptainer`` on PATH, and checks which T1w was
staged into the output tree. CAT12 itself never runs, so the script then
fails its own output verification; only the staged input is asserted.
"""

import gzip
import os
import shutil
import struct
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "snbb_run_cat.sh"
BASH = shutil.which("bash")

pytestmark = pytest.mark.skipif(BASH is None, reason="bash not available")

PREFIX = "sub-0001_ses-01"


def _write_t1w(anat: Path, name: str, dims=(4, 4, 4)) -> None:
    """Minimal NIfTI-1 header (+ vox_offset padding), gzipped like real BIDS."""
    hdr = bytearray(352)
    struct.pack_into("<i", hdr, 0, 348)
    struct.pack_into("<8h", hdr, 40, 3, *dims, 1, 1, 1, 1)
    hdr[344:348] = b"n+1\0"
    (anat / name).write_bytes(gzip.compress(bytes(hdr)))


def _run(tmp_path: Path, t1w: dict) -> tuple[subprocess.CompletedProcess, list[str]]:
    """*t1w* maps filename → dims, or → bytes for a corrupt file."""
    anat = tmp_path / "bids" / "sub-0001" / "ses-01" / "anat"
    anat.mkdir(parents=True)
    for name, content in t1w.items():
        if isinstance(content, bytes):
            (anat / name).write_bytes(content)
        else:
            _write_t1w(anat, name, content)

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    stub = stub_dir / "apptainer"
    stub.write_text("#!/bin/sh\nexit 0\n")
    stub.chmod(0o755)

    out = tmp_path / "derivatives" / "cat12"
    env = {
        **os.environ,
        "PATH": f"{stub_dir}{os.pathsep}{os.environ['PATH']}",
        "SNBB_BIDS_ROOT": (tmp_path / "bids").as_posix(),
        "SNBB_CAT_DERIVATIVES": out.as_posix(),
        "SNBB_CAT_SIF": (tmp_path / "cat12.sif").as_posix(),
        "SNBB_DEBUG_LOG": (tmp_path / "logs" / "debug.log").as_posix(),
        "SNBB_CAT_ATLAS_DIR": (tmp_path / "no_atlases").as_posix(),
    }
    proc = subprocess.run(
        [BASH, SCRIPT.as_posix(), "sub-0001", "ses-01"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    out_anat = out / "sub-0001" / "ses-01" / "anat"
    staged = (
        sorted(p.name for p in out_anat.glob("*_T1w.nii*")) if out_anat.is_dir() else []
    )
    return proc, staged


def test_prefers_highest_complete_rec_norm_run(tmp_path):
    _, staged = _run(
        tmp_path,
        {
            f"{PREFIX}_rec-norm_run-01_T1w.nii.gz": (4, 4, 4),
            f"{PREFIX}_acq-defaced_run-01_T1w.nii.gz": (4, 4, 4),
            f"{PREFIX}_rec-norm_run-02_T1w.nii.gz": (4, 4, 4),
        },
    )
    assert staged == [f"{PREFIX}_rec-norm_run-02_T1w.nii.gz"]


def test_skips_repeat_with_different_dimensions(tmp_path):
    _, staged = _run(
        tmp_path,
        {
            f"{PREFIX}_rec-norm_run-01_T1w.nii.gz": (4, 4, 4),
            f"{PREFIX}_rec-norm_run-02_T1w.nii.gz": (4, 4, 2),
        },
    )
    assert staged == [f"{PREFIX}_rec-norm_run-01_T1w.nii.gz"]


def test_uses_most_common_dimensions(tmp_path):
    _, staged = _run(
        tmp_path,
        {
            f"{PREFIX}_rec-norm_run-01_T1w.nii.gz": (4, 4, 4),
            f"{PREFIX}_rec-norm_run-02_T1w.nii.gz": (4, 4, 4),
            f"{PREFIX}_rec-norm_run-03_T1w.nii.gz": (4, 4, 2),
        },
    )
    assert staged == [f"{PREFIX}_rec-norm_run-02_T1w.nii.gz"]


def test_ignores_unreadable_run01(tmp_path):
    _, staged = _run(
        tmp_path,
        {
            f"{PREFIX}_rec-norm_run-01_T1w.nii.gz": b"corrupt",
            f"{PREFIX}_rec-norm_run-02_T1w.nii.gz": (4, 4, 4),
        },
    )
    assert staged == [f"{PREFIX}_rec-norm_run-02_T1w.nii.gz"]


def test_ignores_unreadable_repeat(tmp_path):
    _, staged = _run(
        tmp_path,
        {
            f"{PREFIX}_rec-norm_run-01_T1w.nii.gz": (4, 4, 4),
            f"{PREFIX}_rec-norm_run-02_T1w.nii.gz": b"corrupt",
        },
    )
    assert staged == [f"{PREFIX}_rec-norm_run-01_T1w.nii.gz"]


def test_uses_repeat_when_no_run01(tmp_path):
    _, staged = _run(
        tmp_path,
        {
            f"{PREFIX}_acq-defaced_run-02_T1w.nii.gz": (4, 4, 4),
            f"{PREFIX}_rec-norm_run-02_T1w.nii.gz": (4, 4, 4),
        },
    )
    assert staged == [f"{PREFIX}_rec-norm_run-02_T1w.nii.gz"]


def test_falls_back_to_run01_when_no_header_readable(tmp_path):
    _, staged = _run(
        tmp_path,
        {
            f"{PREFIX}_rec-norm_run-01_T1w.nii.gz": b"",
            f"{PREFIX}_rec-norm_run-02_T1w.nii.gz": b"",
        },
    )
    assert staged == [f"{PREFIX}_rec-norm_run-01_T1w.nii.gz"]


def test_ambiguous_session_still_fails(tmp_path):
    proc, staged = _run(
        tmp_path,
        {
            f"{PREFIX}_acq-defaced_run-01_T1w.nii.gz": (4, 4, 4),
            f"{PREFIX}_run-02_T1w.nii.gz": (4, 4, 4),
        },
    )
    assert proc.returncode == 1
    assert "ambiguous T1w selection" in proc.stderr
    assert staged == []

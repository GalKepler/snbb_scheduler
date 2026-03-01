"""Tests for snbb_scheduler.freesurfer — T1w/T2w collection and command builders."""
from pathlib import Path
from unittest.mock import patch

import pytest

from snbb_scheduler.freesurfer import (
    build_apptainer_command,
    build_cross_sectional_apptainer_command,
    build_cross_sectional_command,
    build_longitudinal_apptainer_command,
    build_longitudinal_command,
    build_native_command,
    build_template_apptainer_command,
    build_template_command,
    collect_all_session_images,
    collect_images,
    collect_session_t1w,
    collect_session_t2w,
    main,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_t1w(bids: Path, subject: str, session: str, name: str = "") -> Path:
    anat = bids / subject / session / "anat"
    anat.mkdir(parents=True, exist_ok=True)
    filename = name or f"{subject}_{session}_T1w.nii.gz"
    p = anat / filename
    p.touch()
    return p


def _make_t2w(bids: Path, subject: str, session: str, name: str = "") -> Path:
    anat = bids / subject / session / "anat"
    anat.mkdir(parents=True, exist_ok=True)
    filename = name or f"{subject}_{session}_T2w.nii.gz"
    p = anat / filename
    p.touch()
    return p


def _touch_done(subjects_dir: Path, subject_id: str) -> None:
    s = subjects_dir / subject_id / "scripts"
    s.mkdir(parents=True, exist_ok=True)
    (s / "recon-all.done").touch()


# ---------------------------------------------------------------------------
# collect_session_t1w
# ---------------------------------------------------------------------------


def test_collect_session_t1w_returns_path(tmp_path):
    bids = tmp_path / "bids"
    p = _make_t1w(bids, "sub-0001", "ses-01")
    result = collect_session_t1w(bids, "sub-0001", "ses-01")
    assert result == p


def test_collect_session_t1w_returns_none_when_missing(tmp_path):
    bids = tmp_path / "bids"
    bids.mkdir()
    assert collect_session_t1w(bids, "sub-0001", "ses-01") is None


def test_collect_session_t1w_excludes_defaced(tmp_path):
    bids = tmp_path / "bids"
    _make_t1w(bids, "sub-0001", "ses-01", "sub-0001_ses-01_acq-defaced_T1w.nii.gz")
    assert collect_session_t1w(bids, "sub-0001", "ses-01") is None


def test_collect_session_t1w_prefers_rec_norm(tmp_path):
    bids = tmp_path / "bids"
    _make_t1w(bids, "sub-0001", "ses-01", "sub-0001_ses-01_T1w.nii.gz")
    norm = _make_t1w(bids, "sub-0001", "ses-01", "sub-0001_ses-01_rec-norm_T1w.nii.gz")
    result = collect_session_t1w(bids, "sub-0001", "ses-01")
    assert result == norm


def test_collect_session_t1w_falls_back_when_no_rec_norm(tmp_path):
    bids = tmp_path / "bids"
    p = _make_t1w(bids, "sub-0001", "ses-01", "sub-0001_ses-01_T1w.nii.gz")
    result = collect_session_t1w(bids, "sub-0001", "ses-01")
    assert result == p


# ---------------------------------------------------------------------------
# collect_session_t2w
# ---------------------------------------------------------------------------


def test_collect_session_t2w_returns_path(tmp_path):
    bids = tmp_path / "bids"
    p = _make_t2w(bids, "sub-0001", "ses-01")
    result = collect_session_t2w(bids, "sub-0001", "ses-01")
    assert result == p


def test_collect_session_t2w_returns_none_when_missing(tmp_path):
    bids = tmp_path / "bids"
    bids.mkdir()
    assert collect_session_t2w(bids, "sub-0001", "ses-01") is None


def test_collect_session_t2w_excludes_defaced(tmp_path):
    bids = tmp_path / "bids"
    _make_t2w(bids, "sub-0001", "ses-01", "sub-0001_ses-01_acq-defaced_T2w.nii.gz")
    assert collect_session_t2w(bids, "sub-0001", "ses-01") is None


def test_collect_session_t2w_prefers_rec_norm(tmp_path):
    bids = tmp_path / "bids"
    _make_t2w(bids, "sub-0001", "ses-01", "sub-0001_ses-01_T2w.nii.gz")
    norm = _make_t2w(bids, "sub-0001", "ses-01", "sub-0001_ses-01_rec-norm_T2w.nii.gz")
    result = collect_session_t2w(bids, "sub-0001", "ses-01")
    assert result == norm


# ---------------------------------------------------------------------------
# collect_all_session_images
# ---------------------------------------------------------------------------


def test_collect_all_session_images_single_session(tmp_path):
    bids = tmp_path / "bids"
    t1 = _make_t1w(bids, "sub-0001", "ses-01")
    result = collect_all_session_images(bids, "sub-0001")
    assert list(result.keys()) == ["ses-01"]
    assert result["ses-01"] == (t1, None)


def test_collect_all_session_images_with_t2w(tmp_path):
    bids = tmp_path / "bids"
    t1 = _make_t1w(bids, "sub-0001", "ses-01")
    t2 = _make_t2w(bids, "sub-0001", "ses-01")
    result = collect_all_session_images(bids, "sub-0001")
    assert result["ses-01"] == (t1, t2)


def test_collect_all_session_images_multi_session(tmp_path):
    bids = tmp_path / "bids"
    t1a = _make_t1w(bids, "sub-0001", "ses-01")
    t1b = _make_t1w(bids, "sub-0001", "ses-02")
    result = collect_all_session_images(bids, "sub-0001")
    assert list(result.keys()) == ["ses-01", "ses-02"]
    assert result["ses-01"] == (t1a, None)
    assert result["ses-02"] == (t1b, None)


def test_collect_all_session_images_skips_sessions_without_t1w(tmp_path):
    bids = tmp_path / "bids"
    _make_t1w(bids, "sub-0001", "ses-01")
    # ses-02 has only anat dir, no T1w
    (bids / "sub-0001" / "ses-02" / "anat").mkdir(parents=True, exist_ok=True)
    result = collect_all_session_images(bids, "sub-0001")
    assert list(result.keys()) == ["ses-01"]


def test_collect_all_session_images_missing_subject(tmp_path):
    bids = tmp_path / "bids"
    bids.mkdir()
    result = collect_all_session_images(bids, "sub-9999")
    assert result == {}


def test_collect_all_session_images_skips_non_session_dirs(tmp_path):
    bids = tmp_path / "bids"
    _make_t1w(bids, "sub-0001", "ses-01")
    # Create a non-ses- directory (should be ignored)
    (bids / "sub-0001" / "extra_dir").mkdir()
    result = collect_all_session_images(bids, "sub-0001")
    assert list(result.keys()) == ["ses-01"]


# ---------------------------------------------------------------------------
# collect_images (legacy across-session API)
# ---------------------------------------------------------------------------


def test_collect_images_returns_t1w_list(tmp_path):
    bids = tmp_path / "bids"
    t1 = _make_t1w(bids, "sub-0001", "ses-01")
    t1w, t2w = collect_images(bids, "sub-0001")
    assert t1 in t1w
    assert t2w == []


def test_collect_images_excludes_defaced(tmp_path):
    bids = tmp_path / "bids"
    _make_t1w(bids, "sub-0001", "ses-01", "sub-0001_ses-01_acq-defaced_T1w.nii.gz")
    t1w, _ = collect_images(bids, "sub-0001")
    assert t1w == []


def test_collect_images_prefers_rec_norm(tmp_path):
    bids = tmp_path / "bids"
    _make_t1w(bids, "sub-0001", "ses-01", "sub-0001_ses-01_T1w.nii.gz")
    norm = _make_t1w(bids, "sub-0001", "ses-01", "sub-0001_ses-01_rec-norm_T1w.nii.gz")
    t1w, _ = collect_images(bids, "sub-0001")
    assert t1w == [norm]


# ---------------------------------------------------------------------------
# build_cross_sectional_command
# ---------------------------------------------------------------------------


def test_build_cross_sectional_command_no_t2w(tmp_path):
    t1w = tmp_path / "T1w.nii.gz"
    t1w.touch()
    cmd = build_cross_sectional_command(
        subject_id="sub-0001",
        output_dir=tmp_path / "freesurfer",
        t1w=t1w,
        t2w=None,
        threads=4,
    )
    assert "recon-all" in cmd
    assert "-subject" in cmd
    assert "sub-0001" in cmd
    assert "-i" in cmd
    assert "-T2" not in cmd
    assert "-T2pial" not in cmd
    assert "-openmp" in cmd
    assert "4" in cmd


def test_build_cross_sectional_command_with_t2w(tmp_path):
    t1w = tmp_path / "T1w.nii.gz"
    t2w = tmp_path / "T2w.nii.gz"
    t1w.touch()
    t2w.touch()
    cmd = build_cross_sectional_command(
        subject_id="sub-0001_ses-01",
        output_dir=tmp_path / "freesurfer",
        t1w=t1w,
        t2w=t2w,
        threads=8,
    )
    assert "-T2" in cmd
    assert "-T2pial" in cmd
    assert str(t2w) in cmd


def test_build_cross_sectional_command_subject_id_format(tmp_path):
    t1w = tmp_path / "T1w.nii.gz"
    t1w.touch()
    cmd = build_cross_sectional_command(
        subject_id="sub-0001_ses-01",
        output_dir=tmp_path / "freesurfer",
        t1w=t1w,
        t2w=None,
        threads=4,
    )
    assert "sub-0001_ses-01" in cmd
    assert "-subject" in cmd


# ---------------------------------------------------------------------------
# build_template_command
# ---------------------------------------------------------------------------


def test_build_template_command_two_sessions(tmp_path):
    cmd = build_template_command(
        subject="sub-0001",
        sessions=["ses-01", "ses-02"],
        output_dir=tmp_path / "freesurfer",
        threads=8,
    )
    assert "recon-all" in cmd
    assert "-base" in cmd
    assert "sub-0001" in cmd
    assert "-tp" in cmd
    assert "sub-0001_ses-01" in cmd
    assert "sub-0001_ses-02" in cmd
    assert cmd.count("-tp") == 2


def test_build_template_command_three_sessions(tmp_path):
    cmd = build_template_command(
        subject="sub-0001",
        sessions=["ses-01", "ses-02", "ses-03"],
        output_dir=tmp_path / "freesurfer",
        threads=4,
    )
    assert cmd.count("-tp") == 3
    assert "sub-0001_ses-03" in cmd


def test_build_template_command_has_parallel_flags(tmp_path):
    cmd = build_template_command(
        subject="sub-0001",
        sessions=["ses-01", "ses-02"],
        output_dir=tmp_path / "freesurfer",
        threads=8,
    )
    assert "-parallel" in cmd
    assert "-openmp" in cmd


# ---------------------------------------------------------------------------
# build_longitudinal_command
# ---------------------------------------------------------------------------


def test_build_longitudinal_command(tmp_path):
    cmd = build_longitudinal_command(
        subject="sub-0001",
        session="ses-01",
        output_dir=tmp_path / "freesurfer",
        threads=8,
    )
    assert "recon-all" in cmd
    assert "-long" in cmd
    # timepoint ID comes right after -long
    long_idx = cmd.index("-long")
    assert cmd[long_idx + 1] == "sub-0001_ses-01"
    # base (template) ID follows timepoint
    assert cmd[long_idx + 2] == "sub-0001"
    assert "-parallel" in cmd
    assert "-openmp" in cmd


# ---------------------------------------------------------------------------
# Apptainer command builders
# ---------------------------------------------------------------------------


@pytest.fixture()
def apptainer_paths(tmp_path):
    sif = tmp_path / "freesurfer.sif"
    sif.touch()
    license_ = tmp_path / "license.txt"
    license_.touch()
    bids = tmp_path / "bids"
    bids.mkdir()
    output = tmp_path / "freesurfer"
    output.mkdir()
    return sif, license_, bids, output


def test_build_cross_sectional_apptainer_command_has_binds(tmp_path, apptainer_paths):
    sif, lic, bids, output = apptainer_paths
    t1w = bids / "sub-0001" / "ses-01" / "anat" / "T1w.nii.gz"
    t1w.parent.mkdir(parents=True)
    t1w.touch()

    cmd = build_cross_sectional_apptainer_command(
        sif=sif,
        fs_license=lic,
        bids_dir=bids,
        output_dir=output,
        subject_id="sub-0001",
        t1w=t1w,
        t2w=None,
        threads=4,
    )
    cmd_str = " ".join(str(c) for c in cmd)
    assert "apptainer" in cmd_str
    assert "/data:ro" in cmd_str
    assert "/output" in cmd_str
    assert "/opt/fs_license.txt:ro" in cmd_str
    assert "recon-all" in cmd_str
    assert "-subject" in cmd_str
    assert "/data/" in cmd_str  # remapped T1w path


def test_build_cross_sectional_apptainer_command_with_t2w(tmp_path, apptainer_paths):
    sif, lic, bids, output = apptainer_paths
    t1w = bids / "sub-0001" / "ses-01" / "anat" / "T1w.nii.gz"
    t2w = bids / "sub-0001" / "ses-01" / "anat" / "T2w.nii.gz"
    t1w.parent.mkdir(parents=True)
    t1w.touch()
    t2w.touch()

    cmd = build_cross_sectional_apptainer_command(
        sif=sif,
        fs_license=lic,
        bids_dir=bids,
        output_dir=output,
        subject_id="sub-0001",
        t1w=t1w,
        t2w=t2w,
        threads=4,
    )
    cmd_str = " ".join(str(c) for c in cmd)
    assert "-T2" in cmd_str
    assert "-T2pial" in cmd_str


def test_build_template_apptainer_command(tmp_path, apptainer_paths):
    sif, lic, bids, output = apptainer_paths
    cmd = build_template_apptainer_command(
        sif=sif,
        fs_license=lic,
        bids_dir=bids,
        output_dir=output,
        subject="sub-0001",
        sessions=["ses-01", "ses-02"],
        threads=8,
    )
    cmd_str = " ".join(str(c) for c in cmd)
    assert "apptainer" in cmd_str
    assert "-base" in cmd_str
    assert "sub-0001" in cmd_str
    assert "sub-0001_ses-01" in cmd_str
    assert "sub-0001_ses-02" in cmd_str
    assert "-tp" in cmd_str


def test_build_longitudinal_apptainer_command(tmp_path, apptainer_paths):
    sif, lic, bids, output = apptainer_paths
    cmd = build_longitudinal_apptainer_command(
        sif=sif,
        fs_license=lic,
        bids_dir=bids,
        output_dir=output,
        subject="sub-0001",
        session="ses-01",
        threads=8,
    )
    cmd_str = " ".join(str(c) for c in cmd)
    assert "apptainer" in cmd_str
    assert "-long" in cmd_str
    assert "sub-0001_ses-01" in cmd_str
    # template ID must appear after the timepoint ID
    long_idx = cmd.index("-long")
    assert cmd[long_idx + 1] == "sub-0001_ses-01"
    assert cmd[long_idx + 2] == "sub-0001"


# ---------------------------------------------------------------------------
# Legacy API (build_native_command, build_apptainer_command)
# ---------------------------------------------------------------------------


def test_build_native_command_multi_t1w(tmp_path):
    t1a = tmp_path / "T1a.nii.gz"
    t1b = tmp_path / "T1b.nii.gz"
    t1a.touch()
    t1b.touch()
    cmd = build_native_command(
        subject="sub-0001",
        output_dir=tmp_path / "freesurfer",
        t1w_files=[t1a, t1b],
        t2w_files=[],
        threads=4,
    )
    assert cmd.count("-i") == 2


def test_build_native_command_uses_first_t2w(tmp_path):
    t1 = tmp_path / "T1.nii.gz"
    t2a = tmp_path / "T2a.nii.gz"
    t2b = tmp_path / "T2b.nii.gz"
    t1.touch()
    t2a.touch()
    t2b.touch()
    cmd = build_native_command(
        subject="sub-0001",
        output_dir=tmp_path / "freesurfer",
        t1w_files=[t1],
        t2w_files=[t2a, t2b],
        threads=4,
    )
    assert str(t2a) in cmd
    assert str(t2b) not in cmd


def test_build_apptainer_command_remaps_paths(tmp_path):
    sif = tmp_path / "fs.sif"
    sif.touch()
    lic = tmp_path / "license.txt"
    lic.touch()
    bids = tmp_path / "bids"
    bids.mkdir()
    output = tmp_path / "freesurfer"
    output.mkdir()
    t1w = bids / "sub-0001" / "ses-01" / "anat" / "T1w.nii.gz"
    t1w.parent.mkdir(parents=True)
    t1w.touch()
    cmd = build_apptainer_command(
        sif=sif,
        fs_license=lic,
        bids_dir=bids,
        output_dir=output,
        subject="sub-0001",
        t1w_files=[t1w],
        t2w_files=[],
        threads=4,
    )
    cmd_str = " ".join(str(c) for c in cmd)
    assert "/data/" in cmd_str
    assert str(t1w) not in cmd_str  # host path should be remapped


# ---------------------------------------------------------------------------
# main() — single-session
# ---------------------------------------------------------------------------


def test_main_single_session_runs_cross_sectional(tmp_path):
    bids = tmp_path / "bids"
    output = tmp_path / "freesurfer"
    _make_t1w(bids, "sub-0001", "ses-01")

    with patch("snbb_scheduler.freesurfer._run", return_value=0) as mock_run:
        rc = main([
            "--bids-dir", str(bids),
            "--output-dir", str(output),
            "--subject", "sub-0001",
            "--threads", "4",
        ])
    assert rc == 0
    assert mock_run.call_count == 1
    label = mock_run.call_args[0][1]
    assert "cross-sectional" in label
    assert "sub-0001" in label


def test_main_single_session_skips_if_done(tmp_path):
    bids = tmp_path / "bids"
    output = tmp_path / "freesurfer"
    _make_t1w(bids, "sub-0001", "ses-01")
    _touch_done(output, "sub-0001")

    with patch("snbb_scheduler.freesurfer._run", return_value=0) as mock_run:
        rc = main([
            "--bids-dir", str(bids),
            "--output-dir", str(output),
            "--subject", "sub-0001",
        ])
    assert rc == 0
    mock_run.assert_not_called()


def test_main_single_session_returns_nonzero_on_failure(tmp_path):
    bids = tmp_path / "bids"
    output = tmp_path / "freesurfer"
    _make_t1w(bids, "sub-0001", "ses-01")

    with patch("snbb_scheduler.freesurfer._run", return_value=1):
        rc = main([
            "--bids-dir", str(bids),
            "--output-dir", str(output),
            "--subject", "sub-0001",
        ])
    assert rc == 1


# ---------------------------------------------------------------------------
# main() — multi-session
# ---------------------------------------------------------------------------


def test_main_multi_session_runs_all_five_steps(tmp_path):
    """2 sessions → 2 cross-sectional + 1 template + 2 longitudinal = 5 calls."""
    bids = tmp_path / "bids"
    output = tmp_path / "freesurfer"
    _make_t1w(bids, "sub-0001", "ses-01")
    _make_t1w(bids, "sub-0001", "ses-02")

    calls = []

    def fake_run(cmd, label):
        calls.append(label)
        return 0

    with patch("snbb_scheduler.freesurfer._run", side_effect=fake_run):
        rc = main([
            "--bids-dir", str(bids),
            "--output-dir", str(output),
            "--subject", "sub-0001",
        ])

    assert rc == 0
    assert len(calls) == 5
    assert any("cross-sectional" in c and "ses-01" in c for c in calls)
    assert any("cross-sectional" in c and "ses-02" in c for c in calls)
    assert any("template" in c for c in calls)
    assert any("longitudinal" in c and "ses-01" in c for c in calls)
    assert any("longitudinal" in c and "ses-02" in c for c in calls)


def test_main_multi_session_skips_completed_cross(tmp_path):
    bids = tmp_path / "bids"
    output = tmp_path / "freesurfer"
    _make_t1w(bids, "sub-0001", "ses-01")
    _make_t1w(bids, "sub-0001", "ses-02")
    # ses-01 cross-sectional is already done
    _touch_done(output, "sub-0001_ses-01")

    calls = []

    def fake_run(cmd, label):
        calls.append(label)
        return 0

    with patch("snbb_scheduler.freesurfer._run", side_effect=fake_run):
        rc = main([
            "--bids-dir", str(bids),
            "--output-dir", str(output),
            "--subject", "sub-0001",
        ])

    assert rc == 0
    # ses-02 cross-sectional, template, ses-01 long, ses-02 long = 4
    assert len(calls) == 4
    assert not any("cross-sectional" in c and "ses-01" in c for c in calls)
    assert any("cross-sectional" in c and "ses-02" in c for c in calls)


def test_main_multi_session_skips_completed_template(tmp_path):
    bids = tmp_path / "bids"
    output = tmp_path / "freesurfer"
    _make_t1w(bids, "sub-0001", "ses-01")
    _make_t1w(bids, "sub-0001", "ses-02")
    # Cross-sectionals done, template done
    _touch_done(output, "sub-0001_ses-01")
    _touch_done(output, "sub-0001_ses-02")
    _touch_done(output, "sub-0001")

    calls = []

    def fake_run(cmd, label):
        calls.append(label)
        return 0

    with patch("snbb_scheduler.freesurfer._run", side_effect=fake_run):
        rc = main([
            "--bids-dir", str(bids),
            "--output-dir", str(output),
            "--subject", "sub-0001",
        ])

    assert rc == 0
    # Only 2 longitudinal remain
    assert len(calls) == 2
    assert all("longitudinal" in c for c in calls)


def test_main_multi_session_all_steps_done_no_runs(tmp_path):
    bids = tmp_path / "bids"
    output = tmp_path / "freesurfer"
    _make_t1w(bids, "sub-0001", "ses-01")
    _make_t1w(bids, "sub-0001", "ses-02")
    _touch_done(output, "sub-0001_ses-01")
    _touch_done(output, "sub-0001_ses-02")
    _touch_done(output, "sub-0001")
    _touch_done(output, "sub-0001_ses-01.long.sub-0001")
    _touch_done(output, "sub-0001_ses-02.long.sub-0001")

    with patch("snbb_scheduler.freesurfer._run", return_value=0) as mock_run:
        rc = main([
            "--bids-dir", str(bids),
            "--output-dir", str(output),
            "--subject", "sub-0001",
        ])

    assert rc == 0
    mock_run.assert_not_called()


def test_main_multi_session_stops_on_cross_failure(tmp_path):
    bids = tmp_path / "bids"
    output = tmp_path / "freesurfer"
    _make_t1w(bids, "sub-0001", "ses-01")
    _make_t1w(bids, "sub-0001", "ses-02")

    calls = []

    def fake_run(cmd, label):
        calls.append(label)
        return 1  # always fail

    with patch("snbb_scheduler.freesurfer._run", side_effect=fake_run):
        rc = main([
            "--bids-dir", str(bids),
            "--output-dir", str(output),
            "--subject", "sub-0001",
        ])

    assert rc == 1
    # Should stop after first failure (ses-01 cross-sectional)
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# main() — error cases
# ---------------------------------------------------------------------------


def test_main_no_t1w_images_returns_error(tmp_path):
    bids = tmp_path / "bids"
    bids.mkdir()
    output = tmp_path / "freesurfer"
    rc = main([
        "--bids-dir", str(bids),
        "--output-dir", str(output),
        "--subject", "sub-9999",
    ])
    assert rc == 1


def test_main_sif_without_fs_license_returns_error(tmp_path):
    bids = tmp_path / "bids"
    _make_t1w(bids, "sub-0001", "ses-01")
    rc = main([
        "--bids-dir", str(bids),
        "--output-dir", str(tmp_path / "freesurfer"),
        "--subject", "sub-0001",
        "--sif", str(tmp_path / "fs.sif"),
        # --fs-license intentionally omitted
    ])
    assert rc == 1


# ---------------------------------------------------------------------------
# main() — with Apptainer SIF
# ---------------------------------------------------------------------------


def test_main_single_session_apptainer_command(tmp_path):
    bids = tmp_path / "bids"
    output = tmp_path / "freesurfer"
    sif = tmp_path / "fs.sif"
    lic = tmp_path / "license.txt"
    sif.touch()
    lic.touch()
    _make_t1w(bids, "sub-0001", "ses-01")

    captured = []

    def fake_run(cmd, label):
        captured.append(cmd)
        return 0

    with patch("snbb_scheduler.freesurfer._run", side_effect=fake_run):
        rc = main([
            "--bids-dir", str(bids),
            "--output-dir", str(output),
            "--subject", "sub-0001",
            "--sif", str(sif),
            "--fs-license", str(lic),
        ])

    assert rc == 0
    assert len(captured) == 1
    cmd_str = " ".join(str(c) for c in captured[0])
    assert "apptainer" in cmd_str
    assert "/data/" in cmd_str  # T1w path remapped


def test_main_multi_session_apptainer_uses_container_commands(tmp_path):
    bids = tmp_path / "bids"
    output = tmp_path / "freesurfer"
    sif = tmp_path / "fs.sif"
    lic = tmp_path / "license.txt"
    sif.touch()
    lic.touch()
    _make_t1w(bids, "sub-0001", "ses-01")
    _make_t1w(bids, "sub-0001", "ses-02")

    captured = []

    def fake_run(cmd, label):
        captured.append(cmd)
        return 0

    with patch("snbb_scheduler.freesurfer._run", side_effect=fake_run):
        rc = main([
            "--bids-dir", str(bids),
            "--output-dir", str(output),
            "--subject", "sub-0001",
            "--sif", str(sif),
            "--fs-license", str(lic),
        ])

    assert rc == 0
    assert len(captured) == 5
    for cmd in captured:
        cmd_str = " ".join(str(c) for c in cmd)
        assert "apptainer" in cmd_str

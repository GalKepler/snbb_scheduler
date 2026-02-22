from pathlib import Path

import pytest

from snbb_scheduler.config import SchedulerConfig


def test_defaults():
    cfg = SchedulerConfig()
    assert cfg.dicom_root == Path("/data/snbb/dicom")
    assert cfg.bids_root == Path("/data/snbb/bids")
    assert cfg.derivatives_root == Path("/data/snbb/derivatives")
    assert cfg.qsiprep_dir == "qsiprep"
    assert cfg.freesurfer_dir == "freesurfer"
    assert cfg.slurm_partition == "normal"
    assert cfg.slurm_account == "snbb"
    assert cfg.state_file == Path("/data/snbb/.scheduler_state.parquet")


def test_derived_paths():
    cfg = SchedulerConfig(derivatives_root=Path("/data/derivatives"))
    assert cfg.qsiprep_root == Path("/data/derivatives/qsiprep")
    assert cfg.freesurfer_root == Path("/data/derivatives/freesurfer")


def test_derived_paths_custom_dirs():
    cfg = SchedulerConfig(
        derivatives_root=Path("/data/derivatives"),
        qsiprep_dir="qsiprep-v1",
        freesurfer_dir="fs7",
    )
    assert cfg.qsiprep_root == Path("/data/derivatives/qsiprep-v1")
    assert cfg.freesurfer_root == Path("/data/derivatives/fs7")


def test_from_yaml(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "dicom_root: /my/dicom\n"
        "slurm_partition: gpu\n"
        "slurm_account: mylab\n"
    )
    cfg = SchedulerConfig.from_yaml(yaml_file)
    assert cfg.dicom_root == Path("/my/dicom")
    assert cfg.slurm_partition == "gpu"
    assert cfg.slurm_account == "mylab"
    # Non-overridden defaults remain
    assert cfg.bids_root == Path("/data/snbb/bids")


def test_from_yaml_all_path_fields(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "dicom_root: /a/dicom\n"
        "bids_root: /a/bids\n"
        "derivatives_root: /a/derivatives\n"
        "state_file: /a/state.parquet\n"
    )
    cfg = SchedulerConfig.from_yaml(yaml_file)
    assert isinstance(cfg.dicom_root, Path)
    assert isinstance(cfg.bids_root, Path)
    assert isinstance(cfg.derivatives_root, Path)
    assert isinstance(cfg.state_file, Path)


def test_from_yaml_empty_file(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("")
    cfg = SchedulerConfig.from_yaml(yaml_file)
    assert cfg.dicom_root == Path("/data/snbb/dicom")

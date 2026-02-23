from pathlib import Path

import pytest

from snbb_scheduler.config import DEFAULT_PROCEDURES, Procedure, SchedulerConfig


# ---------------------------------------------------------------------------
# SchedulerConfig defaults
# ---------------------------------------------------------------------------


def test_defaults():
    cfg = SchedulerConfig()
    assert cfg.dicom_root == Path("/data/snbb/dicom")
    assert cfg.bids_root == Path("/data/snbb/bids")
    assert cfg.derivatives_root == Path("/data/snbb/derivatives")
    assert cfg.slurm_partition == "debug"
    assert cfg.slurm_account == "snbb"
    assert cfg.state_file == Path("/data/snbb/.scheduler_state.parquet")


def test_default_procedures_present():
    cfg = SchedulerConfig()
    names = [p.name for p in cfg.procedures]
    assert "bids" in names
    assert "qsiprep" in names
    assert "freesurfer" in names


# ---------------------------------------------------------------------------
# Procedure dataclass
# ---------------------------------------------------------------------------


def test_procedure_defaults():
    proc = Procedure(name="fmriprep", output_dir="fmriprep", script="snbb_run_fmriprep.sh")
    assert proc.scope == "session"
    assert proc.depends_on == []
    assert proc.completion_marker is None


def test_procedure_subject_scope():
    proc = Procedure(
        name="freesurfer",
        output_dir="freesurfer",
        script="snbb_run_freesurfer.sh",
        scope="subject",
        depends_on=["bids"],
        completion_marker="scripts/recon-all.done",
    )
    assert proc.scope == "subject"
    assert proc.depends_on == ["bids"]
    assert proc.completion_marker == "scripts/recon-all.done"


# ---------------------------------------------------------------------------
# get_procedure_root
# ---------------------------------------------------------------------------


def test_get_procedure_root_bids_uses_bids_root():
    cfg = SchedulerConfig(bids_root=Path("/data/bids"))
    bids = cfg.get_procedure("bids")
    assert cfg.get_procedure_root(bids) == Path("/data/bids")


def test_get_procedure_root_derivatives_procedures():
    cfg = SchedulerConfig(derivatives_root=Path("/data/derivatives"))
    qsiprep = cfg.get_procedure("qsiprep")
    freesurfer = cfg.get_procedure("freesurfer")
    assert cfg.get_procedure_root(qsiprep) == Path("/data/derivatives/qsiprep")
    assert cfg.get_procedure_root(freesurfer) == Path("/data/derivatives/freesurfer")


def test_get_procedure_root_custom_procedure():
    cfg = SchedulerConfig(derivatives_root=Path("/data/derivatives"))
    fmriprep = Procedure(name="fmriprep", output_dir="fmriprep", script="snbb_run_fmriprep.sh")
    assert cfg.get_procedure_root(fmriprep) == Path("/data/derivatives/fmriprep")


# ---------------------------------------------------------------------------
# get_procedure
# ---------------------------------------------------------------------------


def test_get_procedure_known():
    cfg = SchedulerConfig()
    proc = cfg.get_procedure("qsiprep")
    assert proc.name == "qsiprep"


def test_get_procedure_unknown_raises():
    cfg = SchedulerConfig()
    with pytest.raises(KeyError, match="fmriprep"):
        cfg.get_procedure("fmriprep")


# ---------------------------------------------------------------------------
# from_yaml
# ---------------------------------------------------------------------------


def test_from_yaml_overrides_paths(tmp_path):
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
    assert cfg.bids_root == Path("/data/snbb/bids")  # unchanged default


def test_from_yaml_all_path_fields_are_paths(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "dicom_root: /a/dicom\n"
        "bids_root: /a/bids\n"
        "derivatives_root: /a/derivatives\n"
        "state_file: /a/state.parquet\n"
    )
    cfg = SchedulerConfig.from_yaml(yaml_file)
    for attr in ("dicom_root", "bids_root", "derivatives_root", "state_file"):
        assert isinstance(getattr(cfg, attr), Path)


def test_from_yaml_empty_file_uses_defaults(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("")
    cfg = SchedulerConfig.from_yaml(yaml_file)
    assert cfg.dicom_root == Path("/data/snbb/dicom")


def test_from_yaml_custom_procedures(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "procedures:\n"
        "  - name: qsiprep\n"
        "    output_dir: qsiprep\n"
        "    script: snbb_run_qsiprep.sh\n"
        "    scope: session\n"
        "    depends_on: []\n"
        "    completion_marker: null\n"
        "  - name: qsirecon\n"
        "    output_dir: qsirecon\n"
        "    script: snbb_run_qsirecon.sh\n"
        "    scope: session\n"
        "    depends_on: [qsiprep]\n"
        "    completion_marker: null\n"
    )
    cfg = SchedulerConfig.from_yaml(yaml_file)
    assert len(cfg.procedures) == 2
    proc = cfg.procedures[1]
    assert proc.name == "qsirecon"
    assert proc.depends_on == ["qsiprep"]
    assert proc.scope == "session"


def test_from_yaml_procedures_are_procedure_objects(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "procedures:\n"
        "  - name: fmriprep\n"
        "    output_dir: fmriprep\n"
        "    script: snbb_run_fmriprep.sh\n"
    )
    cfg = SchedulerConfig.from_yaml(yaml_file)
    assert all(isinstance(p, Procedure) for p in cfg.procedures)


# ---------------------------------------------------------------------------
# Mutability: procedures list is not shared between instances
# ---------------------------------------------------------------------------


def test_procedures_list_independent_per_instance():
    cfg1 = SchedulerConfig()
    cfg2 = SchedulerConfig()
    cfg1.procedures.append(
        Procedure(name="extra", output_dir="extra", script="extra.sh")
    )
    assert len(cfg2.procedures) == len(DEFAULT_PROCEDURES)


# ---------------------------------------------------------------------------
# Validation — depends_on references
# ---------------------------------------------------------------------------


def test_invalid_depends_on_raises_at_init():
    """depends_on that references an unknown procedure raises ValueError."""
    with pytest.raises(ValueError, match="depends on"):
        SchedulerConfig(
            procedures=[
                Procedure(
                    name="orphan",
                    output_dir="orphan",
                    script="orphan.sh",
                    depends_on=["nonexistent"],
                )
            ]
        )


def test_invalid_depends_on_names_listed_in_error():
    with pytest.raises(ValueError, match="nonexistent"):
        SchedulerConfig(
            procedures=[
                Procedure(
                    name="orphan",
                    output_dir="orphan",
                    script="orphan.sh",
                    depends_on=["nonexistent"],
                )
            ]
        )


def test_valid_depends_on_does_not_raise():
    """A procedure that depends on a known sibling is accepted."""
    SchedulerConfig(
        procedures=[
            Procedure(name="step1", output_dir="step1", script="step1.sh"),
            Procedure(name="step2", output_dir="step2", script="step2.sh", depends_on=["step1"]),
        ]
    )


def test_from_yaml_invalid_depends_on_raises(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "procedures:\n"
        "  - name: orphan\n"
        "    output_dir: orphan\n"
        "    script: orphan.sh\n"
        "    depends_on: [ghost]\n"
    )
    with pytest.raises(ValueError, match="depends on"):
        SchedulerConfig.from_yaml(yaml_file)


# ---------------------------------------------------------------------------
# Validation — malformed YAML
# ---------------------------------------------------------------------------


def test_from_yaml_malformed_raises_value_error(tmp_path):
    yaml_file = tmp_path / "bad.yaml"
    yaml_file.write_text("key: [unclosed bracket\n")
    with pytest.raises(ValueError, match="Invalid YAML"):
        SchedulerConfig.from_yaml(yaml_file)


def test_from_yaml_malformed_error_includes_path(tmp_path):
    yaml_file = tmp_path / "bad.yaml"
    yaml_file.write_text(": bad:\n  - [broken")
    with pytest.raises(ValueError, match=str(yaml_file)):
        SchedulerConfig.from_yaml(yaml_file)

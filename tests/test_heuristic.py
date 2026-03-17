"""Tests for scripts/heuristic.py — dynamic fMRI extraction and all eras.

Uses mock SeqInfo namedtuples built from real dicominfo TSV data
(8 sessions in /mnt/62/Bids/.heudiconv/).
"""
from __future__ import annotations

import importlib
import sys
from collections import namedtuple
from pathlib import Path
from types import ModuleType

import pytest

# ---------------------------------------------------------------------------
# Import heuristic.py from scripts/ (not a package)
# ---------------------------------------------------------------------------

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


@pytest.fixture(scope="module")
def heuristic() -> ModuleType:
    """Import heuristic.py as a module, patching heudiconv.utils if needed."""
    # Create a minimal SeqInfo stub so heuristic.py can import without
    # the real heudiconv package installed in the test environment.
    _SeqInfo = namedtuple(
        "SeqInfo",
        [
            "total_files_till_now",
            "example_dcm_file",
            "series_id",
            "dcm_dir_name",
            "series_files",
            "unspecified",
            "dim1",
            "dim2",
            "dim3",
            "dim4",
            "TR",
            "TE",
            "protocol_name",
            "is_motion_corrected",
            "is_derived",
            "patient_id",
            "study_description",
            "referring_physician_name",
            "series_description",
            "sequence_name",
            "image_type",
            "accession_number",
            "patient_age",
            "patient_sex",
            "date",
            "series_uid",
            "time",
            "custom",
        ],
    )
    # Inject a fake heudiconv.utils module if not available
    try:
        from heudiconv.utils import SeqInfo  # noqa: F401
    except ImportError:
        fake_mod = ModuleType("heudiconv")
        fake_utils = ModuleType("heudiconv.utils")
        fake_utils.SeqInfo = _SeqInfo
        sys.modules["heudiconv"] = fake_mod
        sys.modules["heudiconv.utils"] = fake_utils

    spec = importlib.util.spec_from_file_location(
        "heuristic", _SCRIPTS_DIR / "heuristic.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Helper to build mock SeqInfo
# ---------------------------------------------------------------------------

_DEFAULTS = dict(
    total_files_till_now=0,
    example_dcm_file="x.dcm",
    series_id="1-desc",
    dcm_dir_name="1_desc",
    series_files=1,
    unspecified="",
    dim1=256,
    dim2=256,
    dim3=176,
    dim4=1,
    TR=2.0,
    TE=3.0,
    protocol_name="proto",
    is_motion_corrected=False,
    is_derived=False,
    patient_id="P001",
    study_description="SNBB",
    referring_physician_name="",
    series_description="",
    sequence_name="",
    image_type=("ORIGINAL", "PRIMARY", "M", "ND"),
    accession_number="",
    patient_age="",
    patient_sex="",
    date="20240101",
    series_uid="1.2.3",
    time="120000",
    custom=None,
)


def _seq(series_description: str, series_id: str | None = None, **kw):
    """Build a mock SeqInfo namedtuple with sensible defaults."""
    vals = {**_DEFAULTS, "series_description": series_description}
    if series_id is not None:
        vals["series_id"] = series_id
    vals.update(kw)

    try:
        from heudiconv.utils import SeqInfo

        return SeqInfo(**vals)
    except ImportError:
        SeqInfo = namedtuple("SeqInfo", vals.keys())
        return SeqInfo(**vals)


def _key_template(key):
    """Extract the template string from a create_key tuple."""
    return key[0]


def _find_key(info, substring):
    """Find the first key whose template contains *substring*."""
    for k in info:
        if substring in _key_template(k):
            return k
    return None


def _find_keys(info, substring):
    """Find all keys whose template contains *substring*."""
    return [k for k in info if substring in _key_template(k)]


# ===========================================================================
# Tests: Anatomical
# ===========================================================================


class TestAnatomical:
    def test_t1w_no_norm(self, heuristic):
        s = _seq("T1w_MPRAGE_RL", series_id="7-T1w",
                 image_type=("ORIGINAL", "PRIMARY", "M", "DIS2D"))
        info = heuristic.infotodict([s])
        k = _find_key(info, "_T1w")
        assert k is not None and "rec-norm" not in _key_template(k)
        assert "7-T1w" in info[k]

    def test_t1w_norm(self, heuristic):
        s = _seq("T1w_MPRAGE_RL", series_id="8-T1w",
                 image_type=("ORIGINAL", "PRIMARY", "M", "NORM", "DIS2D"))
        info = heuristic.infotodict([s])
        k = _find_key(info, "rec-norm_run-{item:02d}_T1w")
        assert k is not None
        assert "8-T1w" in info[k]

    def test_t2w_no_norm(self, heuristic):
        s = _seq("T2w_SPC_RL", series_id="9-T2w",
                 image_type=("ORIGINAL", "PRIMARY", "M", "DIS2D"))
        info = heuristic.infotodict([s])
        k = _find_key(info, "_T2w")
        assert k is not None and "rec-norm" not in _key_template(k)
        assert "9-T2w" in info[k]

    def test_t2w_norm(self, heuristic):
        s = _seq("T2w_SPC_RL", series_id="10-T2w",
                 image_type=("ORIGINAL", "PRIMARY", "M", "NORM", "DIS2D"))
        info = heuristic.infotodict([s])
        k = _find_key(info, "rec-norm_run-{item:02d}_T2w")
        assert k is not None
        assert "10-T2w" in info[k]

    def test_flair_dark_fluid(self, heuristic):
        s = _seq("t2_tirm_tra_dark-fluid_FLAIR", series_id="6-flair")
        info = heuristic.infotodict([s])
        k = _find_key(info, "_FLAIR")
        assert "6-flair" in info[k]

    def test_flair_short_variant(self, heuristic):
        s = _seq("t2_tirm_FLAIR", series_id="16-flair")
        info = heuristic.infotodict([s])
        k = _find_key(info, "_FLAIR")
        assert "16-flair" in info[k]

    def test_legacy_mprage_iso1mm(self, heuristic):
        s = _seq("MPRAGE_iso1mm", series_id="1-t1",
                 image_type=("ORIGINAL", "PRIMARY", "M"))
        info = heuristic.infotodict([s])
        k = _find_key(info, "_T1w")
        assert k is not None and "rec-norm" not in _key_template(k)
        assert "1-t1" in info[k]

    def test_legacy_mprage_enchanced(self, heuristic):
        s = _seq("MPRAGE_EnchancedContrast", series_id="2-t1",
                 image_type=("ORIGINAL", "PRIMARY", "M"))
        info = heuristic.infotodict([s])
        k = _find_key(info, "_T1w")
        assert k is not None and "rec-norm" not in _key_template(k)
        assert "2-t1" in info[k]

    def test_legacy_t1_iso1mm(self, heuristic):
        s = _seq("T1_iso1mm", series_id="3-t1",
                 image_type=("ORIGINAL", "PRIMARY", "M"))
        info = heuristic.infotodict([s])
        k = _find_key(info, "_T1w")
        assert "3-t1" in info[k]


# ===========================================================================
# Tests: DWI
# ===========================================================================


class TestDWI:
    def test_new_dwi_ap(self, heuristic):
        s = _seq("dMRI_MB4_185dirs_d15D45_AP", series_id="11-dwi")
        info = heuristic.infotodict([s])
        k = _find_key(info, "dir-AP_run-{item:02d}_dwi")
        assert "11-dwi" in info[k]

    def test_new_dwi_ap_sbref(self, heuristic):
        s = _seq("dMRI_MB4_185dirs_d15D45_AP_SBRef", series_id="10-sbref")
        info = heuristic.infotodict([s])
        k = _find_key(info, "dir-AP_run-{item:02d}_sbref")
        assert "10-sbref" in info[k]

    def test_new_dwi_pa(self, heuristic):
        s = _seq("dMRI_MB4_6dirs_d15D45_PA", series_id="13-dwi")
        info = heuristic.infotodict([s])
        k = _find_key(info, "dir-PA_run-{item:02d}_dwi")
        assert "13-dwi" in info[k]

    def test_new_dwi_pa_sbref(self, heuristic):
        s = _seq("dMRI_MB4_6dirs_d15D45_PA_SBRef", series_id="12-sbref")
        info = heuristic.infotodict([s])
        k = _find_key(info, "dir-PA_run-{item:02d}_sbref")
        assert "12-sbref" in info[k]

    def test_legacy_dwi_ap(self, heuristic):
        s = _seq("ep2d_d15.5D60_MB3_AP", series_id="5-dwi")
        info = heuristic.infotodict([s])
        k = _find_key(info, "dir-AP_run-{item:02d}_dwi")
        assert "5-dwi" in info[k]

    def test_legacy_dwi_pa(self, heuristic):
        s = _seq("ep2d_d15.5D60_MB3_PA", series_id="6-dwi")
        info = heuristic.infotodict([s])
        k = _find_key(info, "dir-PA_run-{item:02d}_dwi")
        assert "6-dwi" in info[k]

    def test_legacy_64dir_original_only(self, heuristic):
        """Only ORIGINAL volumes should be classified; derived (ADC, FA) skipped."""
        original = _seq(
            "ep2d_diff_64dir_iso1.7_S2P2",
            series_id="7-dwi",
            image_type=("ORIGINAL", "PRIMARY", "DIFFUSION", "NONE"),
        )
        derived = _seq(
            "ep2d_diff_64dir_iso1.7_S2P2",
            series_id="8-adc",
            image_type=("DERIVED", "PRIMARY", "DIFFUSION", "ADC"),
        )
        info = heuristic.infotodict([original, derived])
        k = _find_key(info, "dir-AP_run-{item:02d}_dwi")
        assert "7-dwi" in info[k]
        assert "8-adc" not in info[k]

    def test_legacy_64dir_ib100(self, heuristic):
        s = _seq(
            "ep2d_diff_64dir_ib100",
            series_id="3-dwi",
            image_type=("ORIGINAL", "PRIMARY", "DIFFUSION", "NONE"),
        )
        info = heuristic.infotodict([s])
        k = _find_key(info, "dir-AP_run-{item:02d}_dwi")
        assert "3-dwi" in info[k]

    def test_sbref_not_classified_as_bold(self, heuristic):
        """DWI SBRef must never land in a DWI main or fMRI key."""
        s = _seq("dMRI_MB4_185dirs_d15D45_AP_SBRef", series_id="10-sbref")
        info = heuristic.infotodict([s])
        dwi_main = _find_key(info, "dir-AP_run-{item:02d}_dwi")
        assert "10-sbref" not in info[dwi_main]
        # Should not appear in any fMRI key either
        for k in info:
            if "func/" in _key_template(k):
                assert "10-sbref" not in info[k]


# ===========================================================================
# Tests: Field maps
# ===========================================================================


class TestFieldMaps:
    def test_spinecho_ap(self, heuristic):
        s = _seq("SpinEchoFieldMap_AP", series_id="2-fmap")
        info = heuristic.infotodict([s])
        k = _find_key(info, "acq-rest_dir-AP_run-{item:02d}_epi")
        assert k is not None
        assert "2-fmap" in info[k]

    def test_spinecho_pa(self, heuristic):
        s = _seq("SpinEchoFieldMap_PA", series_id="3-fmap")
        info = heuristic.infotodict([s])
        k = _find_key(info, "acq-rest_dir-PA_run-{item:02d}_epi")
        assert k is not None
        assert "3-fmap" in info[k]

    def test_se_rsfmri_fieldmap_ap(self, heuristic):
        s = _seq("SE_rsfMRI_FieldMap_AP", series_id="4-fmap")
        info = heuristic.infotodict([s])
        k = _find_key(info, "acq-rest_dir-AP_run-{item:02d}_epi")
        assert k is not None
        assert "4-fmap" in info[k]

    def test_se_tfmri_fieldmap_ap(self, heuristic):
        s = _seq("SE_tfMRI_FieldMap_AP", series_id="10-fmap")
        info = heuristic.infotodict([s])
        k = _find_key(info, "acq-task_dir-AP_run-{item:02d}_epi")
        assert k is not None
        assert "10-fmap" in info[k]

    def test_se_tfmri_fieldmap_pa(self, heuristic):
        s = _seq("SE_tfMRI_FieldMap_PA", series_id="11-fmap")
        info = heuristic.infotodict([s])
        k = _find_key(info, "acq-task_dir-PA_run-{item:02d}_epi")
        assert k is not None
        assert "11-fmap" in info[k]

    def test_rest_and_task_fmaps_separate(self, heuristic):
        """When both rest and task fieldmaps exist, they go to separate keys."""
        seqinfo = [
            _seq("SpinEchoFieldMap_AP", "1-restAP"),
            _seq("SpinEchoFieldMap_PA", "2-restPA"),
            _seq("SE_tfMRI_FieldMap_AP", "3-taskAP"),
            _seq("SE_tfMRI_FieldMap_PA", "4-taskPA"),
        ]
        info = heuristic.infotodict(seqinfo)
        rest_ap = _find_key(info, "acq-rest_dir-AP")
        task_ap = _find_key(info, "acq-task_dir-AP")
        assert "1-restAP" in info[rest_ap]
        assert "3-taskAP" in info[task_ap]
        assert "1-restAP" not in info[task_ap]
        assert "3-taskAP" not in info[rest_ap]


# ===========================================================================
# Tests: fMRI — current era (dynamic extraction)
# ===========================================================================


class TestFMRICurrent:
    def test_rsfmri_bold(self, heuristic):
        s = _seq("rsfMRI_AP", series_id="5-rest")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-rest_run-{item:02d}_bold")
        assert k is not None
        assert "5-rest" in info[k]

    def test_rsfmri_sbref(self, heuristic):
        s = _seq("rsfMRI_AP_SBRef", series_id="4-rest-sb")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-rest_run-{item:02d}_sbref")
        assert k is not None
        assert "4-rest-sb" in info[k]

    def test_emotionalnback_bold(self, heuristic):
        s = _seq("tfMRI_EmotionalNBack_AP", series_id="15-enb")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-emotionalnback_run-{item:02d}_bold")
        assert k is not None
        assert "15-enb" in info[k]

    def test_emotionalnback_sbref(self, heuristic):
        s = _seq("tfMRI_EmotionalNBack_AP_SBRef", series_id="14-enb-sb")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-emotionalnback_run-{item:02d}_sbref")
        assert k is not None
        assert "14-enb-sb" in info[k]

    def test_bjj2_bold(self, heuristic):
        s = _seq("tfMRI_BJJ2_AP", series_id="13-bjj")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-bjj2_run-{item:02d}_bold")
        assert k is not None
        assert "13-bjj" in info[k]

    def test_climbing2_sbref(self, heuristic):
        s = _seq("tfMRI_Climbing2_AP_SBRef", series_id="14-clmb-sb")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-climbing2_run-{item:02d}_sbref")
        assert k is not None
        assert "14-clmb-sb" in info[k]

    def test_music2_bold(self, heuristic):
        s = _seq("tfMRI_Music2_AP", series_id="17-mus")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-music2_run-{item:02d}_bold")
        assert k is not None
        assert "17-mus" in info[k]

    # ── Video task protocols (previously dropped) ─────────────────────────

    def test_social_dynamic_video_bold(self, heuristic):
        s = _seq("tfMRI_social_dynamic_video_AP", series_id="13-sdv")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-socialdynamicvideo_run-{item:02d}_bold")
        assert k is not None
        assert "13-sdv" in info[k]

    def test_social_dynamic_video_sbref(self, heuristic):
        s = _seq("tfMRI_social_dynamic_video_AP_SBRef", series_id="12-sdv-sb")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-socialdynamicvideo_run-{item:02d}_sbref")
        assert k is not None
        assert "12-sdv-sb" in info[k]

    def test_social_dialogue_video(self, heuristic):
        s = _seq("tfMRI_social_dialogue_video_AP", series_id="15-sdlg")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-socialdialoguevideo_run-{item:02d}_bold")
        assert k is not None
        assert "15-sdlg" in info[k]

    def test_neutral_painting_video(self, heuristic):
        s = _seq("tfMRI_neutral_painting_video_AP", series_id="18-npv")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-neutralpaintingvideo_run-{item:02d}_bold")
        assert k is not None
        assert "18-npv" in info[k]

    def test_neutral_nature_video(self, heuristic):
        s = _seq("tfMRI_neutral_nature_video_AP", series_id="22-nnv")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-neutralnaturevideo_run-{item:02d}_bold")
        assert k is not None
        assert "22-nnv" in info[k]

    def test_academic_documentary_video(self, heuristic):
        s = _seq("tfMRI_academic_documentary_video_AP", series_id="26-adv")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-academicdocumentaryvideo_run-{item:02d}_bold")
        assert k is not None
        assert "26-adv" in info[k]

    def test_academic_lecture_video(self, heuristic):
        s = _seq("tfMRI_academic_lecture_video_AP", series_id="28-alv")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-academiclecturevideo_run-{item:02d}_bold")
        assert k is not None
        assert "28-alv" in info[k]


# ===========================================================================
# Tests: fMRI — CMRR era
# ===========================================================================


class TestFMRICMRR:
    def test_cmrr_rsfmri(self, heuristic):
        s = _seq("cmrr_rsfMRI_MB3_2mm_AP", series_id="5-cmrr-rest")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-rest_run-{item:02d}_bold")
        assert k is not None
        assert "5-cmrr-rest" in info[k]

    def test_cmrr_rsfmri_sbref(self, heuristic):
        s = _seq("cmrr_rsfMRI_MB3_2mm_AP_SBRef", series_id="4-cmrr-rest-sb")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-rest_run-{item:02d}_sbref")
        assert k is not None
        assert "4-cmrr-rest-sb" in info[k]

    def test_cmrr_stories(self, heuristic):
        s = _seq("cmrr_Stories_MB3_AP", series_id="6-stories")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-stories_run-{item:02d}_bold")
        assert k is not None
        assert "6-stories" in info[k]

    def test_cmrr_stories_sbref(self, heuristic):
        s = _seq("cmrr_Stories_MB3_AP_SBRef", series_id="5-stories-sb")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-stories_run-{item:02d}_sbref")
        assert k is not None
        assert "5-stories-sb" in info[k]

    def test_cmrr_lexical(self, heuristic):
        s = _seq("cmrr_Lexical_MB3_AP", series_id="7-lex")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-lexical_run-{item:02d}_bold")
        assert k is not None
        assert "7-lex" in info[k]

    def test_cmrr_nback(self, heuristic):
        s = _seq("cmrr_NBack_MB3_AP", series_id="8-nb")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-nback_run-{item:02d}_bold")
        assert k is not None
        assert "8-nb" in info[k]


# ===========================================================================
# Tests: fMRI — early era
# ===========================================================================


class TestFMRIEarly:
    def test_ep2d_bold_rest(self, heuristic):
        s = _seq("ep2d_bold_200vol", series_id="3-rest")
        info = heuristic.infotodict([s])
        k = _find_key(info, "task-rest_run-{item:02d}_bold")
        assert k is not None
        assert "3-rest" in info[k]


# ===========================================================================
# Tests: ignored series
# ===========================================================================


class TestIgnored:
    def test_localizer_ignored(self, heuristic):
        s = _seq("localizer_3D_2 (9X5X5)", series_id="1-loc")
        info = heuristic.infotodict([s])
        for k, v in info.items():
            assert "1-loc" not in v, f"Localizer should be ignored, found in {_key_template(k)}"

    def test_derived_dwi_ignored(self, heuristic):
        """DWI-derived maps (ADC, FA) without ORIGINAL flag are ignored."""
        s = _seq(
            "ep2d_diff_64dir_iso1.7_S2P2",
            series_id="99-adc",
            image_type=("DERIVED", "PRIMARY", "DIFFUSION", "ADC"),
        )
        info = heuristic.infotodict([s])
        for k, v in info.items():
            assert "99-adc" not in v


# ===========================================================================
# Tests: full session integration (S105615 — includes video tasks + FLAIR)
# ===========================================================================


class TestFullSessionS105615:
    """Reproduce the complete S105615 session to verify end-to-end classification."""

    @pytest.fixture()
    def session_info(self, heuristic):
        seqinfo = [
            _seq("localizer_3D_2 (9X5X5)", "1-loc",
                 image_type=("ORIGINAL", "PRIMARY", "M", "NORM", "DIS2D")),
            _seq("SpinEchoFieldMap_AP", "2-fmAP",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("SpinEchoFieldMap_PA", "3-fmPA",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("rsfMRI_AP_SBRef", "4-restSB",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("rsfMRI_AP", "5-rest1",
                 image_type=("ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAIC")),
            _seq("rsfMRI_AP_SBRef", "6-restSB2",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("rsfMRI_AP", "7-rest2",
                 image_type=("ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAIC")),
            _seq("tfMRI_EmotionalNBack_AP_SBRef", "8-enbSB",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("tfMRI_EmotionalNBack_AP", "9-enb",
                 image_type=("ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAIC")),
            _seq("T1w_MPRAGE_RL", "10-t1",
                 image_type=("ORIGINAL", "PRIMARY", "M", "DIS2D")),
            _seq("T1w_MPRAGE_RL", "11-t1norm",
                 image_type=("ORIGINAL", "PRIMARY", "M", "NORM", "DIS2D")),
            _seq("tfMRI_social_dynamic_video_AP_SBRef", "12-sdvSB",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("tfMRI_social_dynamic_video_AP", "13-sdv",
                 image_type=("ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAIC")),
            _seq("tfMRI_social_dialogue_video_AP_SBRef", "14-sdlgSB",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("tfMRI_social_dialogue_video_AP", "15-sdlg",
                 image_type=("ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAIC")),
            _seq("t2_tirm_FLAIR", "16-flair",
                 image_type=("ORIGINAL", "PRIMARY", "M", "NORM", "DIS2D")),
            _seq("tfMRI_neutral_painting_video_AP_SBRef", "17-npvSB",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("tfMRI_neutral_painting_video_AP", "18-npv1",
                 image_type=("ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAIC")),
            _seq("tfMRI_neutral_painting_video_AP_SBRef", "19-npvSB2",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("tfMRI_neutral_painting_video_AP", "20-npv2",
                 image_type=("ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAIC")),
            _seq("tfMRI_neutral_nature_video_AP_SBRef", "21-nnvSB",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("tfMRI_neutral_nature_video_AP", "22-nnv",
                 image_type=("ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAIC")),
            _seq("T2w_SPC_RL", "23-t2",
                 image_type=("ORIGINAL", "PRIMARY", "M", "DIS2D")),
            _seq("T2w_SPC_RL", "24-t2norm",
                 image_type=("ORIGINAL", "PRIMARY", "M", "NORM", "DIS2D")),
            _seq("tfMRI_academic_documentary_video_AP_SBRef", "25-advSB",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("tfMRI_academic_documentary_video_AP", "26-adv",
                 image_type=("ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAIC")),
            _seq("tfMRI_academic_lecture_video_AP_SBRef", "27-alvSB",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("tfMRI_academic_lecture_video_AP", "28-alv",
                 image_type=("ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAIC")),
            _seq("dMRI_MB4_6dirs_d15D45_PA_SBRef", "29-dwiPASB",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("dMRI_MB4_6dirs_d15D45_PA", "30-dwiPA",
                 image_type=("ORIGINAL", "PRIMARY", "DIFFUSION", "NONE", "MB", "ND", "MOSAIC")),
            _seq("dMRI_MB4_185dirs_d15D45_AP_SBRef", "31-dwiAPSB",
                 image_type=("ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC")),
            _seq("dMRI_MB4_185dirs_d15D45_AP", "32-dwiAP",
                 image_type=("ORIGINAL", "PRIMARY", "DIFFUSION", "NONE", "MB", "ND", "MOSAIC")),
        ]
        return heuristic.infotodict(seqinfo)

    def test_localizer_ignored(self, session_info):
        for k, v in session_info.items():
            assert "1-loc" not in v

    def test_fmap(self, session_info):
        # S105615 has only SpinEchoFieldMap → acq-rest; no SE_tfMRI_FieldMap
        ap = _find_key(session_info, "acq-rest_dir-AP_run-{item:02d}_epi")
        pa = _find_key(session_info, "acq-rest_dir-PA_run-{item:02d}_epi")
        assert ap is not None and pa is not None
        assert "2-fmAP" in session_info[ap]
        assert "3-fmPA" in session_info[pa]
        # No acq-task fmaps expected in this session
        task_ap = _find_key(session_info, "acq-task_dir-AP_run-{item:02d}_epi")
        assert session_info[task_ap] == []

    def test_rest_bold_two_runs(self, session_info):
        k = _find_key(session_info, "task-rest_run-{item:02d}_bold")
        assert "5-rest1" in session_info[k]
        assert "7-rest2" in session_info[k]
        assert len(session_info[k]) == 2

    def test_rest_sbref_two_runs(self, session_info):
        k = _find_key(session_info, "task-rest_run-{item:02d}_sbref")
        assert "4-restSB" in session_info[k]
        assert "6-restSB2" in session_info[k]
        assert len(session_info[k]) == 2

    def test_t1w(self, session_info):
        t1 = _find_key(session_info, "run-{item:02d}_T1w")
        assert t1 is not None and "rec-norm" not in _key_template(t1)
        assert "10-t1" in session_info[t1]
        t1n = _find_key(session_info, "rec-norm_run-{item:02d}_T1w")
        assert "11-t1norm" in session_info[t1n]

    def test_t2w(self, session_info):
        t2 = _find_key(session_info, "run-{item:02d}_T2w")
        assert t2 is not None and "rec-norm" not in _key_template(t2)
        assert "23-t2" in session_info[t2]
        t2n = _find_key(session_info, "rec-norm_run-{item:02d}_T2w")
        assert "24-t2norm" in session_info[t2n]

    def test_flair(self, session_info):
        k = _find_key(session_info, "_FLAIR")
        assert "16-flair" in session_info[k]

    def test_video_tasks_classified(self, session_info):
        """All 6 video task protocols produce correct BIDS keys."""
        expected = {
            "task-socialdynamicvideo": ["13-sdv"],
            "task-socialdialoguevideo": ["15-sdlg"],
            "task-neutralpaintingvideo": ["18-npv1", "20-npv2"],
            "task-neutralnaturevideo": ["22-nnv"],
            "task-academicdocumentaryvideo": ["26-adv"],
            "task-academiclecturevideo": ["28-alv"],
        }
        for task_substr, ids in expected.items():
            k = _find_key(session_info, f"{task_substr}_run-{{item:02d}}_bold")
            assert k is not None, f"Missing key for {task_substr}"
            for sid in ids:
                assert sid in session_info[k], f"{sid} not in {task_substr}"

    def test_neutral_painting_video_repeated_runs(self, session_info):
        """Two runs of neutral_painting_video with distinct SBRefs."""
        bold = _find_key(session_info, "task-neutralpaintingvideo_run-{item:02d}_bold")
        sbref = _find_key(session_info, "task-neutralpaintingvideo_run-{item:02d}_sbref")
        assert len(session_info[bold]) == 2
        assert len(session_info[sbref]) == 2

    def test_dwi(self, session_info):
        ap = _find_key(session_info, "dir-AP_run-{item:02d}_dwi")
        pa = _find_key(session_info, "dir-PA_run-{item:02d}_dwi")
        ap_sb = _find_key(session_info, "dir-AP_run-{item:02d}_sbref")
        pa_sb = _find_key(session_info, "dir-PA_run-{item:02d}_sbref")
        assert "32-dwiAP" in session_info[ap]
        assert "30-dwiPA" in session_info[pa]
        assert "31-dwiAPSB" in session_info[ap_sb]
        assert "29-dwiPASB" in session_info[pa_sb]

    def test_no_double_classification(self, session_info):
        """Every series_id appears in at most one key."""
        all_ids = []
        for v in session_info.values():
            all_ids.extend(v)
        assert len(all_ids) == len(set(all_ids)), (
            f"Duplicate classifications: "
            f"{[x for x in all_ids if all_ids.count(x) > 1]}"
        )

    def test_total_classified(self, session_info):
        """31 of 32 series classified (localizer excluded)."""
        total = sum(len(v) for v in session_info.values())
        assert total == 31


# ===========================================================================
# Tests: helpers
# ===========================================================================


class TestHelpers:
    def test_to_bids_label(self, heuristic):
        assert heuristic._to_bids_label("EmotionalNBack") == "emotionalnback"
        assert heuristic._to_bids_label("social_dynamic_video") == "socialdynamicvideo"
        assert heuristic._to_bids_label("BJJ2") == "bjj2"
        assert heuristic._to_bids_label("Music_Movement1") == "musicmovement1"

    def test_is_sbref(self, heuristic):
        assert heuristic._is_sbref("rsfMRI_AP_SBRef") is True
        assert heuristic._is_sbref("rsfMRI_AP") is False
        assert heuristic._is_sbref("cmrr_Stories_MB3_AP_SBRef") is True

    def test_extract_fmri_task_current(self, heuristic):
        assert heuristic._extract_fmri_task("rsfMRI_AP") == "rest"
        assert heuristic._extract_fmri_task("rsfMRI_AP_SBRef") == "rest"
        assert heuristic._extract_fmri_task("tfMRI_BJJ2_AP") == "bjj2"
        assert heuristic._extract_fmri_task("tfMRI_EmotionalNBack_AP_SBRef") == "emotionalnback"
        assert heuristic._extract_fmri_task("tfMRI_social_dynamic_video_AP") == "socialdynamicvideo"

    def test_extract_fmri_task_cmrr(self, heuristic):
        assert heuristic._extract_fmri_task("cmrr_rsfMRI_MB3_2mm_AP") == "rest"
        assert heuristic._extract_fmri_task("cmrr_Stories_MB3_AP") == "stories"
        assert heuristic._extract_fmri_task("cmrr_NBack_MB3_AP_SBRef") == "nback"

    def test_extract_fmri_task_early(self, heuristic):
        assert heuristic._extract_fmri_task("ep2d_bold_200vol") == "rest"

    def test_extract_fmri_task_none(self, heuristic):
        assert heuristic._extract_fmri_task("localizer_3D_2 (9X5X5)") is None
        assert heuristic._extract_fmri_task("T1w_MPRAGE_RL") is None
        assert heuristic._extract_fmri_task("dMRI_MB4_185dirs_d15D45_AP") is None

    def test_create_key_empty_raises(self, heuristic):
        with pytest.raises(ValueError):
            heuristic.create_key("")
        with pytest.raises(ValueError):
            heuristic.create_key(None)


# ===========================================================================
# Tests: run-{item:02d} in all templates
# ===========================================================================


class TestRunEntity:
    def test_all_keys_have_run_entity(self, heuristic):
        """Every template should include run-{item:02d}."""
        # Process a session with variety of protocols to generate all key types
        seqinfo = [
            _seq("T1w_MPRAGE_RL", "1-t1",
                 image_type=("ORIGINAL", "PRIMARY", "M", "DIS2D")),
            _seq("T1w_MPRAGE_RL", "2-t1n",
                 image_type=("ORIGINAL", "PRIMARY", "M", "NORM", "DIS2D")),
            _seq("T2w_SPC_RL", "3-t2",
                 image_type=("ORIGINAL", "PRIMARY", "M", "DIS2D")),
            _seq("t2_tirm_tra_dark-fluid_FLAIR", "4-flair"),
            _seq("dMRI_MB4_185dirs_d15D45_AP", "5-dwi"),
            _seq("dMRI_MB4_185dirs_d15D45_AP_SBRef", "6-dwisb"),
            _seq("SpinEchoFieldMap_AP", "7-fmap-rest"),
            _seq("SE_tfMRI_FieldMap_AP", "8-fmap-task"),
            _seq("rsfMRI_AP", "9-rest"),
            _seq("rsfMRI_AP_SBRef", "10-restsb"),
            _seq("tfMRI_BJJ2_AP", "11-bjj"),
        ]
        info = heuristic.infotodict(seqinfo)
        for k in info:
            tmpl = _key_template(k)
            assert "run-{item:02d}" in tmpl, f"Missing run entity in: {tmpl}"

"""heuristic.py — heudiconv heuristic for the SNBB neuroimaging pipeline.

Handles three scanner eras:

Early (2015-2019)
    T1w : MPRAGE_iso1mm
    DWI : ep2d_diff_64dir* (ORIGINAL volumes only), ep2d_d15.5D60_MB3_AP/PA
    fMRI: ep2d_bold_200vol (resting-state)

Mid (2020-2022) — CMRR protocols
    T1w : MPRAGE_EnchancedContrast, T1_iso1mm
    fMRI: cmrr_rsfMRI_*, cmrr_Stories_*, cmrr_Lexical_*, cmrr_NBack_*

Current (2023+) — Siemens
    T1w : T1w_MPRAGE_RL
    T2w : T2w_SPC_RL
    FLAIR: t2_tirm_*_FLAIR (both tra_dark-fluid and short variants)
    DWI : dMRI_MB4_185dirs_d15D45_AP, dMRI_MB4_6dirs_d15D45_PA
    fmap: SpinEchoFieldMap_AP/PA, SE_(rs|t)fMRI_FieldMap_AP/PA
    fMRI: (t|rs)fMRI_<TaskName>_AP — dynamically extracted

Task fMRI is handled dynamically — any protocol matching the naming
pattern gets its task name extracted and mapped to a BIDS label
automatically. No code changes are needed for new tasks.

Sequences silently ignored
---------------------------
Localizer, IR-EPI TI series, scanner-derived DWI maps (ADC, FA, ColFA).
"""
from __future__ import annotations

import re
from typing import Optional

from heudiconv.utils import SeqInfo


def create_key(
    template: Optional[str],
    outtype: tuple[str, ...] = ("nii.gz", "json"),
    annotation_classes: None = None,
) -> tuple[str, tuple[str, ...], None]:
    if template is None or not template:
        raise ValueError("Template must be a valid format string")
    return (template, outtype, annotation_classes)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_bids_label(name: str) -> str:
    """Lowercase alphanumeric only (valid BIDS entity value)."""
    return re.sub(r"[^a-zA-Z0-9]", "", name).lower()


def _is_sbref(desc: str) -> bool:
    """True if series_description indicates an SBRef acquisition."""
    return desc.lower().endswith("_sbref")


# Current-era fMRI regex: matches (t|rs)fMRI_<Task>_AP or rsfMRI_AP,
# with optional _SBRef suffix.
_FMRI_RE = re.compile(
    r"(?:t|rs)fMRI_(?:(?P<task>.+?)_)?AP(?:_SBRef)?$"
)


def _extract_fmri_task(desc: str) -> str | None:
    """Return BIDS task label from an fMRI series_description, or None.

    Covers current-era (t|rs)fMRI, CMRR-era cmrr_*, and early-era
    ep2d_bold protocols.
    """
    # CMRR era first — these contain 'fMRI' substrings that would
    # confuse the main regex.
    if desc.lower().startswith("cmrr_"):
        if "rsfmri" in desc.lower():
            return "rest"
        parts = desc.split("_")
        if len(parts) >= 2:
            return _to_bids_label(parts[1])
        return None

    # Current era: (rs|t)fMRI_<Task>_AP(_SBRef)?
    m = _FMRI_RE.search(desc)
    if m:
        task = m.group("task")
        if task is None:
            # rsfMRI_AP — no explicit task name → resting state
            if "rsfMRI" in desc:
                return "rest"
            return None
        return _to_bids_label(task)

    # Early era: ep2d_bold_* → resting state
    if "ep2d_bold" in desc.lower():
        return "rest"

    return None


# ---------------------------------------------------------------------------
# Main heuristic
# ---------------------------------------------------------------------------

def infotodict(
    seqinfo: list[SeqInfo],
) -> dict[tuple[str, tuple[str, ...], None], list]:
    """Heuristic evaluator for the SNBB pipeline.

    Uses a single ``elif`` chain so every series matches at most one key.
    SBRef-specific patterns are always tested before their generic AP/PA
    counterparts to prevent substring collisions.
    """

    # ── Anatomical ────────────────────────────────────────────────────────
    t1w = create_key(
        "{bids_subject_session_dir}/anat/"
        "{bids_subject_session_prefix}_run-{item:02d}_T1w"
    )
    t1w_norm = create_key(
        "{bids_subject_session_dir}/anat/"
        "{bids_subject_session_prefix}_rec-norm_run-{item:02d}_T1w"
    )
    t2w = create_key(
        "{bids_subject_session_dir}/anat/"
        "{bids_subject_session_prefix}_run-{item:02d}_T2w"
    )
    t2w_norm = create_key(
        "{bids_subject_session_dir}/anat/"
        "{bids_subject_session_prefix}_rec-norm_run-{item:02d}_T2w"
    )
    flair = create_key(
        "{bids_subject_session_dir}/anat/"
        "{bids_subject_session_prefix}_run-{item:02d}_FLAIR"
    )

    # ── Diffusion ─────────────────────────────────────────────────────────
    dwi_ap = create_key(
        "{bids_subject_session_dir}/dwi/"
        "{bids_subject_session_prefix}_dir-AP_run-{item:02d}_dwi"
    )
    dwi_pa = create_key(
        "{bids_subject_session_dir}/dwi/"
        "{bids_subject_session_prefix}_dir-PA_run-{item:02d}_dwi"
    )
    dwi_ap_sbref = create_key(
        "{bids_subject_session_dir}/dwi/"
        "{bids_subject_session_prefix}_dir-AP_run-{item:02d}_sbref"
    )
    dwi_pa_sbref = create_key(
        "{bids_subject_session_dir}/dwi/"
        "{bids_subject_session_prefix}_dir-PA_run-{item:02d}_sbref"
    )

    # ── Field maps (spin-echo EPI) ────────────────────────────────────────
    # Two acquisition types: rest (SpinEchoFieldMap, SE_rsfMRI_FieldMap)
    # and task (SE_tfMRI_FieldMap).  bids_post uses the acq label to wire
    # IntendedFor to the correct BOLD files.
    fmap_rest_ap = create_key(
        "{bids_subject_session_dir}/fmap/"
        "{bids_subject_session_prefix}_acq-rest_dir-AP_run-{item:02d}_epi"
    )
    fmap_rest_pa = create_key(
        "{bids_subject_session_dir}/fmap/"
        "{bids_subject_session_prefix}_acq-rest_dir-PA_run-{item:02d}_epi"
    )
    fmap_task_ap = create_key(
        "{bids_subject_session_dir}/fmap/"
        "{bids_subject_session_prefix}_acq-task_dir-AP_run-{item:02d}_epi"
    )
    fmap_task_pa = create_key(
        "{bids_subject_session_dir}/fmap/"
        "{bids_subject_session_prefix}_acq-task_dir-PA_run-{item:02d}_epi"
    )

    # ── Static info dict (fMRI keys added dynamically below) ──────────────
    info: dict[tuple[str, tuple[str, ...], None], list] = {
        t1w: [],
        t1w_norm: [],
        t2w: [],
        t2w_norm: [],
        flair: [],
        dwi_ap: [],
        dwi_pa: [],
        dwi_ap_sbref: [],
        dwi_pa_sbref: [],
        fmap_rest_ap: [],
        fmap_rest_pa: [],
        fmap_task_ap: [],
        fmap_task_pa: [],
    }

    # Dynamic fMRI keys: task_label → (bold_key, sbref_key)
    _fmri_keys: dict[str, tuple] = {}

    def _get_fmri_keys(task_label: str):
        """Return (bold_key, sbref_key) for *task_label*, creating on first use."""
        if task_label not in _fmri_keys:
            bold = create_key(
                "{bids_subject_session_dir}/func/"
                "{bids_subject_session_prefix}"
                "_task-%s_run-{item:02d}_bold" % task_label
            )
            sbref = create_key(
                "{bids_subject_session_dir}/func/"
                "{bids_subject_session_prefix}"
                "_task-%s_run-{item:02d}_sbref" % task_label
            )
            _fmri_keys[task_label] = (bold, sbref)
            info[bold] = []
            info[sbref] = []
        return _fmri_keys[task_label]

    for s in seqinfo:
        p = s.series_description

        # ── 1. Anatomical ─────────────────────────────────────────────────
        if (
            "T1w_MPRAGE" in p
            or "MPRAGE_iso1mm" in p
            or "MPRAGE_EnchancedContrast" in p
            or "T1_iso1mm" in p
        ):
            if "NORM" in s.image_type:
                info[t1w_norm].append(s.series_id)
            else:
                info[t1w].append(s.series_id)

        elif "T2w_SPC" in p:
            if "NORM" in s.image_type:
                info[t2w_norm].append(s.series_id)
            else:
                info[t2w].append(s.series_id)

        elif "FLAIR" in p and "t2_tirm" in p:
            info[flair].append(s.series_id)

        # ── 2. DWI SBRef — before generic DWI ────────────────────────────
        elif _is_sbref(p) and (
            "dmri" in p.lower() or "ep2d_d15.5d60_mb3" in p.lower()
        ):
            if "_ap" in p.lower():
                info[dwi_ap_sbref].append(s.series_id)
            elif "_pa" in p.lower():
                info[dwi_pa_sbref].append(s.series_id)

        # ── 3. DWI main volumes ──────────────────────────────────────────
        elif (
            ("dMRI_MB4_185dirs_d15D45_AP" in p or "ep2d_d15.5D60_MB3_AP" in p)
            and not _is_sbref(p)
        ):
            info[dwi_ap].append(s.series_id)

        elif "ep2d_diff_64dir" in p and "ORIGINAL" in s.image_type:
            info[dwi_ap].append(s.series_id)

        elif (
            ("dMRI_MB4_6dirs_d15D45_PA" in p or "ep2d_d15.5D60_MB3_PA" in p)
            and not _is_sbref(p)
        ):
            info[dwi_pa].append(s.series_id)

        # ── 4. Field maps (spin-echo EPI) ────────────────────────────────
        # Task-specific fieldmaps (SE_tfMRI_FieldMap) → acq-task
        elif "SE_tfMRI_FieldMap_AP" in p:
            info[fmap_task_ap].append(s.series_id)
        elif "SE_tfMRI_FieldMap_PA" in p:
            info[fmap_task_pa].append(s.series_id)

        # Rest / general fieldmaps (SpinEchoFieldMap, SE_rsfMRI_FieldMap) → acq-rest
        elif "SpinEchoFieldMap_AP" in p or "SE_rsfMRI_FieldMap_AP" in p:
            info[fmap_rest_ap].append(s.series_id)
        elif "SpinEchoFieldMap_PA" in p or "SE_rsfMRI_FieldMap_PA" in p:
            info[fmap_rest_pa].append(s.series_id)

        # ── 5. fMRI (dynamic task extraction) ────────────────────────────
        else:
            task = _extract_fmri_task(p)
            if task is not None:
                bold_key, sbref_key = _get_fmri_keys(task)
                if _is_sbref(p):
                    info[sbref_key].append(s.series_id)
                else:
                    info[bold_key].append(s.series_id)

        # Everything else (localizer, IR-EPI TI series, derived DWI maps)
        # is intentionally not matched and will be ignored by heudiconv.

    return info

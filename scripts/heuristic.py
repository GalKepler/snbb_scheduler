"""heuristic.py — heudiconv heuristic for the SNBB neuroimaging pipeline.

Handles both the new (2024+) Siemens protocol naming and the legacy
(pre-2024) naming used at the Ya'acoby Lab scanner.

Protocol name variants per modality
------------------------------------
T1w        : T1w_MPRAGE_RL
T2w        : T2w_SPC_RL
FLAIR      : t2_tirm_tra_dark-fluid_FLAIR
DWI AP     : dMRI_MB4_185dirs_d15D45_AP   (new)
             ep2d_d15.5D60_MB3_AP         (legacy)
             ep2d_diff_64dir_iso1.7_S2P2  (legacy; ORIGINAL volumes only)
DWI PA     : dMRI_MB4_6dirs_d15D45_PA     (new)
             ep2d_d15.5D60_MB3_PA         (legacy)
fmap func  : SpinEchoFieldMap_AP/PA       (new)
             SE_rsfMRI_FieldMap_AP/PA     (legacy)
rsfMRI     : rsfMRI_AP
Task fMRI  : rsfMRI, bjj1-3, climbing1-3, music1-3, movement1-2, emotionalnback
             New scanner prefixes task fMRI with 't': tfMRI_EmotionalNBack_AP

Sequences silently ignored
---------------------------
Localizer, IR-EPI TI series, scanner-derived DWI maps (ADC, FA, ColFA).
"""
from __future__ import annotations

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


def infotodict(
    seqinfo: list[SeqInfo],
) -> dict[tuple[str, tuple[str, ...], None], list]:
    """Heuristic evaluator for the SNBB pipeline.

    Uses a single ``elif`` chain so every series matches at most one key.
    SBRef-specific patterns are always tested before their generic AP/PA
    counterparts to prevent substring collisions.
    """

    # ── Anatomical ────────────────────────────────────────────────────────────
    # Siemens MPRAGE produces two reconstructions: raw (no NORM flag) and
    # bias-field corrected (NORM flag).  BIDS uses the ``rec-`` entity for
    # reconstruction variants; ``rec-norm`` is the accepted label for
    # bias-corrected images.
    t1w = create_key(
        "{bids_subject_session_dir}/anat/{bids_subject_session_prefix}_T1w"
    )
    t1w_norm = create_key(
        "{bids_subject_session_dir}/anat/{bids_subject_session_prefix}_rec-norm_T1w"
    )
    t2w = create_key(
        "{bids_subject_session_dir}/anat/{bids_subject_session_prefix}_T2w"
    )
    t2w_norm = create_key(
        "{bids_subject_session_dir}/anat/{bids_subject_session_prefix}_rec-norm_T2w"
    )
    flair = create_key(
        "{bids_subject_session_dir}/anat/{bids_subject_session_prefix}_FLAIR"
    )

    # ── Diffusion ─────────────────────────────────────────────────────────────
    # The short reverse-PE DWI (6 dirs PA) is placed in fmap/ per BIDS
    # convention — it serves as a fieldmap for DWI distortion correction.
    dwi_ap = create_key(
        "{bids_subject_session_dir}/dwi/{bids_subject_session_prefix}_dir-AP_dwi"
    )
    dwi_pa = create_key(
        "{bids_subject_session_dir}/fmap/{bids_subject_session_prefix}_acq-dwi_dir-PA_epi"
    )
    dwi_ap_sbref = create_key(
        "{bids_subject_session_dir}/dwi/{bids_subject_session_prefix}_dir-AP_sbref"
    )
    dwi_pa_sbref = create_key(
        "{bids_subject_session_dir}/dwi/{bids_subject_session_prefix}_dir-PA_sbref"
    )

    # ── Field maps (spin-echo EPI, for functional distortion correction) ──────
    fmap_ap = create_key(
        "{bids_subject_session_dir}/fmap/{bids_subject_session_prefix}_acq-func_dir-AP_epi"
    )
    fmap_pa = create_key(
        "{bids_subject_session_dir}/fmap/{bids_subject_session_prefix}_acq-func_dir-PA_epi"
    )

    # ── Resting-state fMRI ────────────────────────────────────────────────────
    rest = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-rest_bold"
    )
    rest_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-rest_sbref"
    )

    # ── Task fMRI ─────────────────────────────────────────────────────────────
    bjj1 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-bjj1_bold"
    )
    bjj1_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-bjj1_sbref"
    )
    bjj2 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-bjj2_bold"
    )
    bjj2_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-bjj2_sbref"
    )
    bjj3 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-bjj3_bold"
    )
    bjj3_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-bjj3_sbref"
    )
    climbing1 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-climbing1_bold"
    )
    climbing1_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-climbing1_sbref"
    )
    climbing2 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-climbing2_bold"
    )
    climbing2_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-climbing2_sbref"
    )
    climbing3 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-climbing3_bold"
    )
    climbing3_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-climbing3_sbref"
    )
    music1 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-music1_bold"
    )
    music1_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-music1_sbref"
    )
    music2 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-music2_bold"
    )
    music2_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-music2_sbref"
    )
    music3 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-music3_bold"
    )
    music3_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-music3_sbref"
    )
    movement1 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-movement1_bold"
    )
    movement1_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-movement1_sbref"
    )
    movement2 = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-movement2_bold"
    )
    movement2_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-movement2_sbref"
    )
    emotionalnback = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-emotionalnback_bold"
    )
    emotionalnback_sbref = create_key(
        "{bids_subject_session_dir}/func/{bids_subject_session_prefix}_task-emotionalnback_sbref"
    )

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
        fmap_ap: [],
        fmap_pa: [],
        rest: [],
        rest_sbref: [],
        bjj1: [],
        bjj1_sbref: [],
        bjj2: [],
        bjj2_sbref: [],
        bjj3: [],
        bjj3_sbref: [],
        climbing1: [],
        climbing1_sbref: [],
        climbing2: [],
        climbing2_sbref: [],
        climbing3: [],
        climbing3_sbref: [],
        music1: [],
        music1_sbref: [],
        music2: [],
        music2_sbref: [],
        music3: [],
        music3_sbref: [],
        movement1: [],
        movement1_sbref: [],
        movement2: [],
        movement2_sbref: [],
        emotionalnback: [],
        emotionalnback_sbref: [],
    }

    for s in seqinfo:
        p = s.protocol_name

        # ── Anatomical ────────────────────────────────────────────────────────
        if "T1w_MPRAGE" in p:
            if "NORM" in s.image_type:
                info[t1w_norm].append(s.series_id)
            else:
                info[t1w].append(s.series_id)

        elif "T2w_SPC" in p:
            if "NORM" in s.image_type:
                info[t2w_norm].append(s.series_id)
            else:
                info[t2w].append(s.series_id)

        elif "t2_tirm_tra_dark-fluid_FLAIR" in p:
            info[flair].append(s.series_id)

        # ── DWI SBRef — must come before generic AP/PA checks ─────────────────
        # "dMRI_MB4_185dirs_d15D45_AP" is a substring of "…_AP_SBRef", so the
        # SBRef-specific pattern must be tested first.
        elif "dMRI_MB4_185dirs_d15D45_AP_SBRef" in p:
            info[dwi_ap_sbref].append(s.series_id)

        elif "dMRI_MB4_6dirs_d15D45_PA_SBRef" in p:
            info[dwi_pa_sbref].append(s.series_id)

        # ── DWI main volumes ──────────────────────────────────────────────────
        # For the legacy 64-dir protocol the scanner also writes derived maps
        # (ADC, FA, ColFA) with the same protocol name.  Restrict to ORIGINAL
        # volumes to skip those derived reconstructions.
        elif "dMRI_MB4_185dirs_d15D45_AP" in p:
            info[dwi_ap].append(s.series_id)

        elif "ep2d_d15.5D60_MB3_AP" in p and "SBRef" not in p:
            info[dwi_ap].append(s.series_id)

        elif "ep2d_diff_64dir" in p and "ORIGINAL" in s.image_type:
            info[dwi_ap].append(s.series_id)

        elif "dMRI_MB4_6dirs_d15D45_PA" in p:
            info[dwi_pa].append(s.series_id)

        elif "ep2d_d15.5D60_MB3_PA" in p and "SBRef" not in p:
            info[dwi_pa].append(s.series_id)

        # ── Field maps (spin-echo EPI) ─────────────────────────────────────────
        elif "SpinEchoFieldMap_AP" in p or "SE_rsfMRI_FieldMap_AP" in p:
            info[fmap_ap].append(s.series_id)

        elif "SpinEchoFieldMap_PA" in p or "SE_rsfMRI_FieldMap_PA" in p:
            info[fmap_pa].append(s.series_id)

        # ── Resting-state fMRI — SBRef before generic ─────────────────────────
        elif "rsfMRI_AP_SBRef" in p:
            info[rest_sbref].append(s.series_id)

        elif "rsfMRI_AP" in p:
            info[rest].append(s.series_id)

        # ── Task fMRI — SBRef patterns tested before their generic counterparts ─
        elif "fMRI_BJJ1_AP_SBRef" in p:
            info[bjj1_sbref].append(s.series_id)
        elif "fMRI_BJJ1_AP" in p:
            info[bjj1].append(s.series_id)

        elif "fMRI_BJJ2_AP_SBRef" in p:
            info[bjj2_sbref].append(s.series_id)
        elif "fMRI_BJJ2_AP" in p:
            info[bjj2].append(s.series_id)

        elif "fMRI_BJJ3_AP_SBRef" in p:
            info[bjj3_sbref].append(s.series_id)
        elif "fMRI_BJJ3_AP" in p:
            info[bjj3].append(s.series_id)

        elif "fMRI_Climbing1_AP_SBRef" in p:
            info[climbing1_sbref].append(s.series_id)
        elif "fMRI_Climbing1_AP" in p:
            info[climbing1].append(s.series_id)

        elif "fMRI_Climbing2_AP_SBRef" in p:
            info[climbing2_sbref].append(s.series_id)
        elif "fMRI_Climbing2_AP" in p:
            info[climbing2].append(s.series_id)

        elif "fMRI_Climbing3_AP_SBRef" in p:
            info[climbing3_sbref].append(s.series_id)
        elif "fMRI_Climbing3_AP" in p:
            info[climbing3].append(s.series_id)

        elif "fMRI_Music1_AP_SBRef" in p:
            info[music1_sbref].append(s.series_id)
        elif "fMRI_Music1_AP" in p:
            info[music1].append(s.series_id)

        elif "fMRI_Music2_AP_SBRef" in p:
            info[music2_sbref].append(s.series_id)
        elif "fMRI_Music2_AP" in p:
            info[music2].append(s.series_id)

        elif "fMRI_Music3_AP_SBRef" in p:
            info[music3_sbref].append(s.series_id)
        elif "fMRI_Music3_AP" in p:
            info[music3].append(s.series_id)

        elif "fMRI_Music_Movement1_AP_SBRef" in p:
            info[movement1_sbref].append(s.series_id)
        elif "fMRI_Music_Movement1_AP" in p:
            info[movement1].append(s.series_id)

        elif "fMRI_Music_Movement2_AP_SBRef" in p:
            info[movement2_sbref].append(s.series_id)
        elif "fMRI_Music_Movement2_AP" in p:
            info[movement2].append(s.series_id)

        # Emotional N-Back: accept both 'fMRI_' (old) and 'tfMRI_' (new) prefixes.
        elif "fMRI_EmotionalNBack_AP_SBRef" in p or "tfMRI_EmotionalNBack_AP_SBRef" in p:
            info[emotionalnback_sbref].append(s.series_id)
        elif "fMRI_EmotionalNBack_AP" in p or "tfMRI_EmotionalNBack_AP" in p:
            info[emotionalnback].append(s.series_id)

        # Everything else (localizer, IR-EPI TI series, derived DWI maps) is
        # intentionally not matched and will be ignored by heudiconv.

    return info

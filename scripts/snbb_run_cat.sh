#!/usr/bin/env bash
# snbb_run_cat.sh — CAT12 segmentation + surface metrics via Apptainer
# (standalone MCR build, no MATLAB license needed)
# Called by the scheduler:  sbatch ... snbb_run_cat.sh sub-XXXX ses-YY
#
# The container (SNBB_CAT_SIF) and its invocation contract are owned by the
# bagpipe project (/media/storage/bagpipe) — this script is a deliberately
# independent duplicate, ported from
# /media/storage/bagpipe/src/bagpipe/preprocess/cat12_cohort.py
# (_run_job, _build_combined_add, _ensure_staged, _is_complete) and
# /media/storage/bagpipe/src/bagpipe/core/apptainer.py. If bagpipe changes the
# container or its batch options, this script must be updated by hand — there
# is no shared code path.
#
# CAT12's classic output mode (BIDS.BIDSno=0) writes mri/report/label/surf
# NEXT TO ITS INPUT FILE — there is no working BIDS-redirect field in this
# CAT26 build. So the raw T1w is symlinked into the output tree first, and
# the container is pointed at that symlink; output lands alongside it.
#
# ── Site configuration ────────────────────────────────────────────────────────
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_CAT_DERIVATIVES="${SNBB_CAT_DERIVATIVES:-/media/storage/yalab-dev/snbb_scheduler/derivatives/cat12}"
SNBB_CAT_SIF="${SNBB_CAT_SIF:-/media/storage/bagpipe/outputs/containers/cat12.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/cat/debug_submit.log}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=4:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=2

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001
SESSION="$2"          # e.g. ses-01

ANAT_DIR="${SNBB_BIDS_ROOT}/${SUBJECT}/${SESSION}/anat"
OUT_ANAT_DIR="${SNBB_CAT_DERIVATIVES}/${SUBJECT}/${SESSION}/anat"

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ${SESSION} ==="
    echo "SNBB_BIDS_ROOT:        ${SNBB_BIDS_ROOT}"
    echo "SNBB_CAT_DERIVATIVES:  ${SNBB_CAT_DERIVATIVES}"
    echo "SNBB_CAT_SIF:          ${SNBB_CAT_SIF}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

[[ -d "${ANAT_DIR}" ]] || { echo "ERROR: anat dir not found: ${ANAT_DIR}" >&2; exit 1; }

# ── Select the T1w (mirrors bagpipe's _select_best_t1w) ────────────────────────
# Deliberately uses the non-defaced T1w — CAT12 output must match bagpipe's,
# which never runs on defaced input. Do NOT depend this procedure on `defacing`.
mapfile -t T1W_CANDIDATES < <(find "${ANAT_DIR}" -maxdepth 1 -name '*_T1w.nii*' | sort)

if [[ ${#T1W_CANDIDATES[@]} -eq 0 ]]; then
    echo "ERROR: no T1w found in ${ANAT_DIR}" >&2
    exit 1
fi

T1W=""
for cand in "${T1W_CANDIDATES[@]}"; do
    base="$(basename "${cand}")"
    if [[ "${base}" == *rec-norm* && "${base}" != *acq-defaced* && "${base}" == *run-01* ]]; then
        T1W="${cand}"
        break
    fi
done
if [[ -z "${T1W}" ]]; then
    if [[ ${#T1W_CANDIDATES[@]} -eq 1 ]]; then
        T1W="${T1W_CANDIDATES[0]}"
    else
        echo "ERROR: ambiguous T1w selection (${#T1W_CANDIDATES[@]} candidates, no clear rec-norm/run-01 winner) in ${ANAT_DIR}" >&2
        printf '  %s\n' "${T1W_CANDIDATES[@]}" >&2
        exit 1
    fi
fi

# ── Stage the input at its desired output location ────────────────────────────
mkdir -p "${OUT_ANAT_DIR}"
STAGED="${OUT_ANAT_DIR}/$(basename "${T1W}")"
ln -sfn "${T1W}" "${STAGED}"

# ── Run CAT12 (classic mode + surface/thickness + extra ROI atlases) ──────────
# `-a` overwrites, it does not accumulate — every option must be one argument.
BATCH_LINES="matlabbatch{1}.spm.tools.cat.estwrite.nproc = 1;
matlabbatch{1}.spm.tools.cat.estwrite.output.BIDS.BIDSno = 0;
matlabbatch{1}.spm.tools.cat.estwrite.extopts.admin.lazy = 1;
matlabbatch{1}.spm.tools.cat.estwrite.output.ct.native = 1;
matlabbatch{1}.spm.tools.cat.estwrite.output.ct.warped = 1;
matlabbatch{1}.spm.tools.cat.estwrite.output.pp.native = 1;
matlabbatch{1}.spm.tools.cat.estwrite.output.ROImenu.atlases.neuromorphometrics = 1;
matlabbatch{1}.spm.tools.cat.estwrite.output.ROImenu.atlases.lpba40 = 1;
matlabbatch{1}.spm.tools.cat.estwrite.output.ROImenu.atlases.cobra = 1;
matlabbatch{1}.spm.tools.cat.estwrite.output.ROImenu.atlases.thalamus = 1;
matlabbatch{1}.spm.tools.cat.estwrite.output.ROImenu.atlases.thalamic_nuclei = 1;
matlabbatch{1}.spm.tools.cat.estwrite.output.ROImenu.atlases.suit = 1;"

# --writable-tmpfs: CAT12 needs a writable overlay for its own scratch/report
#   files.
# --cleanenv + --env SHELL=/bin/bash: apptainer otherwise leaks the host's
#   $SHELL into the container; the MCR uses $SHELL to spawn every external
#   system() call, including every CAT_* surface/thickness binary. The host
#   shell (zsh) doesn't exist in the container, so those calls fail with
#   execve ENOENT unless $SHELL is pinned to something that exists inside.
#   --cleanenv also strips DISPLAY/XAUTHORITY, which otherwise make the MCR
#   try (and silently fail, exit 0) to init an AWT desktop.
apptainer run --writable-tmpfs --cleanenv \
    --env SHELL=/bin/bash \
    --bind "${SNBB_BIDS_ROOT}":"${SNBB_BIDS_ROOT}":ro \
    --bind "${SNBB_CAT_DERIVATIVES}":"${SNBB_CAT_DERIVATIVES}" \
    "${SNBB_CAT_SIF}" \
    -a "${BATCH_LINES}" \
    "${STAGED}"

# ── Verify completion ourselves ────────────────────────────────────────────────
# cat_standalone.sh unconditionally exit 0's in standalone mode, regardless of
# whether the underlying MCR executable actually succeeded — so the exit code
# above cannot be trusted. Check the same markers snbb_scheduler's checks.py
# uses, and fail loudly if any are missing.
STEM="$(basename "${STAGED}")"
STEM="${STEM%.nii.gz}"
STEM="${STEM%.nii}"

missing=0
check() {
    if ! compgen -G "$1" > /dev/null; then
        echo "ERROR: expected output missing: $1" >&2
        missing=1
    fi
}
check "${OUT_ANAT_DIR}/report/cat_${STEM}.xml"
check "${OUT_ANAT_DIR}/label/catROI_${STEM}.xml"
check "${OUT_ANAT_DIR}/surf/lh.gyrification.${STEM}"
check "${OUT_ANAT_DIR}/surf/lh.depth.${STEM}"
check "${OUT_ANAT_DIR}/surf/lh.fractaldimension.${STEM}"
check "${OUT_ANAT_DIR}/surf/lh.area.${STEM}"

if [[ "${missing}" -ne 0 ]]; then
    echo "ERROR: CAT12 run for ${SUBJECT} ${SESSION} did not produce all expected output" >&2
    exit 1
fi

echo "CAT12 complete: ${SUBJECT} ${SESSION}"

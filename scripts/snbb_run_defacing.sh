#!/usr/bin/env bash
# snbb_run_defacing_fsl.sh — T1w/T2w defacing via fsl_deface (FSL, no container)
# Called by the scheduler:  sbatch ... snbb_run_defacing_fsl.sh sub-XXXX ses-YY
#
# Applies fsl_deface to every T1w and T2w image in the session's anat/
# directory and writes BIDS-named defaced copies:
#   <stem>_desc-defaced_T1w.nii.gz
# JSON sidecars are copied alongside each defaced image.
# fsl_deface must be on PATH (install FSL or load the FSL module before submitting).
#
# ── Site configuration ────────────────────────────────────────────────────────
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=1:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=4

set -euo pipefail

SUBJECT="$1"
SESSION="$2"

ANAT_DIR="${SNBB_BIDS_ROOT}/${SUBJECT}/${SESSION}/anat"

[[ -d "${ANAT_DIR}" ]] || { echo "ERROR: anat dir not found: ${ANAT_DIR}" >&2; exit 1; }

deface_one() {
    local input="$1"
    local modality="$2"          # T1w | T2w

    local basename
    basename=$(basename "${input}" .nii.gz)
    # Insert desc-defaced before the modality suffix (BIDS convention)
    local stem="${basename%_${modality}}"
    local output="${ANAT_DIR}/${stem}_desc-defaced_${modality}.nii.gz"

    echo "fsl_deface: ${input} → ${output}"
    fsl_deface "${input}" "${output}"

    # Copy JSON sidecar if present
    local json="${ANAT_DIR}/${basename}.json"
    if [[ -f "${json}" ]]; then
        cp "${json}" "${ANAT_DIR}/${stem}_desc-defaced_${modality}.json"
    fi
}

# Deface T1w images (skip already-defaced files)
while IFS= read -r -d '' img; do
    deface_one "${img}" "T1w"
done < <(find "${ANAT_DIR}" -maxdepth 1 -name '*_T1w.nii.gz' ! -name '*desc-*' -print0)

# Deface T2w images
while IFS= read -r -d '' img; do
    deface_one "${img}" "T2w"
done < <(find "${ANAT_DIR}" -maxdepth 1 -name '*_T2w.nii.gz' ! -name '*desc-*' -print0)

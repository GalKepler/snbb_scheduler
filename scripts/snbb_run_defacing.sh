#!/usr/bin/env bash
# snbb_run_defacing.sh — T1w/T2w defacing via fsl_deface (FSL, no container)
# Called by the scheduler:  sbatch ... snbb_run_defacing.sh sub-XXXX ses-YY
#
# Applies fsl_deface to every T1w and T2w image in the session's anat/
# directory and writes BIDS-named defaced copies:
#   <stem>_acq-defaced_T1w.nii.gz
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
    # Insert acq-defaced before run- (BIDS entity order: acq before run).
    # Handles both run-present and run-absent filenames.
    local output
    if [[ "${basename}" =~ _run-([0-9]+)_${modality}$ ]]; then
        local pre_run="${basename%_run-*}"
        local run_tag="run-${BASH_REMATCH[1]}"
        output="${ANAT_DIR}/${pre_run}_acq-defaced_${run_tag}_${modality}.nii.gz"
    else
        local stem="${basename%_${modality}}"
        output="${ANAT_DIR}/${stem}_acq-defaced_${modality}.nii.gz"
    fi

    echo "fsl_deface: ${input} → ${output}"
    fsl_deface "${input}" "${output}"

    # Copy JSON sidecar if present
    local json="${ANAT_DIR}/${basename}.json"
    local output_json="${output%.nii.gz}.json"
    if [[ -f "${json}" ]]; then
        cp "${json}" "${output_json}"
    fi
}

# Deface T1w images (skip already-defaced files)
while IFS= read -r -d '' img; do
    deface_one "${img}" "T1w"
done < <(find "${ANAT_DIR}" -maxdepth 1 -name '*_T1w.nii.gz' ! -name '*acq-*' -print0)

# Deface T2w images
while IFS= read -r -d '' img; do
    deface_one "${img}" "T2w"
done < <(find "${ANAT_DIR}" -maxdepth 1 -name '*_T2w.nii.gz' ! -name '*acq-*' -print0)

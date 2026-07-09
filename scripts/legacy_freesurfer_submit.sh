#!/usr/bin/env bash
# legacy_freesurfer_submit.sh — sbatch legacy_freesurfer_run.sh for every scan
# dir under a legacy root (e.g. Converted_NIfTI_T1/00comp).
#
# Usage: ./legacy_freesurfer_submit.sh /mnt/62/Legacy_Data_Processed/Converted_NIfTI_T1/00comp
#
# Skips dirs whose subject (dir name, "_" stripped) already has
# scripts/recon-all.done in SNBB_FS_OUTPUT.

SNBB_FS_OUTPUT="${SNBB_FS_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer_legacy}"

set -euo pipefail

ROOT="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for scan_dir in "${ROOT}"/*/; do
    scan_dir="${scan_dir%/}"
    subject="$(basename "${scan_dir}" | tr -d '_')"

    if [[ -f "${SNBB_FS_OUTPUT}/${subject}/scripts/recon-all.done" ]]; then
        echo "SKIP: ${subject} already complete."
        continue
    fi

    echo "SUBMIT: ${subject}  (${scan_dir})"
    sbatch --job-name="fs_legacy_${subject}" "${SCRIPT_DIR}/legacy_freesurfer_run.sh" "${scan_dir}"
done

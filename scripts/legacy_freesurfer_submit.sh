#!/usr/bin/env bash
# legacy_freesurfer_submit.sh — sbatch legacy_freesurfer_run.sh for every scan
# dir under a legacy root (e.g. Converted_NIfTI_T1/00comp).
#
# Usage: ./legacy_freesurfer_submit.sh /mnt/62/Legacy_Data_Processed/Converted_NIfTI_T1/00comp
#
# Skips dirs whose subject (dir name, "_" stripped) already has
# scripts/recon-all.done in SNBB_FS_OUTPUT.
#
# Caps concurrent "fs_legacy_*" jobs (pending+running) at SNBB_FS_MAX_CONCURRENT
# (default 4) to avoid oversubscribing node memory (each job requests 16G).

SNBB_FS_OUTPUT="${SNBB_FS_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer_legacy}"
SNBB_FS_MAX_CONCURRENT="${SNBB_FS_MAX_CONCURRENT:-4}"
SNBB_FS_LOG_DIR="${SNBB_FS_LOG_DIR:-/media/storage/yalab-dev/snbb_scheduler/logs/freesurfer_legacy}"

set -euo pipefail

ROOT="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "${SNBB_FS_LOG_DIR}"

wait_for_slot() {
    while [[ "$(squeue -h -u "$(whoami)" --format='%j' 2>/dev/null | grep -c '^fs_legacy_')" -ge "${SNBB_FS_MAX_CONCURRENT}" ]]; do
        sleep 30
    done
}

for scan_dir in "${ROOT}"/*/; do
    scan_dir="${scan_dir%/}"
    subject="$(basename "${scan_dir}" | tr -d '_')"

    if [[ -f "${SNBB_FS_OUTPUT}/${subject}/scripts/recon-all.done" ]]; then
        echo "SKIP: ${subject} already complete."
        continue
    fi

    wait_for_slot
    echo "SUBMIT: ${subject}  (${scan_dir})"
    sbatch --job-name="fs_legacy_${subject}" \
        --output="${SNBB_FS_LOG_DIR}/${subject}_%j.out" \
        --error="${SNBB_FS_LOG_DIR}/${subject}_%j.err" \
        "${SCRIPT_DIR}/legacy_freesurfer_run.sh" "${scan_dir}"
done

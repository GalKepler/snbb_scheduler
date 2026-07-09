#!/usr/bin/env bash
# legacy_freesurfer_run.sh — cross-sectional recon-all for one legacy scan dir.
# Standalone, NOT part of snbb_scheduler package/rules — for one-off processing
# of non-BIDS legacy data (e.g. Converted_NIfTI_T1/00comp).
#
# Usage (direct):  ./legacy_freesurfer_run.sh /path/to/00comp/20160505_1214
# Usage (sbatch):  sbatch ... legacy_freesurfer_run.sh /path/to/00comp/20160505_1214
#
# Subject ID = scan dir name with "_" stripped (YYYYMMDDHHMM).
# Picks the largest *T1w*.nii.gz in the dir if more than one is present.
# Skips already-completed subjects (scripts/recon-all.done present).

# ── Site configuration ────────────────────────────────────────────────────────
SNBB_FS_OUTPUT="${SNBB_FS_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer_legacy}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FREESURFER_SIF="${SNBB_FREESURFER_SIF:-/media/storage/apptainer/images/freesurfer-8.1.0.sif}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=24:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=8

set -euo pipefail

SCAN_DIR="$1"          # e.g. /mnt/62/Legacy_Data_Processed/Converted_NIfTI_T1/00comp/20160505_1214
SCAN_DIR="${SCAN_DIR%/}"
SUBJECT="$(basename "${SCAN_DIR}" | tr -d '_')"   # 20160505_1214 -> 201605051214

T1W="$(find "${SCAN_DIR}" -maxdepth 1 -iname '*T1w*.nii.gz' -printf '%s %p\n' | sort -rn | head -1 | cut -d' ' -f2-)"
if [[ -z "${T1W}" ]]; then
    echo "ERROR: no T1w nii.gz found in ${SCAN_DIR}" >&2
    exit 1
fi

mkdir -p "${SNBB_FS_OUTPUT}"

if [[ -f "${SNBB_FS_OUTPUT}/${SUBJECT}/scripts/recon-all.done" ]]; then
    echo "SKIP: ${SUBJECT} already complete." >&2
    exit 0
fi

apptainer run \
    --env FS_LICENSE=/opt/fs_license.txt \
    --bind "${SCAN_DIR}:/data:ro" \
    --bind "${SNBB_FS_OUTPUT}:/output" \
    --bind "${SNBB_FS_LICENSE}:/opt/fs_license.txt:ro" \
    "${SNBB_FREESURFER_SIF}" \
    recon-all \
        -subject "${SUBJECT}" \
        -sd /output \
        -i "/data/$(basename "${T1W}")" \
        -all \
        -parallel \
        -openmp "${SLURM_CPUS_PER_TASK:-8}"

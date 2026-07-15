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
# Optional: root of local scratch on compute nodes.
# When set, scan input and FreeSurfer output are staged locally, then rsynced
# back to the remote destination on success.
# Leave empty (default) to use remote paths directly.
SNBB_LOCAL_TMP_ROOT="${SNBB_LOCAL_TMP_ROOT:-}"
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

if [[ -n "${SNBB_LOCAL_TMP_ROOT}" ]]; then
    # ── Local-scratch mode ────────────────────────────────────────────────────
    # Stage scan input and FreeSurfer output on the compute node's local disk.
    # On any exit (success, error, or SIGTERM) the local workdir is cleaned up,
    # except when rsync-out fails — in that case the local output is preserved
    # for manual recovery.

    LOCAL_WORKDIR="${SNBB_LOCAL_TMP_ROOT}/snbb_legacyfs_${SLURM_JOB_ID:-$$}_${SUBJECT}"
    LOCAL_DATA="${LOCAL_WORKDIR}/data"
    LOCAL_OUTPUT="${LOCAL_WORKDIR}/output"
    CLEANUP_ON_EXIT=true

    _cleanup() {
        if [[ "${CLEANUP_ON_EXIT}" == "true" ]]; then
            echo "Cleaning up local workdir: ${LOCAL_WORKDIR}" >&2
            rm -rf "${LOCAL_WORKDIR}"
        else
            echo "Preserving local workdir for recovery: ${LOCAL_WORKDIR}" >&2
        fi
    }
    trap _cleanup EXIT

    mkdir -p "${LOCAL_DATA}" "${LOCAL_OUTPUT}"

    rsync -a "${SCAN_DIR}/" "${LOCAL_DATA}/"

    apptainer run \
        --cleanenv \
        --env FS_LICENSE=/opt/fs_license.txt \
        --bind "${LOCAL_DATA}:/data:ro" \
        --bind "${LOCAL_OUTPUT}:/output" \
        --bind "${SNBB_FS_LICENSE}:/opt/fs_license.txt:ro" \
        "${SNBB_FREESURFER_SIF}" \
        recon-all \
            -subject "${SUBJECT}" \
            -sd /output \
            -i "/data/$(basename "${T1W}")" \
            -all \
            -parallel \
            -openmp "${SLURM_CPUS_PER_TASK:-8}"

    CLEANUP_ON_EXIT=false
    # -L: dereference symlinks (dest FS may not support them, e.g. CIFS/NFS -> ENOTSUP)
    # fsaverage: symlink to container's shared atlas dir, dangling on the host, not subject output.
    rsync -avL --exclude=fsaverage "${LOCAL_OUTPUT}/" "${SNBB_FS_OUTPUT}/" || {
        echo "ERROR: rsync to remote destination failed. Local output preserved at ${LOCAL_OUTPUT}" >&2
        exit 1
    }
    CLEANUP_ON_EXIT=true
    # ─────────────────────────────────────────────────────────────────────────
else
    # ── Remote filesystem mode ────────────────────────────────────────────────
    apptainer run \
        --cleanenv \
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
    # ─────────────────────────────────────────────────────────────────────────
fi

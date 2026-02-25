#!/usr/bin/env bash
# snbb_run_freesurfer.sh — FreeSurfer recon-all via Apptainer container
# Called by the snbb_scheduler as:  sbatch ... snbb_run_freesurfer.sh sub-XXXX ses-YY
#
# Runs recon-all inside a FreeSurfer Apptainer container. The helper script
# snbb_recon_all_helper.py globbs all T1w (and T2w) NIfTI files for the
# subject across all BIDS sessions and builds the -i argument list.
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_FS_OUTPUT="${SNBB_FS_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FREESURFER_SIF="${SNBB_FREESURFER_SIF:-/media/storage/apptainer/images/freesurfer-8.1.0.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/freesurfer/debug_submit.log}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=24:00:00
#SBATCH --mem=20G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001  ($2 = session, ignored — FreeSurfer is subject-scoped)

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ==="
    echo "SNBB_BIDS_ROOT:      ${SNBB_BIDS_ROOT}"
    echo "SNBB_FS_OUTPUT:      ${SNBB_FS_OUTPUT}"
    echo "SNBB_FS_LICENSE:     ${SNBB_FS_LICENSE}"
    echo "SNBB_FREESURFER_SIF: ${SNBB_FREESURFER_SIF}"
    echo "PATH:                ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

mkdir -p "${SNBB_FS_OUTPUT}"

python3 "$(dirname "$0")/snbb_recon_all_helper.py" \
    --bids-dir    "${SNBB_BIDS_ROOT}" \
    --output-dir  "${SNBB_FS_OUTPUT}" \
    --subject     "${SUBJECT}" \
    --threads     "${SLURM_CPUS_PER_TASK:-8}" \
    --sif         "${SNBB_FREESURFER_SIF}" \
    --fs-license  "${SNBB_FS_LICENSE}"

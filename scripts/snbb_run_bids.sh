#!/usr/bin/env bash
# snbb_run_bids.sh — DICOM → BIDS conversion via heudiconv (Apptainer)
# Called by the snbb_scheduler as:  sbatch ... snbb_run_bids.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_DICOM_ROOT="${SNBB_DICOM_ROOT:-/data/snbb/dicom}"
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/data/snbb/bids}"
SNBB_HEURISTIC="${SNBB_HEURISTIC:-/data/snbb/heuristic.py}"
SNBB_HEUDICONV_SIF="${SNBB_HEUDICONV_SIF:-/data/containers/heudiconv.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/data/snbb/logs/bids/debug_submit.log}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=4:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=4

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001
PARTICIPANT="${SUBJECT#sub-}"   # strip prefix → 0001

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ==="
    echo "SNBB_DICOM_ROOT:  ${SNBB_DICOM_ROOT}"
    echo "SNBB_BIDS_ROOT:   ${SNBB_BIDS_ROOT}"
    echo "SNBB_HEURISTIC:   ${SNBB_HEURISTIC}"
    echo "SNBB_HEUDICONV_SIF: ${SNBB_HEUDICONV_SIF}"
    echo "PATH:             ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

apptainer run --cleanenv \
    --bind "${SNBB_DICOM_ROOT}":"${SNBB_DICOM_ROOT}":ro \
    --bind "${SNBB_BIDS_ROOT}":"${SNBB_BIDS_ROOT}" \
    --bind "${SNBB_HEURISTIC}":"${SNBB_HEURISTIC}":ro \
    "${SNBB_HEUDICONV_SIF}" \
    --files "${SNBB_DICOM_ROOT}" \
    --outdir "${SNBB_BIDS_ROOT}" \
    --heuristic "${SNBB_HEURISTIC}" \
    --subjects "${PARTICIPANT}" \
    --converter dcm2niix \
    --bids \
    --overwrite

#!/usr/bin/env bash
# snbb_run_defacing.sh — T1w/T2w defacing via bidsonym (Apptainer)
# Called by the snbb_scheduler as:  sbatch ... snbb_run_defacing.sh sub-XXXX ses-YY
#
# bidsonym applies pydeface to the anatomical images and writes defaced
# copies using the desc-defaced BIDS entity (e.g. *_desc-defaced_T1w.nii.gz).
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_BIDSONYM_SIF="${SNBB_BIDSONYM_SIF:-/media/storage/apptainer/images/bidsonym-0.4.0.sif}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=1:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=4

set -euo pipefail

SUBJECT="$1"   # e.g. sub-0001
SESSION="$2"   # e.g. ses-202602161208

PARTICIPANT="${SUBJECT#sub-}"  # strip prefix → 0001
SESSION_ID="${SESSION#ses-}"   # strip prefix → 202602161208

apptainer run --cleanenv \
    --bind "${SNBB_BIDS_ROOT}":"${SNBB_BIDS_ROOT}" \
    "${SNBB_BIDSONYM_SIF}" \
    "${SNBB_BIDS_ROOT}" participant \
    --participant_label "${PARTICIPANT}" \
    --ses "${SESSION_ID}" \
    --deid pydeface \
    --del_nodeface

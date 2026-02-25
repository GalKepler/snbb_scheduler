#!/usr/bin/env python3
"""snbb_bids_post.py — BIDS fieldmap post-processing for the SNBB pipeline.

For a given subject/session this script:

1. Derives the fmap EPI from the PA DWI: globs dwi/*_dir-PA_dwi.nii.gz, computes
   the mean b0, and writes fmap/*_acq-dwi_dir-PA_epi.nii.gz + JSON sidecar.
2. Adds IntendedFor fields to all fmap JSON sidecars:
     acq-dwi  fmaps → dwi/*_dir-AP_dwi.nii.gz  (AP only; PA is the fmap source)
     acq-func fmaps → func/*_bold.nii.gz
3. Hides spurious .bvec/.bval files in fmap/ by renaming them with a leading dot.

Usage:
    python snbb_bids_post.py <subject> <session> <bids_root> [--dry-run]
    e.g.: python snbb_bids_post.py sub-0001 ses-202602161208 /data/snbb/bids
"""
from __future__ import annotations

import argparse
import json
import shutil
import stat
import sys
from pathlib import Path
from typing import Any

import nibabel as nib
import numpy as np


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------


def _run_step(
    step_func,
    step_name: str,
    results: dict[str, Any],
    *args,
    **kwargs,
) -> None:
    """Run a post-processing step and record its results."""
    try:
        step_result = step_func(*args, **kwargs)
        results[step_name] = step_result
        if not step_result["success"]:
            results["errors"].extend(step_result.get("errors", []))
            results["success"] = False
    except Exception as e:
        results["errors"].append(
            f"{step_name.replace('_', ' ').capitalize()} failed: {e}"
        )
        results["success"] = False


# ---------------------------------------------------------------------------
# Step 1: derive fmap EPI from dir-PA DWI
# ---------------------------------------------------------------------------


def _pa_dwi_to_fmap_stem(pa_stem: str) -> str:
    """Transform ``*_dir-PA_dwi`` stem → ``*_acq-dwi_dir-PA_epi`` stem."""
    return pa_stem.replace("_dir-PA_dwi", "_acq-dwi_dir-PA_epi")


def derive_fmap_from_dwi_pa(
    participant_dir: Path,
    session: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Derive fmap/*_acq-dwi_dir-PA_epi.nii.gz from dwi/*_dir-PA_dwi.nii.gz.

    For each PA DWI NIfTI found in dwi/:
    - Reads companion .bval to identify b0 volumes (bval < 100 s/mm²).
    - Computes the mean b0 image (3-D).
    - Writes the result to fmap/*_acq-dwi_dir-PA_epi.nii.gz.
    - Copies the companion .json sidecar to fmap/*_acq-dwi_dir-PA_epi.json.
    """
    results: dict[str, Any] = {
        "success": True,
        "derived_files": [],
        "errors": [],
        "dry_run": dry_run,
    }

    dwi_dir = participant_dir / "dwi"
    if not dwi_dir.exists():
        results["success"] = False
        results["errors"].append(f"DWI directory not found: {dwi_dir}")
        return results

    pa_niftis = list(dwi_dir.glob("*_dir-PA_dwi.nii.gz"))
    if not pa_niftis:
        results["success"] = False
        results["errors"].append("No *_dir-PA_dwi.nii.gz files found in dwi/")
        return results

    fmap_dir = participant_dir / "fmap"

    for pa_nii in pa_niftis:
        stem = pa_nii.name.replace(".nii.gz", "")
        fmap_stem = _pa_dwi_to_fmap_stem(stem)
        fmap_nii = fmap_dir / f"{fmap_stem}.nii.gz"
        fmap_json = fmap_dir / f"{fmap_stem}.json"

        # Locate companion files
        bval_path = dwi_dir / f"{stem}.bval"
        json_path = dwi_dir / f"{stem}.json"

        if dry_run:
            results["derived_files"].append(
                {"source": pa_nii.name, "output": fmap_nii.name, "note": "dry-run"}
            )
            continue

        # Load NIfTI
        try:
            img = nib.load(pa_nii)
        except Exception as e:
            results["errors"].append(f"Failed to load {pa_nii.name}: {e}")
            results["success"] = False
            continue

        data = np.asarray(img.dataobj)

        # Identify b0 volumes
        if bval_path.exists():
            bvals = np.fromstring(bval_path.read_text(), sep=" ")
            b0_mask = bvals < 100
        else:
            # No bval — treat all volumes as b0
            b0_mask = np.ones(data.shape[-1], dtype=bool) if data.ndim == 4 else np.array([True])

        if data.ndim == 4 and b0_mask.any():
            mean_b0 = np.mean(data[..., b0_mask], axis=3)
        elif data.ndim == 3:
            mean_b0 = data
        else:
            results["errors"].append(f"No b0 volumes found in {pa_nii.name}")
            results["success"] = False
            continue

        # Write derived fmap NIfTI
        fmap_dir.mkdir(parents=True, exist_ok=True)
        out_img = nib.Nifti1Image(mean_b0, img.affine, img.header)
        try:
            nib.save(out_img, fmap_nii)
        except Exception as e:
            results["errors"].append(f"Failed to write {fmap_nii.name}: {e}")
            results["success"] = False
            continue

        # Copy JSON sidecar
        if json_path.exists():
            try:
                shutil.copy2(json_path, fmap_json)
            except Exception as e:
                results["errors"].append(f"Failed to copy JSON sidecar for {pa_nii.name}: {e}")
                results["success"] = False
                continue
        else:
            # Write a minimal JSON so IntendedFor can be added in Step 2
            try:
                fmap_json.write_text("{}\n")
            except Exception as e:
                results["errors"].append(f"Failed to create JSON for {fmap_nii.name}: {e}")
                results["success"] = False
                continue

        results["derived_files"].append(
            {"source": pa_nii.name, "output": fmap_nii.name}
        )

    return results


# ---------------------------------------------------------------------------
# Step 2: add IntendedFor to fmap JSONs
# ---------------------------------------------------------------------------


def _find_dwi_targets(participant_dir: Path) -> list[Path]:
    dwi_dir = participant_dir / "dwi"
    if not dwi_dir.exists():
        return []
    # AP only — PA is the source of the fmap, not a target
    return [f for f in dwi_dir.glob("*_dwi.nii.gz") if "dir-PA" not in f.name]


def _find_func_targets(participant_dir: Path) -> list[Path]:
    func_dir = participant_dir / "func"
    return list(func_dir.glob("*_bold.nii.gz")) if func_dir.exists() else []


def _build_intended_for_path(
    target_file: Path,
    participant_dir: Path,
    session: str | None = None,
) -> str:
    """Return a BIDS-compliant IntendedFor path relative to the subject dir.

    Format: ``ses-<id>/<datatype>/<filename>`` (pre-BIDS-1.7 convention).
    """
    try:
        rel_path = target_file.relative_to(participant_dir)
        if session:
            return f"ses-{session}/{rel_path}"
        return str(rel_path)
    except ValueError:
        return target_file.name


def _make_writable(path: Path) -> None:
    mode = path.stat().st_mode
    if not (mode & stat.S_IWUSR):
        path.chmod(mode | stat.S_IWUSR)


def _read_json(path: Path) -> dict | None:
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"  ERROR reading {path}: {e}", file=sys.stderr)
        return None


def _write_json(path: Path, data: dict) -> bool:
    try:
        _make_writable(path)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        print(f"  ERROR writing {path}: {e}", file=sys.stderr)
        return False


def _process_single_fmap_json(
    fmap_json: Path,
    participant_dir: Path,
    session: str | None,
    dry_run: bool,
    results: dict[str, Any],
) -> None:
    filename = fmap_json.name
    if "acq-dwi" in filename:
        target_files = _find_dwi_targets(participant_dir)
        acq_type = "DWI"
    elif "acq-func" in filename:
        target_files = _find_func_targets(participant_dir)
        acq_type = "functional"
    else:
        results["errors"].append(f"Unknown acquisition type in {filename}")
        return

    if not target_files:
        results["errors"].append(f"No target files found for {filename}")
        return

    intended_for = [
        _build_intended_for_path(t, participant_dir, session) for t in target_files
    ]

    if dry_run:
        results["updated_files"].append(
            {"file": filename, "type": acq_type, "targets": intended_for, "note": "dry-run"}
        )
        return

    data = _read_json(fmap_json)
    if data is None:
        results["errors"].append(f"Failed to read {filename}")
        results["success"] = False
        return

    data["IntendedFor"] = intended_for
    if _write_json(fmap_json, data):
        results["updated_files"].append(
            {"file": filename, "type": acq_type, "targets": intended_for}
        )
    else:
        results["errors"].append(f"Failed to update {filename}")
        results["success"] = False


def add_intended_for_to_fmaps(
    participant_dir: Path,
    session: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Add IntendedFor fields to all *_epi.json files in fmap/."""
    results: dict[str, Any] = {
        "success": True,
        "updated_files": [],
        "errors": [],
        "dry_run": dry_run,
    }

    fmap_dir = participant_dir / "fmap"
    if not fmap_dir.exists():
        results["success"] = False
        results["errors"].append(f"Fieldmap directory not found: {fmap_dir}")
        return results

    fmap_jsons = list(fmap_dir.glob("*_epi.json"))
    if not fmap_jsons:
        results["success"] = False
        results["errors"].append("No fieldmap JSON files found")
        return results

    for fmap_json in fmap_jsons:
        _process_single_fmap_json(fmap_json, participant_dir, session, dry_run, results)

    return results


# ---------------------------------------------------------------------------
# Step 3: hide spurious bvec/bval from fmap/
# ---------------------------------------------------------------------------


def remove_bval_bvec_from_fmaps(
    participant_dir: Path,
    session: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Rename .bvec/.bval files in fmap/ with a leading dot to hide them."""
    results: dict[str, Any] = {
        "success": True,
        "hidden_files": [],
        "errors": [],
        "dry_run": dry_run,
    }

    fmap_dir = participant_dir / "fmap"
    if not fmap_dir.exists():
        results["success"] = False
        results["errors"].append(f"Fieldmap directory not found: {fmap_dir}")
        return results

    files_to_hide = [
        f
        for f in fmap_dir.glob("*_epi.bvec") if not f.name.startswith(".")
    ] + [
        f
        for f in fmap_dir.glob("*_epi.bval") if not f.name.startswith(".")
    ]

    for file_path in files_to_hide:
        try:
            if dry_run:
                results["hidden_files"].append(
                    {"file": file_path.name, "will_hide_as": f".{file_path.name}", "note": "dry-run"}
                )
            else:
                hidden = file_path.parent / f".{file_path.name}"
                file_path.rename(hidden)
                results["hidden_files"].append(
                    {"original": file_path.name, "hidden_as": hidden.name}
                )
        except Exception as e:
            results["errors"].append(f"Failed to hide {file_path.name}: {e}")
            results["success"] = False

    return results


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def post_process_heudiconv_output(
    bids_dir: Path,
    participant: str,
    session: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Orchestrate all BIDS post-processing steps for one participant/session."""
    results: dict[str, Any] = {
        "success": True,
        "errors": [],
        "derive_fmap": {},
        "intended_for": {},
        "cleanup": {},
    }

    participant_dir = bids_dir / f"sub-{participant}"
    if session:
        participant_dir = participant_dir / f"ses-{session}"

    if not participant_dir.exists():
        results["success"] = False
        results["errors"].append(f"Participant directory not found: {participant_dir}")
        return results

    _run_step(derive_fmap_from_dwi_pa, "derive_fmap", results, participant_dir, session, dry_run)
    _run_step(add_intended_for_to_fmaps, "intended_for", results, participant_dir, session, dry_run)
    _run_step(remove_bval_bvec_from_fmaps, "cleanup", results, participant_dir, session, dry_run)

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_results(results: dict[str, Any]) -> None:
    derive_fmap = results.get("derive_fmap", {})
    if derive_fmap:
        for entry in derive_fmap.get("derived_files", []):
            note = f" [{entry['note']}]" if "note" in entry else ""
            print(f"  Derived: {entry['source']} → fmap/{entry['output']}{note}")
        for err in derive_fmap.get("errors", []):
            print(f"  WARNING: {err}")

    intended = results.get("intended_for", {})
    if intended:
        for entry in intended.get("updated_files", []):
            note = f" [{entry['note']}]" if "note" in entry else ""
            ntargets = len(entry.get("targets", []))
            print(f"  Updated: fmap/{entry['file']} ({entry['type']}, {ntargets} target(s)){note}")
        for err in intended.get("errors", []):
            print(f"  WARNING: {err}")

    cleanup = results.get("cleanup", {})
    if cleanup:
        for entry in cleanup.get("hidden_files", []):
            note = f" [{entry['note']}]" if "note" in entry else ""
            original = entry.get("original") or entry.get("file")
            hidden_as = entry.get("hidden_as") or entry.get("will_hide_as")
            print(f"  Hidden:  {original} → {hidden_as}{note}")

    for err in results.get("errors", []):
        print(f"  ERROR: {err}", file=sys.stderr)


def _strip_prefix(value: str, prefix: str) -> str:
    return value[len(prefix):] if value.startswith(prefix) else value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="BIDS fieldmap post-processing for HeudiConv output."
    )
    parser.add_argument("subject", help="Subject ID (with or without 'sub-' prefix)")
    parser.add_argument("session", help="Session ID (with or without 'ses-' prefix)")
    parser.add_argument("bids_root", type=Path, help="Root BIDS directory")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report changes without modifying files",
    )
    args = parser.parse_args()

    participant = _strip_prefix(args.subject, "sub-")
    session = _strip_prefix(args.session, "ses-")

    print(f"=== bids_post: sub-{participant} ses-{session} ===")
    if args.dry_run:
        print("  (dry-run mode)")

    print("Step 1: Derive fmap EPI from dir-PA DWI …")
    print("Step 2: Adding IntendedFor to fmap JSONs …")
    print("Step 3: Hiding spurious bvec/bval in fmap/ …")

    results = post_process_heudiconv_output(
        bids_dir=args.bids_root,
        participant=participant,
        session=session,
        dry_run=args.dry_run,
    )

    _print_results(results)

    if results["success"]:
        print("Done.")
    else:
        print("Done with errors.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

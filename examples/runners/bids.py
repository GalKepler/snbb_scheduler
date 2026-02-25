"""BIDS post-processing utilities for HeudiConv output."""

import json
import stat
from collections.abc import Callable
from pathlib import Path
from typing import Any


def _run_post_processing_step(
    step_func: Callable,
    step_name: str,
    results: dict[str, Any],
    *args,
    **kwargs,
) -> None:
    """Helper to run a post-processing step and record its results."""
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


def post_process_heudiconv_output(
    bids_dir: Path,
    participant: str,
    session: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Post-process HeudiConv output to ensure BIDS compliance.

    Orchestrates all post-processing steps:

    1. Verify fieldmap EPI files exist
    2. Add IntendedFor fields to fmap JSONs
    3. Hide bval/bvec from fmap directories (rename with dot prefix)

    Parameters
    ----------
    bids_dir : Path
        Root BIDS directory.
    participant : str
        Participant ID (without 'sub-' prefix).
    session : Optional[str], optional
        Session ID (without 'ses-' prefix), if applicable, by default None.
    dry_run : bool, optional
        If True, report changes without modifying files, by default False.

    Returns
    -------
    Dict[str, Any]
        A dictionary with results:

        - 'success': bool
        - 'verification': dict
        - 'intended_for': dict
        - 'cleanup': dict
        - 'errors': list
    """
    results = {
        "success": True,
        "errors": [],
        "verification": {},
        "intended_for": {},
        "cleanup": {},
    }

    # Build participant directory path
    participant_dir = bids_dir / f"sub-{participant}"
    if session:
        participant_dir = participant_dir / f"ses-{session}"

    if not participant_dir.exists():
        results["success"] = False
        results["errors"].append(f"Participant directory not found: {participant_dir}")
        return results

    # Step 1: Verify fieldmap EPI files exist
    _run_post_processing_step(
        verify_fmap_epi_files,
        "Verification",
        results,
        participant_dir,
        session,
    )

    # Step 2: Add IntendedFor to fieldmap JSONs
    _run_post_processing_step(
        add_intended_for_to_fmaps,
        "IntendedFor processing",
        results,
        participant_dir,
        session,
        dry_run,
    )

    # Step 3: Hide bval/bvec from fmap directories
    _run_post_processing_step(
        remove_bval_bvec_from_fmaps,
        "Cleanup",
        results,
        participant_dir,
        session,
        dry_run,
    )

    return results


def verify_fmap_epi_files(
    participant_dir: Path,
    session: str | None = None,
) -> dict[str, Any]:
    """
    Verify that expected fieldmap EPI files exist.

    Checks for existence of ``*acq-dwi*_epi.nii.gz`` and ``.json`` in ``fmap/`` directory.

    Parameters
    ----------
    participant_dir : Path
        Path to participant directory (or session directory if session exists).
    session : Optional[str], optional
        Session ID (for logging purposes), by default None.

    Returns
    -------
    Dict[str, Any]
        Dictionary with verification results.
    """
    results = {
        "success": True,
        "found_files": [],
        "missing_files": [],
        "errors": [],
    }

    fmap_dir = participant_dir / "fmap"

    if not fmap_dir.exists():
        results["success"] = False
        results["errors"].append(f"Fieldmap directory not found: {fmap_dir}")
        return results

    # Look for DWI fieldmap files
    dwi_epi_nii = list(fmap_dir.glob("*acq-dwi*_epi.nii.gz"))
    dwi_epi_json = list(fmap_dir.glob("*acq-dwi*_epi.json"))

    if dwi_epi_nii:
        results["found_files"].extend([str(f.name) for f in dwi_epi_nii])
    else:
        results["missing_files"].append("*acq-dwi*_epi.nii.gz")
        results["errors"].append("No DWI fieldmap NIfTI files found")
        results["success"] = False

    if dwi_epi_json:
        results["found_files"].extend([str(f.name) for f in dwi_epi_json])
    else:
        results["missing_files"].append("*acq-dwi*_epi.json")
        results["errors"].append("No DWI fieldmap JSON files found")
        results["success"] = False

    return results


def _process_single_fmap_json(
    fmap_json: Path,
    participant_dir: Path,
    session: str | None,
    dry_run: bool,
    results: dict[str, Any],
) -> None:
    """Processes a single fmap JSON file to add IntendedFor field."""
    try:
        # Determine acquisition type from filename
        filename = fmap_json.name

        if "acq-dwi" in filename:
            # DWI fieldmap -> find DWI targets
            target_files = _find_dwi_targets(participant_dir)
            acq_type = "DWI"
        elif "acq-func" in filename:
            # Functional fieldmap -> find all BOLD targets
            target_files = _find_func_targets(participant_dir)
            acq_type = "functional"
        else:
            results["errors"].append(f"Unknown acquisition type in {filename}")
            return

        if not target_files:
            results["errors"].append(f"No target files found for {filename}")
            return

        # Build IntendedFor paths (relative to session or participant directory)
        intended_for_paths = [
            _build_intended_for_path(target, participant_dir, session)
            for target in target_files
        ]

        # Update JSON file
        if not dry_run:
            success = _update_json_sidecar(fmap_json, intended_for_paths)
            if success:
                results["updated_files"].append(
                    {
                        "file": str(fmap_json.name),
                        "type": acq_type,
                        "targets": intended_for_paths,
                    }
                )
            else:
                results["errors"].append(f"Failed to update {filename}")
        else:
            results["updated_files"].append(
                {
                    "file": str(fmap_json.name),
                    "type": acq_type,
                    "targets": intended_for_paths,
                    "note": "Dry run - not modified",
                }
            )

    except Exception as e:
        results["errors"].append(f"Error processing {fmap_json.name}: {e}")
        results["success"] = False


def add_intended_for_to_fmaps(
    participant_dir: Path,
    session: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Add IntendedFor fields to fieldmap JSON files.

    Maps fieldmaps to target files based on acquisition type:

    - ``acq-dwi*_epi.json`` -> all ``dwi/*_dwi.nii.gz`` files
    - ``acq-func*_epi.json`` -> all ``func/*_bold.nii.gz`` files

    Parameters
    ----------
    participant_dir : Path
        Path to participant directory (or session directory if session exists).
    session : Optional[str], optional
        Session ID (for building relative paths), by default None.
    dry_run : bool, optional
        If True, report changes without modifying files, by default False.

    Returns
    -------
    Dict[str, Any]
        Dictionary with processing results.
    """
    results = {
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

    # Find all fieldmap JSON files
    fmap_jsons = list(fmap_dir.glob("*_epi.json"))

    if not fmap_jsons:
        results["errors"].append("No fieldmap JSON files found")
        results["success"] = False
        return results

    for fmap_json in fmap_jsons:
        _process_single_fmap_json(fmap_json, participant_dir, session, dry_run, results)

    return results


def remove_bval_bvec_from_fmaps(
    participant_dir: Path,
    session: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Hide .bvec and .bval files from fmap directories by renaming with dot prefix.

    These files are incorrectly generated by dcm2niix for fieldmaps
    and are not BIDS-compliant for EPI fieldmaps. Instead of deleting,
    we rename them with a leading dot to hide them (e.g., ``.filename.bvec``).

    Parameters
    ----------
    participant_dir : Path
        Path to participant directory (or session directory if session exists).
    session : Optional[str], optional
        Session ID (for logging purposes), by default None.
    dry_run : bool, optional
        If True, report files to hide without renaming, by default False.

    Returns
    -------
    Dict[str, Any]
        Dictionary with cleanup results.
    """
    results = {
        "success": True,
        "hidden_files": [],
        "errors": [],
        "dry_run": dry_run,
    }

    fmap_dir = participant_dir / "fmap"

    if not fmap_dir.exists():
        results["errors"].append(f"Fieldmap directory not found: {fmap_dir}")
        results["success"] = False
        return results

    # Find all .bvec and .bval files in fmap directory (excluding already hidden ones)
    bvec_files = [f for f in fmap_dir.glob("*_epi.bvec") if not f.name.startswith(".")]
    bval_files = [f for f in fmap_dir.glob("*_epi.bval") if not f.name.startswith(".")]

    files_to_hide = bvec_files + bval_files

    if not files_to_hide:
        # Not an error - just means files are already clean/hidden
        return results

    for file_path in files_to_hide:
        try:
            if not dry_run:
                # Rename with leading dot to hide
                hidden_path = file_path.parent / f".{file_path.name}"
                file_path.rename(hidden_path)
                results["hidden_files"].append(
                    {
                        "original": str(file_path.name),
                        "hidden_as": str(hidden_path.name),
                    }
                )
            else:
                results["hidden_files"].append(
                    {
                        "file": str(file_path.name),
                        "will_hide_as": f".{file_path.name}",
                        "note": "Dry run - not renamed",
                    }
                )
        except Exception as e:
            results["errors"].append(f"Failed to hide {file_path.name}: {e}")
            results["success"] = False

    return results


# Private helper functions


def _find_dwi_targets(participant_dir: Path) -> list[Path]:
    """Find all DWI NIfTI files in dwi directory."""
    dwi_dir = participant_dir / "dwi"
    if not dwi_dir.exists():
        return []
    return list(dwi_dir.glob("*_dwi.nii.gz"))


def _find_func_targets(participant_dir: Path) -> list[Path]:
    """Find all functional BOLD NIfTI files in func directory."""
    func_dir = participant_dir / "func"
    if not func_dir.exists():
        return []
    return list(func_dir.glob("*_bold.nii.gz"))


def _build_intended_for_path(
    target_file: Path,
    participant_dir: Path,
    session: str | None = None,
) -> str:
    """
    Build BIDS-compliant relative path for IntendedFor field.

    Paths are relative to the session directory (if session exists)
    or participant directory.

    Parameters
    ----------
    target_file : Path
        Absolute path to target file.
    participant_dir : Path
        Path to participant/session directory.
    session : Optional[str], optional
        Session ID if applicable, by default None.

    Returns
    -------
    str
        Relative path string for IntendedFor field.
    """
    # Get path relative to participant_dir
    try:
        rel_path = target_file.relative_to(participant_dir)
        if session:  # Add "ses-{session}" before
            rel_path = f"ses-{session}/{rel_path}"
        return str(rel_path)
    except ValueError:
        # If relative_to fails, build manually
        # This shouldn't happen if paths are constructed correctly
        return str(target_file.name)


def _update_json_sidecar(json_path: Path, intended_for: list[str]) -> bool:
    """
    Update JSON sidecar file with IntendedFor field.

    Reads existing JSON, adds/updates IntendedFor field, and writes back.
    Preserves all existing fields. Handles read-only files by making them writable.

    Parameters
    ----------
    json_path : Path
        Path to JSON file.
    intended_for : List[str]
        List of relative paths for IntendedFor field.

    Returns
    -------
    bool
        True if successful, False otherwise.
    """
    try:
        # Read existing JSON
        data = _read_json_sidecar(json_path)
        if data is None:
            return False

        # Add IntendedFor field (BIDS spec requires array)
        data["IntendedFor"] = intended_for

        # Make file writable if it's read-only (HeudiConv creates read-only files)
        current_mode = json_path.stat().st_mode
        if not (current_mode & stat.S_IWUSR):
            # Add user write permission
            json_path.chmod(current_mode | stat.S_IWUSR)

        # Write back with formatting
        with open(json_path, "w") as f:
            json.dump(data, f, indent=2)

        return True

    except Exception as e:
        print(f"Error updating {json_path}: {e}")
        return False


def _read_json_sidecar(json_path: Path) -> dict[str, Any] | None:
    """
    Read JSON sidecar file with error handling.

    Parameters
    ----------
    json_path : Path
        Path to JSON file.

    Returns
    -------
    Optional[Dict[str, Any]]
        Dictionary with JSON contents, or None if reading fails.
    """
    try:
        with open(json_path) as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading {json_path}: {e}")
        return None

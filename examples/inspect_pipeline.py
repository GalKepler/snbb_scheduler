"""inspect_pipeline.py — explore pipeline state without submitting anything.

Run with:
    python examples/inspect_pipeline.py --config examples/snbb_config.yaml

Works against any config; paths that don't exist produce empty DataFrames.
"""

import argparse
from pathlib import Path

from snbb_scheduler.checks import is_complete
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.manifest import build_manifest, load_state
from snbb_scheduler.sessions import discover_sessions


def main(config_path: str) -> None:
    cfg = SchedulerConfig.from_yaml(config_path)

    # ------------------------------------------------------------------
    # 1. Discovered sessions
    # ------------------------------------------------------------------
    print("=" * 60)
    print("DISCOVERED SESSIONS")
    print("=" * 60)
    sessions = discover_sessions(cfg)
    if sessions.empty:
        print("  No sessions found (dicom_root may not exist yet).")
    else:
        print(f"  {len(sessions)} session(s) across "
              f"{sessions['subject'].nunique()} subject(s)\n")
        print(sessions[["subject", "session", "dicom_exists"]].to_string(index=False))

    print()

    # ------------------------------------------------------------------
    # 2. Completion status per procedure
    # ------------------------------------------------------------------
    print("=" * 60)
    print("COMPLETION STATUS")
    print("=" * 60)
    if sessions.empty:
        print("  No sessions to check.")
    else:
        for proc in cfg.procedures:
            root = cfg.get_procedure_root(proc)
            if proc.scope == "subject":
                subjects = sessions["subject"].unique()
                paths = [(s, root / s) for s in subjects]
            else:
                paths = [
                    (f"{row['subject']}/{row['session']}",
                     root / row["subject"] / row["session"])
                    for _, row in sessions.iterrows()
                ]

            done = sum(1 for _, p in paths if is_complete(proc, p))
            total = len(paths)
            bar = "#" * done + "-" * (total - done)
            print(f"  {proc.name:<15} [{bar}] {done}/{total}")

    print()

    # ------------------------------------------------------------------
    # 3. Pending manifest
    # ------------------------------------------------------------------
    print("=" * 60)
    print("PENDING TASKS")
    print("=" * 60)
    manifest = build_manifest(sessions, cfg)
    if manifest.empty:
        print("  Nothing to do — all outputs are complete.")
    else:
        print(manifest[["subject", "session", "procedure", "priority"]]
              .to_string(index=False))

    print()

    # ------------------------------------------------------------------
    # 4. State file
    # ------------------------------------------------------------------
    print("=" * 60)
    print("JOB STATE")
    print("=" * 60)
    state = load_state(cfg)
    if state.empty:
        print("  No state recorded yet.")
    else:
        summary = state.groupby("status").size().rename("count")
        print(summary.to_string())
        print()
        if "failed" in state["status"].values:
            failed = state[state["status"] == "failed"]
            print(f"  FAILED JOBS ({len(failed)}):")
            print(failed[["subject", "session", "procedure", "job_id"]]
                  .to_string(index=False))

    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect pipeline state.")
    parser.add_argument("--config", required=True, help="Path to config YAML.")
    args = parser.parse_args()
    main(args.config)

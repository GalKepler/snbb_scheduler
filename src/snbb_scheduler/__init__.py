"""snbb_scheduler — rule-based Slurm scheduler for the SNBB neuroimaging pipeline.

Performs a daily sweep of all MRI sessions, evaluates which processing steps
are needed (BIDS conversion, QSIPrep, FreeSurfer, …), and submits jobs to
Slurm automatically. New procedures require only a YAML config entry — no
code changes.

Typical usage::

    from snbb_scheduler.config import SchedulerConfig
    from snbb_scheduler.sessions import discover_sessions
    from snbb_scheduler.manifest import build_manifest, filter_in_flight, load_state, save_state
    from snbb_scheduler.submit import submit_manifest

    cfg      = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
    sessions = discover_sessions(cfg)
    manifest = build_manifest(sessions, cfg)
    state    = load_state(cfg)
    manifest = filter_in_flight(manifest, state)
    new_rows = submit_manifest(manifest, cfg)
"""

__version__ = "0.1.0"

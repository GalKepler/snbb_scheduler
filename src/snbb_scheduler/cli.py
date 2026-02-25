from __future__ import annotations

import logging

import click
import pandas as pd

from snbb_scheduler.audit import get_logger
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.manifest import (
    build_manifest,
    filter_in_flight,
    load_state,
    save_state,
)
from snbb_scheduler.monitor import update_state_from_sacct
from snbb_scheduler.sessions import discover_sessions
from snbb_scheduler.submit import submit_manifest


@click.group()
@click.option(
    "--config",
    "config_path",
    default=None,
    metavar="PATH",
    help="Path to YAML config file. Uses built-in defaults if omitted.",
)
@click.option(
    "--slurm-mem",
    "slurm_mem",
    default=None,
    metavar="MEM",
    help="Memory limit for Slurm jobs (e.g. 32G). Overrides config file.",
)
@click.option(
    "--slurm-cpus",
    "slurm_cpus",
    default=None,
    type=int,
    metavar="N",
    help="CPUs per task for Slurm jobs. Overrides config file.",
)
@click.option(
    "--slurm-log-dir",
    "slurm_log_dir",
    default=None,
    metavar="DIR",
    help="Directory for Slurm stdout/stderr logs. Overrides config file.",
)
@click.pass_context
def main(
    ctx: click.Context,
    config_path: str | None,
    slurm_mem: str | None,
    slurm_cpus: int | None,
    slurm_log_dir: str | None,
) -> None:
    """snbb-scheduler: rule-based scheduler for the SNBB neuroimaging pipeline."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    ctx.ensure_object(dict)
    config = SchedulerConfig.from_yaml(config_path) if config_path else SchedulerConfig()
    if slurm_mem is not None:
        config.slurm_mem = slurm_mem
    if slurm_cpus is not None:
        config.slurm_cpus_per_task = slurm_cpus
    if slurm_log_dir is not None:
        from pathlib import Path
        config.slurm_log_dir = Path(slurm_log_dir)
    ctx.obj["config"] = config


@main.command()
@click.option("--dry-run", is_flag=True, help="Print what would be submitted without submitting.")
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Re-queue all procedures regardless of status (skips completion check and in-flight filter).",
)
@click.option(
    "--procedure",
    "procedure",
    default=None,
    metavar="NAME",
    help="Limit --force to a single procedure (e.g. bids). Ignored without --force.",
)
@click.option(
    "--skip-monitor",
    is_flag=True,
    default=False,
    help="Skip the pre-run sacct status update.",
)
@click.pass_context
def run(
    ctx: click.Context,
    dry_run: bool,
    force: bool,
    procedure: str | None,
    skip_monitor: bool,
) -> None:
    """Discover sessions, evaluate rules, and submit jobs to Slurm."""
    config: SchedulerConfig = ctx.obj["config"]
    audit = get_logger(config)

    click.echo("Discovering sessions…")
    sessions = discover_sessions(config)
    click.echo(f"  Found {len(sessions)} session(s).")

    force_procedures = [procedure] if (force and procedure) else None
    manifest = build_manifest(sessions, config, force=force, force_procedures=force_procedures)
    click.echo(f"  {len(manifest)} task(s) need processing.")

    state = load_state(config)

    if not skip_monitor and not state.empty:
        try:
            updated = update_state_from_sacct(state, audit)
            if not updated.equals(state):
                save_state(updated, config)
                state = updated
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).warning("Monitor update failed: %s", exc)

    if force:
        click.echo("  --force: skipping in-flight filter.")
    else:
        manifest = filter_in_flight(manifest, state)
        click.echo(f"  {len(manifest)} task(s) after filtering in-flight jobs.")

    if manifest.empty:
        click.echo("Nothing to submit.")
        return

    new_state = submit_manifest(manifest, config, dry_run=dry_run, audit=audit)

    if not dry_run:
        parts = [df for df in (state, new_state) if not df.empty]
        combined = pd.concat(parts, ignore_index=True) if parts else new_state
        save_state(combined, config)
        click.echo(f"Submitted {len(new_state)} job(s). State saved to {config.state_file}.")
    else:
        click.echo(f"[DRY RUN] Would submit {len(new_state)} job(s).")


@main.command(name="manifest")
@click.pass_context
def show_manifest(ctx: click.Context) -> None:
    """Show the current task manifest without submitting."""
    config: SchedulerConfig = ctx.obj["config"]

    sessions = discover_sessions(config)
    manifest = build_manifest(sessions, config)

    if manifest.empty:
        click.echo("No tasks pending.")
        return

    click.echo(manifest[["subject", "session", "procedure", "priority"]].to_string(index=False))


@main.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show the current job state (pending/running/complete/failed)."""
    config: SchedulerConfig = ctx.obj["config"]
    state = load_state(config)

    if state.empty:
        click.echo("No state recorded yet.")
        return

    # Summary table: procedure | status | count
    summary = (
        state.groupby(["procedure", "status"], sort=False)
        .size()
        .reset_index(name="count")
    )
    click.echo("Summary:")
    click.echo(summary.to_string(index=False))
    click.echo("")

    # Full details table, optionally with log_path column
    details = state.copy()
    if config.slurm_log_dir is not None:
        from snbb_scheduler.submit import _build_job_name
        def _log_path(row: pd.Series) -> str:
            try:
                proc = config.get_procedure(row["procedure"])
                job_name = _build_job_name(row, proc.scope)
            except KeyError:
                job_name = f"{row['procedure']}_{row['subject']}"
            log_subdir = config.slurm_log_dir / row["procedure"]
            job_id = row.get("job_id") or ""
            return str(log_subdir / f"{job_name}_{job_id}.out")

        details["log_path"] = details.apply(_log_path, axis=1)

    click.echo(details.to_string(index=False))


@main.command()
@click.pass_context
def monitor(ctx: click.Context) -> None:
    """Poll sacct for in-flight job statuses and update the state file."""
    config: SchedulerConfig = ctx.obj["config"]
    audit = get_logger(config)
    state = load_state(config)

    if state.empty:
        click.echo("No state recorded yet.")
        return

    updated = update_state_from_sacct(state, audit)

    # Count transitions
    transitions = 0
    for idx in state.index:
        if idx < len(updated) and state.at[idx, "status"] != updated.at[idx, "status"]:
            transitions += 1

    if not updated.equals(state):
        save_state(updated, config)
        click.echo(f"Updated {transitions} job status(es).")
    else:
        click.echo("No status changes.")

    # Current status breakdown by procedure
    summary = (
        updated.groupby(["procedure", "status"], sort=False)
        .size()
        .reset_index(name="count")
    )
    click.echo(summary.to_string(index=False))


@main.command()
@click.option("--procedure", default=None, help="Procedure name to retry (e.g. bids).")
@click.option("--subject", default=None, help="Subject to retry (e.g. sub-0001).")
@click.pass_context
def retry(ctx: click.Context, procedure: str | None, subject: str | None) -> None:
    """Remove failed state entries so they are retried on the next run."""
    config: SchedulerConfig = ctx.obj["config"]
    audit = get_logger(config)
    state = load_state(config)

    if state.empty:
        click.echo("No state recorded yet.")
        return

    mask = state["status"] == "failed"
    if procedure:
        mask &= state["procedure"] == procedure
    if subject:
        mask &= state["subject"] == subject

    n = mask.sum()
    if n == 0:
        click.echo("No matching failed entries found.")
        return

    cleared = state[mask]
    for _, row in cleared.iterrows():
        audit.log(
            "retry_cleared",
            subject=row["subject"],
            session=row["session"],
            procedure=row["procedure"],
            job_id=row.get("job_id"),
            old_status="failed",
        )

    state = state[~mask].reset_index(drop=True)
    save_state(state, config)
    click.echo(f"Cleared {n} failed entry/entries. They will be retried on the next run.")

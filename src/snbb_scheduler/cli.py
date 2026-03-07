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
    reconcile_with_filesystem,
    save_state,
)
from snbb_scheduler.monitor import update_state_from_sacct
from snbb_scheduler.sessions import build_session_status_table, discover_sessions
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
            updated = reconcile_with_filesystem(updated, config, audit)
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

    # Poll sacct for cancelled/failed jobs, then reconcile with filesystem
    audit = get_logger(config)
    updated = update_state_from_sacct(state, audit)
    updated = reconcile_with_filesystem(updated, config, audit)
    if not updated.equals(state):
        save_state(updated, config)
        state = updated

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


@main.command(name="session-status")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "csv"]),
    default="table",
    help="Output format.",
)
@click.option("--subject", default=None, help="Filter to a single subject.")
@click.option("--procedure", default=None, help="Show only this procedure column.")
@click.pass_context
def session_status(
    ctx: click.Context,
    output_format: str,
    subject: str | None,
    procedure: str | None,
) -> None:
    """Show per-session status with output paths or log file locations."""
    config: SchedulerConfig = ctx.obj["config"]
    table = build_session_status_table(config)

    if table.empty:
        click.echo("No sessions found.")
        return

    if subject is not None:
        table = table[table["subject"] == subject]

    if procedure is not None:
        keep = ["subject", "session"]
        if procedure in table.columns:
            keep.append(procedure)
        else:
            click.echo(f"Unknown procedure: {procedure}")
            return
        table = table[keep]

    if output_format == "csv":
        click.echo(table.to_csv(index=False))
    else:
        click.echo(table.to_string(index=False))


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
    updated = reconcile_with_filesystem(updated, config, audit)

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
@click.option(
    "--session",
    "session_filter",
    default=None,
    metavar="SUB/SES",
    help="Audit a single session, e.g. sub-0001/ses-01.",
)
@click.option("--subject", "subject_filter", default=None, help="Audit all sessions for a subject.")
@click.option("--procedure", "procedure_filter", default=None, help="Procedure-level audit view.")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "markdown", "html", "json"]),
    default="table",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(),
    default=None,
    help="Save report to file.",
)
@click.option("--email", is_flag=True, default=False, help="Send report via email.")
@click.option("--dicom-only", is_flag=True, default=False, help="Only check DICOM source data.")
@click.option("--logs-only", is_flag=True, default=False, help="Only analyze Slurm logs.")
@click.option(
    "--history",
    is_flag=True,
    default=False,
    help="Include comparison with previous audit report.",
)
@click.pass_context
def audit(
    ctx: click.Context,
    session_filter: str | None,
    subject_filter: str | None,
    procedure_filter: str | None,
    fmt: str,
    output_path: str | None,
    email: bool,
    dicom_only: bool,
    logs_only: bool,
    history: bool,
) -> None:
    """Validate outputs, analyze logs, and generate audit reports."""
    from pathlib import Path

    from snbb_scheduler.auditor import run_full_audit
    from snbb_scheduler.report import (
        compare_reports,
        load_previous_report,
        render_html,
        render_json,
        render_markdown,
        save_report,
        send_report_email,
    )

    config: SchedulerConfig = ctx.obj["config"]

    # Full audit (may be filtered below for display)
    report = run_full_audit(config)

    # Filter by subject/session/procedure if requested
    if subject_filter:
        report.session_results = [
            s for s in report.session_results if s.subject == subject_filter
        ]
    if session_filter:
        parts = session_filter.split("/", 1)
        subj = parts[0] if len(parts) > 0 else ""
        ses = parts[1] if len(parts) > 1 else ""
        report.session_results = [
            s for s in report.session_results
            if s.subject == subj and s.session == ses
        ]
    if procedure_filter:
        report.procedure_summaries = [
            ps for ps in report.procedure_summaries
            if ps.procedure == procedure_filter
        ]
        for s in report.session_results:
            if procedure_filter in s.procedures:
                s.procedures = {procedure_filter: s.procedures[procedure_filter]}

    # dicom-only: strip procedure detail
    if dicom_only:
        for s in report.session_results:
            s.procedures = {}
        report.procedure_summaries = []

    # logs-only: strip DICOM and file check detail
    if logs_only:
        for s in report.session_results:
            for pr in s.procedures.values():
                pr.file_checks = []

    # History comparison
    trend_text = ""
    if history and config.audit.report_dir:
        prev = load_previous_report(config.audit.report_dir)
        if prev:
            delta = compare_reports(report, prev)
            trend_text = (
                f"\nTrend vs previous: health {delta['health_trend']:+.1%}, "
                f"+{delta['sessions_added']} sessions, "
                f"{len(delta['new_completions'])} new completions, "
                f"{len(delta['new_failures'])} new failures.\n"
            )

    # Render
    if fmt == "json":
        rendered = render_json(report)
    elif fmt == "markdown":
        rendered = trend_text + render_markdown(report)
    elif fmt == "html":
        rendered = render_html(report)
    else:  # table (default)
        rendered = trend_text + render_markdown(report)

    # Output
    if output_path:
        Path(output_path).write_text(rendered, encoding="utf-8")
        click.echo(f"Report written to {output_path}")
    else:
        click.echo(rendered)

    # Save JSON report for history
    if config.audit.report_dir and not (dicom_only or logs_only):
        saved = save_report(report, config.audit.report_dir, fmt="json")
        click.echo(f"Report saved to {saved}")

    # Email
    if email:
        recipients = config.audit.email_recipients
        if not recipients:
            click.echo("Warning: no email_recipients configured, skipping email.")
        else:
            send_report_email(report, recipients, from_address=config.audit.email_from)
            click.echo(f"Report emailed to {', '.join(recipients)}")


@main.command()
@click.option("--procedure", default=None, help="Procedure name to retry (e.g. bids).")
@click.option("--subject", default=None, help="Subject to retry (e.g. sub-0001).")
@click.option(
    "--status",
    "target_status",
    default="failed",
    type=click.Choice(["failed", "pending", "running", "all"]),
    show_default=True,
    help=(
        "Which status to clear. Use 'pending' or 'running' to force-requeue "
        "jobs that are stuck (e.g. silently cancelled by Slurm)."
    ),
)
@click.pass_context
def retry(
    ctx: click.Context,
    procedure: str | None,
    subject: str | None,
    target_status: str,
) -> None:
    """Remove state entries so they are retried on the next run.

    By default clears 'failed' entries. Use --status pending to requeue
    jobs that appear stuck (e.g. cancelled by Slurm but still showing as pending).
    """
    config: SchedulerConfig = ctx.obj["config"]
    audit = get_logger(config)
    state = load_state(config)

    if state.empty:
        click.echo("No state recorded yet.")
        return

    if target_status == "all":
        mask = pd.Series(True, index=state.index)
    else:
        mask = state["status"] == target_status
    if procedure:
        mask &= state["procedure"] == procedure
    if subject:
        mask &= state["subject"] == subject

    n = mask.sum()
    if n == 0:
        click.echo(f"No matching {target_status} entries found.")
        return

    cleared = state[mask]
    for _, row in cleared.iterrows():
        audit.log(
            "retry_cleared",
            subject=row["subject"],
            session=row["session"],
            procedure=row["procedure"],
            job_id=row.get("job_id"),
            old_status=str(row["status"]),
        )

    state = state[~mask].reset_index(drop=True)
    save_state(state, config)
    click.echo(f"Cleared {n} {target_status} entry/entries. They will be retried on the next run.")

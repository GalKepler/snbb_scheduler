# Cron / Systemd Setup

Run the scheduler automatically every day so new sessions are processed without manual intervention.

---

## cron

### Minimal cron entry

```cron
# /etc/cron.d/snbb-scheduler
# Run every day at 6:00 AM as the snbb service user
0 6 * * * snbb-user snbb-scheduler --config /etc/snbb/config.yaml run >> /var/log/snbb_scheduler.log 2>&1
```

### With explicit monitor pass

```cron
# /etc/cron.d/snbb-scheduler
# 5:55 AM — update job statuses from sacct
55 5 * * * snbb-user snbb-scheduler --config /etc/snbb/config.yaml monitor >> /var/log/snbb_scheduler.log 2>&1

# 6:00 AM — submit new jobs
0 6 * * * snbb-user snbb-scheduler --config /etc/snbb/config.yaml run >> /var/log/snbb_scheduler.log 2>&1
```

!!! note
    `snbb-scheduler run` already runs `monitor` internally before submission. The explicit pre-run monitor is only needed if you want to separate the sacct polling from the submission step, e.g. to log them separately.

### Log rotation

Add a logrotate config to prevent the log file from growing unbounded:

```
# /etc/logrotate.d/snbb-scheduler
/var/log/snbb_scheduler.log {
    daily
    rotate 30
    compress
    missingok
    notifempty
    dateext
}
```

---

## systemd timer

A systemd timer is more flexible than cron: it handles missed runs, logs to the journal, and supports dependencies.

### Service unit

```ini
# /etc/systemd/system/snbb-scheduler.service
[Unit]
Description=SNBB Scheduler — daily neuroimaging pipeline submission
After=network.target

[Service]
Type=oneshot
User=snbb-user
ExecStart=/usr/local/bin/snbb-scheduler --config /etc/snbb/config.yaml run
StandardOutput=journal
StandardError=journal
SyslogIdentifier=snbb-scheduler
```

### Timer unit

```ini
# /etc/systemd/system/snbb-scheduler.timer
[Unit]
Description=Run SNBB Scheduler daily at 06:00

[Timer]
OnCalendar=*-*-* 06:00:00
Persistent=true      # run missed timer if system was down

[Install]
WantedBy=timers.target
```

### Enable and start

```bash
systemctl daemon-reload
systemctl enable --now snbb-scheduler.timer

# Check status
systemctl status snbb-scheduler.timer
systemctl list-timers snbb-scheduler.timer

# View logs
journalctl -u snbb-scheduler.service -f
```

---

## Combined run + monitor

To run monitor at 5:55 and submit at 6:00 using systemd:

```ini
# /etc/systemd/system/snbb-monitor.service
[Unit]
Description=SNBB Scheduler — job status update

[Service]
Type=oneshot
User=snbb-user
ExecStart=/usr/local/bin/snbb-scheduler --config /etc/snbb/config.yaml monitor
```

```ini
# /etc/systemd/system/snbb-monitor.timer
[Unit]
Description=Run SNBB monitor 5 minutes before submission

[Timer]
OnCalendar=*-*-* 05:55:00
Persistent=true

[Install]
WantedBy=timers.target
```

---

## Recommendations

- Set `slurm_log_dir` in config so each Slurm job writes its stdout/stderr to a file you can inspect later
- Set `log_file` in config to persist the audit log alongside the state file
- Set up log rotation for both the cron/systemd log and the audit log
- Check `snbb-scheduler status` regularly to catch failed jobs before they accumulate

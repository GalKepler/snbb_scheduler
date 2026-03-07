# Email setup

This guide explains how to configure `snbb-scheduler audit --email` to deliver audit reports via email. Three scenarios are covered: Gmail (the most common external provider), a generic SMTP relay with authentication, and a local Postfix relay (typical on HPC clusters).

---

## How it works

When you run `snbb-scheduler audit --email`, the scheduler:

1. Runs the full audit
2. Renders the report as both HTML and plain text
3. Connects to the configured SMTP server
4. Optionally negotiates TLS (`smtp_tls: true` → STARTTLS)
5. Optionally authenticates (`smtp_username` + `smtp_password`)
6. Sends the message to all addresses in `email_recipients`

All SMTP settings live in the `audit:` block of your config file.

---

## Scenario 1 — Gmail

Gmail requires an **App Password** (not your normal Google password). App Passwords are 16-character tokens you generate in your Google account for third-party apps.

### Step 1 — Enable 2-Step Verification

App Passwords are only available when 2-Step Verification is active.

1. Go to your Google Account → **Security**
2. Under "How you sign in to Google", enable **2-Step Verification**

### Step 2 — Generate an App Password

1. Go to **Security** → **2-Step Verification** → scroll to the bottom → **App passwords**
2. Click **Create app password**
3. Give it a name (e.g. `snbb-scheduler`) and click **Create**
4. Copy the 16-character password (you will not see it again)

### Step 3 — Configure the scheduler

```yaml
audit:
  email_recipients:
    - pi@example.com
    - data-manager@example.com
  email_from: your@gmail.com
  smtp_host: smtp.gmail.com
  smtp_port: 587
  smtp_tls: true
  smtp_username: your@gmail.com
  smtp_password: abcd efgh ijkl mnop   # 16-char App Password (spaces are ignored by Gmail)
```

### Step 4 — Test

```bash
snbb-scheduler --config /path/to/config.yaml audit --email
```

Expected output:
```
Report emailed to pi@example.com, data-manager@example.com
```

---

## Scenario 2 — Outlook / Microsoft 365

```yaml
audit:
  email_recipients:
    - pi@example.com
  email_from: your@organisation.onmicrosoft.com
  smtp_host: smtp.office365.com
  smtp_port: 587
  smtp_tls: true
  smtp_username: your@organisation.onmicrosoft.com
  smtp_password: your-password
```

> **Note**: If your organisation enforces conditional access or MFA, you may need to create an app-specific password or ask your IT team to whitelist the scheduler's IP address.

---

## Scenario 3 — Local Postfix relay (HPC)

Many HPC clusters run a local Postfix instance that relays mail internally without authentication. This is the default behaviour.

### Step 1 — Verify Postfix is running

```bash
systemctl status postfix
```

If it is not installed:

```bash
# Debian/Ubuntu
sudo apt install postfix

# RHEL/CentOS/Rocky
sudo dnf install postfix
sudo systemctl enable --now postfix
```

During Postfix installation, select **Internet Site** (to relay outbound mail) or **Satellite system** (to relay through your institution's mail server).

### Step 2 — Configure the scheduler

No SMTP fields are needed — the defaults (`localhost:25`, no TLS, no auth) match a standard Postfix relay:

```yaml
audit:
  email_recipients:
    - pi@example.com
  email_from: snbb-scheduler@hpc.example.com
  # smtp_host, smtp_port, smtp_tls, smtp_username, smtp_password all use defaults
```

### Step 3 — Test

```bash
snbb-scheduler --config /path/to/config.yaml audit --email
```

If mail is not delivered, check:

```bash
# View recent mail log
sudo tail -50 /var/log/mail.log

# Check the mail queue
mailq
```

Common problems:

| Symptom | Fix |
|---|---|
| `Connection refused` on port 25 | Postfix is not running: `sudo systemctl start postfix` |
| Mail accepted but not delivered | Check `/var/log/mail.log` for relay or DNS errors |
| `Relay access denied` | Postfix `mynetworks` does not include `127.0.0.1`; edit `/etc/postfix/main.cf` |

---

## Scenario 4 — Generic authenticated SMTP relay

Replace the values with those provided by your relay service:

```yaml
audit:
  email_recipients:
    - pi@example.com
  email_from: snbb@yourdomain.com
  smtp_host: mail.yourdomain.com
  smtp_port: 587
  smtp_tls: true
  smtp_username: snbb@yourdomain.com
  smtp_password: relay-password
```

---

## Keeping credentials out of the config file

If you do not want to store the SMTP password in plain text, set `smtp_password` to a shell environment variable and expand it at config-load time using a wrapper script:

```bash
#!/bin/bash
# /usr/local/bin/snbb-audit-wrapper
export SMTP_PASSWORD=$(cat /etc/snbb/smtp_password.txt)
envsubst < /etc/snbb/config_template.yaml > /tmp/snbb_config_expanded.yaml
snbb-scheduler --config /tmp/snbb_config_expanded.yaml audit --email
rm -f /tmp/snbb_config_expanded.yaml
```

And in your template (`config_template.yaml`):

```yaml
audit:
  smtp_password: ${SMTP_PASSWORD}
```

---

## SMTP field reference

| Field | Default | Description |
|---|---|---|
| `smtp_host` | `"localhost"` | SMTP server hostname |
| `smtp_port` | `25` | SMTP server port |
| `smtp_tls` | `false` | Enable STARTTLS (required for port 587) |
| `smtp_username` | `null` | Username for authentication; omit for unauthenticated relays |
| `smtp_password` | `null` | Password for authentication; omit for unauthenticated relays |
| `email_from` | `"snbb-scheduler@localhost"` | Sender address in the `From:` header |
| `email_recipients` | `[]` | List of recipient addresses |

---

## See also

- [Audit configuration](../configuration/audit.md) — all `audit:` fields
- [`audit` CLI command](../cli/audit.md) — command options
- [Cron setup](cron-setup.md) — scheduling daily audit + email via cron

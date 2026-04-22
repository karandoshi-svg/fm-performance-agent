# PFM Weekly Performance Agent

Pulls WoW KPI data from Superset (US/UK/EU dashboards) every Monday at 9 AM PT and posts to **#pfm-ai-insights**.

## Quick Start

### 1. Install dependency
```bash
pip3 install requests
```

### 2. Configure Slack credentials
```bash
cp ~/pfm-performance-agent/.env.example ~/.pfm-agent.env
# Edit ~/.pfm-agent.env and add your SLACK_WEBHOOK_URL or SLACK_BOT_TOKEN
```

**Get a Slack Incoming Webhook URL (easiest):**
1. Go to https://api.slack.com/apps → your app → **Incoming Webhooks** → toggle On
2. **Add New Webhook to Workspace** → pick **#pfm-ai-insights** → Allow
3. Copy the `https://hooks.slack.com/services/...` URL into `~/.pfm-agent.env`

### 3. Test it (dry run — no Slack creds needed)
```bash
python3 ~/pfm-performance-agent/pfm_weekly_agent.py
```
Without `SLACK_WEBHOOK_URL` or `SLACK_BOT_TOKEN` set, it prints the formatted message instead of posting.

### 4. Install the weekly cron
```bash
bash ~/pfm-performance-agent/setup_cron.sh
```
This registers `cron: 0 9 * * 1` — fires every Monday at 9:00 AM local time.

> **Note:** Your Mac must be awake at 9 AM Monday. macOS will run a missed job next time cron checks if the machine was asleep.

### 5. Refresh Superset auth (as needed)
```bash
cd ~/robinhood/rh && bazel run //mcp-setup/mcp_setup:rh_mcp_setup -- refresh --server superset --ide claude
```

---

## KPIs Tracked

| Market | KPIs |
|--------|------|
| 🇺🇸 US  | Spend, NFAs, Net Deposits, Gold Subs, ROI |
| 🇬🇧 UK  | Spend, NFAs, ROI |
| 🇪🇺 EU  | Spend, NFAs, ROI |

Reporting window: the most recent full Mon–Sun week vs the prior Mon–Sun week.

---

## Logs
```bash
tail -f ~/pfm-performance-agent/pfm_agent.log
```

## Remove the cron
```bash
crontab -l | grep -v pfm_weekly_agent | crontab -
```

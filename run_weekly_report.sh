#!/bin/bash
# Launcher wrapper — catches failures and posts to Slack
LOG=~/pfm-performance-agent/cron.log
echo "=== PFM Weekly Report $(date) ===" >> "$LOG"

/usr/bin/python3 ~/pfm-performance-agent/pfm_weekly_report.py >> "$LOG" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    /usr/bin/python3 -c "
import requests, sys
from pathlib import Path
env = {}
for line in Path('~/.pfm-agent.env').expanduser().read_text().splitlines():
    if '=' in line:
        k, v = line.strip().split('=', 1)
        env[k] = v
requests.post('https://slack.com/api/chat.postMessage',
    headers={'Authorization': 'Bearer ' + env['SLACK_BOT_TOKEN'], 'Content-Type': 'application/json'},
    json={'channel': '#pfm-ai-insights',
          'text': '⚠️ *PFM Weekly Report failed* — exit code $EXIT_CODE. Check ~/pfm-performance-agent/cron.log',
          'mrkdwn': True}, timeout=10)
" >> "$LOG" 2>&1
fi

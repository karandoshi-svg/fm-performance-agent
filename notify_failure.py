#!/usr/bin/env python3
import os
import requests

REPO = "karandoshi-svg/fm-performance-agent"
requests.post(
    "https://slack.com/api/chat.postMessage",
    headers={
        "Authorization": "Bearer " + os.environ["SLACK_BOT_TOKEN"],
        "Content-Type": "application/json",
    },
    json={
        "channel": "#pfm-ai-insights",
        "text": f":warning: *PFM Weekly Report failed on GitHub Actions* — check https://github.com/{REPO}/actions",
        "mrkdwn": True,
    },
)

"""
Slack Block Kit Alerting Module.
Formats and dispatches rich interactive Slack webhook messages for pipeline runs and failures.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests

from config.settings import NotificationSettings, get_settings

logger = logging.getLogger("notifications.slack")


class SlackNotifier:
    """Sends structured Block Kit alerts to configured Slack Incoming Webhooks."""

    def __init__(self, settings: Optional[NotificationSettings] = None):
        self.settings = settings or get_settings().notification
        self.webhook_url = (
            self.settings.slack_webhook_url.get_secret_value()
            if self.settings.slack_webhook_url
            else None
        )
        self.channel = self.settings.slack_channel
        self.enabled = self.settings.slack_enabled and bool(self.webhook_url)

    def build_block_kit_payload(
        self,
        level: str,
        title: str,
        summary_text: str,
        metrics: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Constructs a Slack Block Kit formatted JSON payload."""
        level_upper = level.upper()
        if level_upper == "SUCCESS":
            icon = "🟢"
            status_color = "#2EB886"
        elif level_upper in ("WARNING", "WARN"):
            icon = "🟡"
            status_color = "#EBB424"
        else:
            icon = "🔴"
            status_color = "#E01E5A"

        blocks: List[Dict[str, Any]] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{icon} {title}",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{summary_text}*\n*Timestamp:* `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}`",
                },
            },
            {"type": "divider"},
        ]

        # Add key-value metrics fields if present
        if metrics:
            fields = []
            for k, v in list(metrics.items())[:8]:
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*{k.replace('_', ' ').title()}:*\n`{v}`",
                })
            blocks.append({
                "type": "section",
                "fields": fields,
            })

        # Add error traceback callout if present
        if error_message:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"🚨 *Error Traceback:*\n```{error_message[:1000]}```",
                },
            })

        return {
            "channel": self.channel,
            "attachments": [
                {
                    "color": status_color,
                    "blocks": blocks,
                }
            ],
        }

    def send_alert(
        self,
        level: str,
        title: str,
        summary_text: str,
        metrics: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """Dispatches the alert to Slack webhook."""
        payload = self.build_block_kit_payload(
            level=level,
            title=title,
            summary_text=summary_text,
            metrics=metrics,
            error_message=error_message,
        )

        if not self.enabled:
            logger.info(f"[Mock Slack Alert] {level.upper()} - {title}: {summary_text}")
            return True

        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            logger.info("Slack notification dispatched successfully.")
            return True
        except Exception as exc:
            logger.error(f"Failed to deliver Slack notification: {exc}")
            return False

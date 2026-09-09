"""
Email Alerting Module.
Generates responsive HTML email notifications and sends them via SMTP.
"""

import logging
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

from config.settings import NotificationSettings, get_settings

logger = logging.getLogger("notifications.email")


class EmailNotifier:
    """Sends styled HTML email reports and alerts via SMTP."""

    def __init__(self, settings: Optional[NotificationSettings] = None):
        self.settings = settings or get_settings().notification
        self.enabled = self.settings.email_enabled

    def generate_html_body(
        self,
        level: str,
        title: str,
        summary_text: str,
        metrics: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ) -> str:
        """Generates modern, responsive HTML email markup."""
        level_upper = level.upper()
        if level_upper == "SUCCESS":
            badge_bg = "#2EB886"
            badge_text = "SUCCESS"
        elif level_upper in ("WARNING", "WARN"):
            badge_bg = "#EBB424"
            badge_text = "WARNING"
        else:
            badge_bg = "#E01E5A"
            badge_text = "FAILURE"

        metrics_html = ""
        if metrics:
            rows = "".join(
                f"<tr><td style='padding: 8px; border-bottom: 1px solid #e2e8f0; font-weight: 600; color: #4a5568;'>{k.replace('_', ' ').title()}</td>"
                f"<td style='padding: 8px; border-bottom: 1px solid #e2e8f0; color: #2d3748;'>{v}</td></tr>"
                for k, v in metrics.items()
            )
            metrics_html = f"""
            <h3 style="color: #2d3748; margin-top: 20px;">Execution Metrics</h3>
            <table style="width: 100%; border-collapse: collapse; margin-top: 10px; background-color: #f7fafc; border-radius: 6px;">
                {rows}
            </table>
            """

        error_html = ""
        if error_message:
            error_html = f"""
            <div style="background-color: #fff5f5; border-left: 4px solid #e53e3e; padding: 12px; margin-top: 20px; border-radius: 4px;">
                <h4 style="color: #c53030; margin: 0 0 8px 0;">Error Details:</h4>
                <pre style="color: #9b2c2c; font-size: 13px; white-space: pre-wrap; margin: 0;">{error_message}</pre>
            </div>
            """

        timestamp_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #2d3748; background-color: #f4f6f8; margin: 0; padding: 20px; }}
                .card {{ max-width: 600px; margin: 0 auto; background: #ffffff; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); padding: 24px; border: 1px solid #e2e8f0; }}
                .badge {{ display: inline-block; padding: 4px 12px; font-size: 12px; font-weight: bold; color: white; background-color: {badge_bg}; border-radius: 12px; }}
            </style>
        </head>
        <body>
            <div class="card">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                    <span class="badge">{badge_text}</span>
                    <span style="font-size: 12px; color: #718096;">{timestamp_str}</span>
                </div>
                <h2 style="margin: 0 0 12px 0; color: #1a202c;">{title}</h2>
                <p style="color: #4a5568; font-size: 15px;">{summary_text}</p>
                {metrics_html}
                {error_html}
                <hr style="border: none; border-top: 1px solid #e2e8f0; margin-top: 24px;" />
                <p style="font-size: 11px; color: #a0aec0; margin: 8px 0 0 0; text-align: center;">
                    Enterprise ETL Pipeline & Data Warehouse Synchronizer
                </p>
            </div>
        </body>
        </html>
        """

    def send_alert(
        self,
        level: str,
        title: str,
        summary_text: str,
        metrics: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """Sends an HTML alert email to the configured recipients."""
        html_content = self.generate_html_body(
            level=level,
            title=title,
            summary_text=summary_text,
            metrics=metrics,
            error_message=error_message,
        )

        if not self.enabled or not self.settings.smtp_user:
            logger.info(f"[Mock Email Alert] {level.upper()} - {title} to {self.settings.email_to}")
            return True

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[{level.upper()}] {title} - Enterprise ETL"
            msg["From"] = self.settings.email_from
            msg["To"] = self.settings.email_to

            part = MIMEText(html_content, "html")
            msg.attach(part)

            pwd = (
                self.settings.smtp_password.get_secret_value()
                if self.settings.smtp_password
                else ""
            )

            with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port) as server:
                server.starttls()
                server.login(self.settings.smtp_user, pwd)
                server.sendmail(self.settings.email_from, [self.settings.email_to], msg.as_string())

            logger.info("Email alert delivered successfully.")
            return True
        except Exception as exc:
            logger.error(f"Failed to deliver Email alert: {exc}")
            return False

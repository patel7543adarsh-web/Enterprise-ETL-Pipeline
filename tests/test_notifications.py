"""
Unit Tests for Slack, Email, and Central AlertManager Notifications.
"""

from unittest.mock import MagicMock, patch
import pytest
from pydantic import SecretStr
from config.settings import NotificationSettings
from notifications.email import EmailNotifier
from notifications.manager import AlertManager
from notifications.slack import SlackNotifier


def test_slack_block_kit_payload_formatting():
    """Validates structure and colors of Slack Block Kit payload."""
    settings = NotificationSettings(
        slack_enabled=True,
        slack_webhook_url=SecretStr("https://hooks.slack.com/services/T00/B00/X00"),
        slack_channel="#data-alerts",
    )
    notifier = SlackNotifier(settings=settings)

    # Success payload
    success_payload = notifier.build_block_kit_payload(
        level="SUCCESS",
        title="ETL Succeeded",
        summary_text="All 4 weeks completed",
        metrics={"records": 100, "duration": "5.2s"},
    )
    assert success_payload["channel"] == "#data-alerts"
    assert success_payload["attachments"][0]["color"] == "#2EB886"
    assert "🟢 ETL Succeeded" in success_payload["attachments"][0]["blocks"][0]["text"]["text"]

    # Critical Failure payload
    fail_payload = notifier.build_block_kit_payload(
        level="CRITICAL",
        title="Pipeline Crashed",
        summary_text="Database connection timeout",
        error_message="Traceback (most recent call last)...",
    )
    assert fail_payload["attachments"][0]["color"] == "#E01E5A"
    assert "🔴 Pipeline Crashed" in fail_payload["attachments"][0]["blocks"][0]["text"]["text"]


def test_email_html_body_generation():
    """Validates HTML email structure and CSS inline styling."""
    notifier = EmailNotifier()
    html = notifier.generate_html_body(
        level="CRITICAL",
        title="ETL Extraction Failed",
        summary_text="Stripe API Rate Limit Exceeded",
        metrics={"failed_endpoint": "v1/customers", "retry_count": 5},
        error_message="HTTP 429 Too Many Requests",
    )

    assert "ETL Extraction Failed" in html
    assert "Stripe API Rate Limit Exceeded" in html
    assert "HTTP 429 Too Many Requests" in html
    assert "Failed Endpoint" in html


def test_alert_manager_dispatch():
    """Tests that AlertManager correctly routes calls to Slack and Email notifiers."""
    settings = NotificationSettings(
        slack_enabled=False,
        email_enabled=False,
        alert_on_success=True,
        alert_on_failure=True,
    )
    manager = AlertManager(settings=settings)

    # Test success dispatch
    manager.notify_pipeline_success("Daily ETL", {"rows": 500}, duration_seconds=10.0)

    # Test failure dispatch
    manager.notify_pipeline_failure("Daily ETL", "DB Crash", stage="loading", exception=ValueError("Invalid DB"))

    # Test data quality alert
    manager.notify_data_quality_warning(
        dataset_name="Unified Customers",
        invalid_count=5,
        total_count=100,
        quality_score=95.0,
        errors=["Missing email for customer 12"],
    )

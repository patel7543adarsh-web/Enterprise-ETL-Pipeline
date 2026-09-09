"""
Central Alert & Notification Manager.
Dispatches pipeline status events and quality alerts across configured channels (Slack, Email, Logs).
"""

import logging
import traceback
from typing import Any, Dict, Optional

from config.settings import NotificationSettings, get_settings
from notifications.email import EmailNotifier
from notifications.slack import SlackNotifier

logger = logging.getLogger("notifications.manager")


class AlertManager:
    """Unified alert dispatcher for pipeline runs, failures, and data quality issues."""

    def __init__(self, settings: Optional[NotificationSettings] = None):
        self.settings = settings or get_settings().notification
        self.slack_notifier = SlackNotifier(settings=self.settings)
        self.email_notifier = EmailNotifier(settings=self.settings)

    def notify_pipeline_success(
        self,
        pipeline_name: str,
        metrics: Optional[Dict[str, Any]] = None,
        duration_seconds: float = 0.0,
    ) -> None:
        """Sends a success notification across enabled channels."""
        if not self.settings.alert_on_success:
            return

        title = f"{pipeline_name} Completed Successfully"
        summary = f"Pipeline execution completed in {duration_seconds:.2f} seconds with zero errors."

        run_metrics = metrics.copy() if metrics else {}
        run_metrics["duration_seconds"] = f"{duration_seconds:.2f}s"

        self.slack_notifier.send_alert(
            level="SUCCESS",
            title=title,
            summary_text=summary,
            metrics=run_metrics,
        )
        self.email_notifier.send_alert(
            level="SUCCESS",
            title=title,
            summary_text=summary,
            metrics=run_metrics,
        )

    def notify_pipeline_failure(
        self,
        pipeline_name: str,
        error_message: str,
        stage: Optional[str] = None,
        exception: Optional[Exception] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Sends an urgent failure alert across all channels."""
        if not self.settings.alert_on_failure:
            return

        stage_str = f" at stage '{stage}'" if stage else ""
        title = f"{pipeline_name} FAILED{stage_str}"
        summary = f"Pipeline execution failed: {error_message}"

        full_error = error_message
        if exception:
            full_error += f"\n\nTraceback:\n{''.join(traceback.format_exception(type(exception), exception, exception.__traceback__))}"

        self.slack_notifier.send_alert(
            level="CRITICAL",
            title=title,
            summary_text=summary,
            metrics=metrics,
            error_message=full_error,
        )
        self.email_notifier.send_alert(
            level="CRITICAL",
            title=title,
            summary_text=summary,
            metrics=metrics,
            error_message=full_error,
        )

    def notify_data_quality_warning(
        self,
        dataset_name: str,
        invalid_count: int,
        total_count: int,
        quality_score: float,
        errors: Optional[list] = None,
    ) -> None:
        """Sends a data quality threshold alert."""
        title = f"Data Quality Threshold Warning - {dataset_name}"
        summary = f"Dataset '{dataset_name}' score dropped to {quality_score:.1f}% ({invalid_count}/{total_count} invalid records)."
        
        metrics = {
            "dataset": dataset_name,
            "total_records": str(total_count),
            "invalid_records": str(invalid_count),
            "quality_score": f"{quality_score:.1f}%",
        }
        
        err_msg = "\n".join(errors) if errors else None

        self.slack_notifier.send_alert(
            level="WARNING",
            title=title,
            summary_text=summary,
            metrics=metrics,
            error_message=err_msg,
        )
        self.email_notifier.send_alert(
            level="WARNING",
            title=title,
            summary_text=summary,
            metrics=metrics,
            error_message=err_msg,
        )


def get_alert_manager(settings: Optional[NotificationSettings] = None) -> AlertManager:
    """Convenience factory function for the AlertManager singleton."""
    return AlertManager(settings=settings)

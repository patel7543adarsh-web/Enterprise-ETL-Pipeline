"""
Alerting and Notifications Package.
Provides Slack, Email, and unified multi-channel pipeline notification services.
"""

from notifications.email import EmailNotifier
from notifications.manager import AlertManager, get_alert_manager
from notifications.slack import SlackNotifier

__all__ = [
    "SlackNotifier",
    "EmailNotifier",
    "AlertManager",
    "get_alert_manager",
]

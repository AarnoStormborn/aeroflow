"""
Notifications module for Slack alerts.

Provides alerting for the ingestion service via Slack webhooks.

Usage:
    from src.notifications import get_notifier
    
    notifier = get_notifier()
    notifier.on_success(record_id=1, record_count=100, s3_path="s3://...")
    notifier.on_failure(record_id=2, error_category="API_ERROR", error_message="...")

Configuration (environment variables):
    SLACK_ENABLED: Enable Slack notifications (default: true)
    SLACK_WEBHOOK_URL: Slack incoming webhook URL (required)
    SLACK_NOTIFY_ON_SUCCESS: Also notify on success (default: false)
"""

from src.notifications.config import (
    SlackSettings,
    NotificationSettings,
    notification_settings,
    get_notification_settings,
)
from src.notifications.slack import (
    SlackNotifier,
    create_slack_notifier,
)
from src.notifications.notifier import (
    IngestionNotifier,
    create_notifier,
    get_notifier,
)

__all__ = [
    # Config
    "SlackSettings",
    "NotificationSettings",
    "notification_settings",
    "get_notification_settings",
    # Slack
    "SlackNotifier",
    "create_slack_notifier",
    # Main notifier
    "IngestionNotifier",
    "create_notifier",
    "get_notifier",
]

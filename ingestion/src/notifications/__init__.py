"""
Notifications module for Discord alerts.

Provides alerting for the ingestion service via Discord webhooks.

Usage:
    from src.notifications import get_notifier

    notifier = get_notifier()
    notifier.on_failure(record_id=2, error_category="API_ERROR", error_message="...")

Configuration (environment variables):
    DISCORD_ENABLED: Enable Discord notifications (default: true)
    DISCORD_WEBHOOK_URL: Discord webhook URL (required)
"""

from src.notifications.config import (
    DiscordSettings,
    NotificationSettings,
    get_notification_settings,
    notification_settings,
)
from src.notifications.discord import (
    DiscordNotifier,
    create_discord_notifier,
)
from src.notifications.notifier import (
    IngestionNotifier,
    create_notifier,
    get_notifier,
)

__all__ = [
    # Config
    "DiscordSettings",
    "NotificationSettings",
    "notification_settings",
    "get_notification_settings",
    # Discord
    "DiscordNotifier",
    "create_discord_notifier",
    # Main notifier
    "IngestionNotifier",
    "create_notifier",
    "get_notifier",
]

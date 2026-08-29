"""
Configuration for notifications (Discord).
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DiscordSettings(BaseSettings):
    """Discord configuration for notifications."""

    # Enable/disable Discord notifications
    enabled: bool = Field(default=True)
    # Discord webhook URL
    webhook_url: str | None = Field(default=None)

    model_config = SettingsConfigDict(env_prefix="DISCORD_")


class NotificationSettings(BaseSettings):
    """Main notification settings."""

    discord: DiscordSettings = Field(default_factory=DiscordSettings)

    # Environment name (included in notifications)
    environment: str = Field(default="development")
    # Service name
    service_name: str = Field(default="ingestion-service")

    model_config = SettingsConfigDict(env_prefix="NOTIFY_")


# Singleton instance
_notification_settings: NotificationSettings | None = None


def get_notification_settings() -> NotificationSettings:
    """Get cached notification settings."""
    global _notification_settings
    if _notification_settings is None:
        _notification_settings = NotificationSettings()
    return _notification_settings


notification_settings = get_notification_settings()

__all__ = [
    "DiscordSettings",
    "NotificationSettings",
    "get_notification_settings",
    "notification_settings",
]

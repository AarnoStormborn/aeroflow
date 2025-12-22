"""
Configuration for notifications (Slack).
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SlackSettings(BaseSettings):
    """Slack configuration for notifications."""
    
    # Enable/disable Slack notifications
    enabled: bool = Field(default=True)
    # Slack incoming webhook URL
    webhook_url: str | None = Field(default=None)
    # Whether to notify on success (usually False to avoid noise)
    notify_on_success: bool = Field(default=False)
    
    model_config = SettingsConfigDict(env_prefix="SLACK_")


class NotificationSettings(BaseSettings):
    """Main notification settings."""
    
    slack: SlackSettings = Field(default_factory=SlackSettings)
    
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
    "SlackSettings",
    "NotificationSettings",
    "get_notification_settings",
    "notification_settings",
]

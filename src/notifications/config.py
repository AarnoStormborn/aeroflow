"""
Configuration for notifications (CloudWatch + SNS).
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CloudWatchSettings(BaseSettings):
    """CloudWatch configuration."""
    
    # Enable/disable CloudWatch metrics
    enabled: bool = Field(default=True)
    # Namespace for custom metrics
    namespace: str = Field(default="FlightsForecasting/Ingestion")
    # AWS region for CloudWatch
    region: str = Field(default="us-east-1")
    
    model_config = SettingsConfigDict(env_prefix="CLOUDWATCH_")


class SNSSettings(BaseSettings):
    """SNS configuration for notifications."""
    
    # Enable/disable SNS notifications
    enabled: bool = Field(default=True)
    # SNS Topic ARN for failure notifications
    topic_arn: str | None = Field(default=None)
    # AWS region for SNS
    region: str = Field(default="us-east-1")
    
    model_config = SettingsConfigDict(env_prefix="SNS_")


class NotificationSettings(BaseSettings):
    """Main notification settings."""
    
    cloudwatch: CloudWatchSettings = Field(default_factory=CloudWatchSettings)
    sns: SNSSettings = Field(default_factory=SNSSettings)
    
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
    "CloudWatchSettings",
    "SNSSettings",
    "NotificationSettings",
    "get_notification_settings",
    "notification_settings",
]

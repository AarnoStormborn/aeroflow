"""
Notifications module for CloudWatch metrics and SNS alerts.

Provides monitoring and alerting for the ingestion service:
- CloudWatch custom metrics (success/failure counts, latency)
- SNS notifications for failures (email/SMS alerts)

Usage:
    from src.notifications import get_notifier
    
    notifier = get_notifier()
    notifier.on_success(record_id=1, record_count=100, s3_path="s3://...")
    notifier.on_failure(record_id=2, error_category="API_ERROR", error_message="...")

Configuration (environment variables):
    CLOUDWATCH_ENABLED: Enable CloudWatch metrics (default: true)
    CLOUDWATCH_NAMESPACE: Metric namespace (default: FlightsForecasting/Ingestion)
    SNS_ENABLED: Enable SNS notifications (default: true)
    SNS_TOPIC_ARN: SNS topic ARN for alerts (required for notifications)
"""

from src.notifications.config import (
    CloudWatchSettings,
    SNSSettings,
    NotificationSettings,
    notification_settings,
    get_notification_settings,
)
from src.notifications.cloudwatch import (
    CloudWatchPublisher,
    create_cloudwatch_publisher,
)
from src.notifications.sns import (
    SNSNotifier,
    create_sns_notifier,
)
from src.notifications.notifier import (
    IngestionNotifier,
    create_notifier,
    get_notifier,
)

__all__ = [
    # Config
    "CloudWatchSettings",
    "SNSSettings",
    "NotificationSettings",
    "notification_settings",
    "get_notification_settings",
    # CloudWatch
    "CloudWatchPublisher",
    "create_cloudwatch_publisher",
    # SNS
    "SNSNotifier",
    "create_sns_notifier",
    # Main notifier
    "IngestionNotifier",
    "create_notifier",
    "get_notifier",
]

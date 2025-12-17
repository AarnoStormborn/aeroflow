"""
Main notifier that combines CloudWatch metrics and SNS alerts.

Provides a unified interface for recording metrics and sending notifications.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.utils import logger
from src.notifications.config import notification_settings
from src.notifications.cloudwatch import CloudWatchPublisher, create_cloudwatch_publisher
from src.notifications.sns import SNSNotifier, create_sns_notifier

if TYPE_CHECKING:
    from src.ingestion.db import IngestionRecord


class IngestionNotifier:
    """
    Unified notifier for ingestion events.
    
    Combines:
    - CloudWatch metrics for monitoring
    - SNS notifications for alerts
    """
    
    def __init__(
        self,
        cloudwatch: CloudWatchPublisher | None = None,
        sns: SNSNotifier | None = None,
    ):
        """
        Initialize the notifier.
        
        Args:
            cloudwatch: CloudWatch publisher (created if not provided)
            sns: SNS notifier (created if not provided)
        """
        self.cloudwatch = cloudwatch or create_cloudwatch_publisher()
        self.sns = sns or create_sns_notifier()
        
        logger.info("IngestionNotifier initialized")
    
    def on_success(
        self,
        record_id: int | None,
        record_count: int,
        s3_path: str | None,
        duration_seconds: float | None = None,
    ) -> None:
        """
        Handle successful ingestion.
        
        Args:
            record_id: Database record ID
            record_count: Number of records ingested
            s3_path: S3 path where data was stored
            duration_seconds: Time taken for ingestion
        """
        logger.info(f"Recording success: {record_count} records")
        
        # Publish CloudWatch metrics
        self.cloudwatch.record_success(record_count, duration_seconds)
    
    def on_failure(
        self,
        record_id: int | None,
        error_category: str,
        error_message: str,
        duration_seconds: float | None = None,
    ) -> None:
        """
        Handle failed ingestion.
        
        Args:
            record_id: Database record ID
            error_category: Category of the error
            error_message: Detailed error message
            duration_seconds: Time taken before failure
        """
        logger.error(f"Recording failure: [{error_category}] {error_message}")
        
        # Publish CloudWatch metrics
        self.cloudwatch.record_failure(error_category, duration_seconds)
        
        # Send SNS notification
        self.sns.notify_failure(
            error_category=error_category,
            error_message=error_message,
            record_id=record_id,
        )
    
    def notify_from_record(
        self,
        record: "IngestionRecord",
        duration_seconds: float | None = None,
    ) -> None:
        """
        Send notifications based on an ingestion record.
        
        Args:
            record: The ingestion record
            duration_seconds: Time taken for the ingestion
        """
        from src.ingestion.db import IngestionStatus
        
        if record.status == IngestionStatus.SUCCESS:
            self.on_success(
                record_id=record.id,
                record_count=record.record_count,
                s3_path=record.s3_path,
                duration_seconds=duration_seconds,
            )
        elif record.status == IngestionStatus.FAILED:
            # Extract category from error message if present
            error_message = record.error_message or "Unknown error"
            if error_message.startswith("[") and "]" in error_message:
                category = error_message[1:error_message.index("]")]
                message = error_message[error_message.index("]") + 2:]
            else:
                category = "UNKNOWN"
                message = error_message
            
            self.on_failure(
                record_id=record.id,
                error_category=category,
                error_message=message,
                duration_seconds=duration_seconds,
            )


def create_notifier() -> IngestionNotifier:
    """Create a new notifier."""
    return IngestionNotifier()


# Singleton instance
_notifier: IngestionNotifier | None = None


def get_notifier() -> IngestionNotifier:
    """Get the global notifier instance."""
    global _notifier
    if _notifier is None:
        _notifier = create_notifier()
    return _notifier


__all__ = [
    "IngestionNotifier",
    "create_notifier",
    "get_notifier",
]

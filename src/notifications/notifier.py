"""
Main notifier that sends Slack alerts.

Provides a unified interface for sending notifications.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.utils import logger
from src.notifications.config import notification_settings
from src.notifications.slack import SlackNotifier, create_slack_notifier

if TYPE_CHECKING:
    from src.ingestion.db import IngestionRecord


class IngestionNotifier:
    """
    Unified notifier for ingestion events.
    
    Sends Slack notifications for failures (and optionally successes).
    """
    
    def __init__(
        self,
        slack: SlackNotifier | None = None,
    ):
        """
        Initialize the notifier.
        
        Args:
            slack: Slack notifier (created if not provided)
        """
        self.slack = slack or create_slack_notifier()
        
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
        
        # Only notify on success if configured (usually disabled)
        self.slack.notify_success(
            record_count=record_count,
            s3_path=s3_path,
            duration_seconds=duration_seconds,
        )
    
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
        
        # Send Slack notification
        self.slack.notify_failure(
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

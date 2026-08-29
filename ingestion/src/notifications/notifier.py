"""
Main notifier that sends Discord alerts.

Provides a unified interface for sending notifications.
"""

from typing import TYPE_CHECKING

from src.notifications.discord import DiscordNotifier, create_discord_notifier
from src.utils import logger

if TYPE_CHECKING:
    from src.ingestion.db import IngestionRecord


class IngestionNotifier:
    """
    Unified notifier for ingestion events.

    Sends Discord notifications for failures.
    """

    def __init__(
        self,
        discord: DiscordNotifier | None = None,
    ):
        """
        Initialize the notifier.

        Args:
            discord: Discord notifier (created if not provided)
        """
        self.discord = discord or create_discord_notifier()

        logger.info("IngestionNotifier initialized")

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

        # Send Discord notification
        self.discord.notify_failure(
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

        if record.status == IngestionStatus.FAILED:
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
        elif record.status == IngestionStatus.SUCCESS:
            logger.info(
                f"Ingestion success (record {record.id}): "
                f"{record.record_count} records → {record.s3_path}"
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

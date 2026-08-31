"""
Main notifier that sends Discord alerts.

Provides a unified interface for sending notifications.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.notifications.discord import DiscordNotifier, create_discord_notifier
from src.utils import logger

if TYPE_CHECKING:
    from src.ingestion.db import IngestionRecord


class IngestionNotifier:
    """
    Unified notifier for ingestion events.

    Sends Discord notifications for failures, deduplicated so repeated
    failures of the same category don't spam (cooldown window).
    """

    def __init__(
        self,
        discord: DiscordNotifier | None = None,
        alert_cooldown_seconds: int = 3600,
    ):
        """
        Initialize the notifier.

        Args:
            discord: Discord notifier (created if not provided)
            alert_cooldown_seconds: Min seconds between alerts of the same
                error category (default 1 hour). Set 0 to disable dedup.
        """
        self.discord = discord or create_discord_notifier()
        self.alert_cooldown_seconds = alert_cooldown_seconds

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

        # Deduplicate: skip alert if same category was alerted recently.
        # Uses the DB so it works across fresh containers (serverless).
        if not self._should_alert(error_category):
            return

        # Send Discord notification
        self.discord.notify_failure(
            error_category=error_category,
            error_message=error_message,
            record_id=record_id,
        )

    def _should_alert(self, category: str) -> bool:
        """Return True if an alert for this category should be sent now.

        Consults the persistent ingestion DB: if the most recent failed
        record with this error category is newer than the cooldown window,
        suppress (prevents spam across serverless invocations).
        """
        if self.alert_cooldown_seconds <= 0:
            return True

        try:
            from src.ingestion.db import create_repository

            repo = create_repository()
            last = repo.get_last_failure_by_category(category)
            if last is not None:
                age = (datetime.now(timezone.utc) - last.created_at).total_seconds()
                if age < self.alert_cooldown_seconds:
                    logger.info(
                        f"Suppressing duplicate {category} alert "
                        f"(last failure {age:.0f}s ago, cooldown {self.alert_cooldown_seconds}s)"
                    )
                    return False
        except Exception as e:
            logger.warning(f"Dedup check failed ({e}); sending alert anyway")

        return True

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

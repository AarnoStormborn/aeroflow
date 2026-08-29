"""
Discord notification sender for ingestion alerts.

Sends failure notifications to a Discord channel via webhook.
"""

from datetime import datetime, timezone

import httpx

from src.notifications.config import notification_settings
from src.utils import logger


class DiscordNotifier:
    """
    Sends notifications to Discord via webhooks.

    Publishes rich formatted messages (embeds) for:
    - Ingestion failures (with error details)
    """

    def __init__(
        self,
        webhook_url: str | None = None,
    ):
        """
        Initialize Discord notifier.

        Args:
            webhook_url: Discord webhook URL
        """
        self.webhook_url = webhook_url or notification_settings.discord.webhook_url
        self.enabled = notification_settings.discord.enabled and bool(self.webhook_url)

        if self.enabled:
            logger.info("Discord notifier initialized")
        else:
            if not self.webhook_url:
                logger.warning("Discord webhook URL not configured, notifications disabled")
            else:
                logger.info("Discord notifier disabled")

    def _send(self, payload: dict) -> bool:
        """
        Send a message to Discord.

        Args:
            payload: Discord message payload (embeds)

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.webhook_url:
            logger.debug("Discord disabled or not configured, skipping notification")
            return False

        try:
            with httpx.Client(timeout=10) as client:
                response = client.post(
                    self.webhook_url,
                    json=payload,
                )

                # Discord webhooks return 204 No Content on success
                if response.status_code != 204:
                    logger.error(f"Discord webhook failed: {response.status_code} - {response.text}")
                    return False

                logger.info("Discord notification sent successfully")
                return True

        except Exception as e:
            logger.error(f"Failed to send Discord notification: {e}")
            return False

    def notify_failure(
        self,
        error_category: str,
        error_message: str,
        record_id: int | None = None,
        timestamp: datetime | None = None,
    ) -> bool:
        """
        Send a failure notification to Discord.

        Args:
            error_category: Category of the error
            error_message: Detailed error message
            record_id: Database record ID (if available)
            timestamp: When the failure occurred

        Returns:
            True if notification was sent successfully
        """
        timestamp = timestamp or datetime.now(timezone.utc)

        payload = {
            "content": None,
            "embeds": [
                {
                    "title": "🚨 Ingestion Failed",
                    "color": 0xFF0000,  # Red
                    "fields": [
                        {
                            "name": "Environment",
                            "value": notification_settings.environment,
                            "inline": True,
                        },
                        {
                            "name": "Service",
                            "value": notification_settings.service_name,
                            "inline": True,
                        },
                        {
                            "name": "Error Category",
                            "value": f"`{error_category}`",
                            "inline": True,
                        },
                        {
                            "name": "Record ID",
                            "value": str(record_id) if record_id else "N/A",
                            "inline": True,
                        },
                        {
                            "name": "Error Message",
                            "value": f"```{error_message}```",
                            "inline": False,
                        },
                    ],
                    "footer": {
                        "text": f"⏰ {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                    },
                }
            ],
        }

        return self._send(payload)


def create_discord_notifier() -> DiscordNotifier:
    """Create a new Discord notifier."""
    return DiscordNotifier()


__all__ = ["DiscordNotifier", "create_discord_notifier"]

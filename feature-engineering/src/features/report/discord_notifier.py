"""
Discord notifier for daily reports.

Sends rich notifications with report links to Discord.
"""

from datetime import date, datetime, timezone

import httpx
from loguru import logger

from src.features.config import settings


class DiscordNotifier:
    """Sends notifications to Discord via webhook."""

    def __init__(self, webhook_url: str | None = None):
        """Initialize Discord notifier."""
        self.webhook_url = webhook_url or settings.discord_webhook_url
        self.enabled = bool(self.webhook_url)

        if self.enabled:
            logger.info("Discord notifier initialized")
        else:
            logger.warning("Discord webhook URL not configured")

    def _send(self, payload: dict) -> bool:
        """Send a message to Discord."""
        if not self.enabled:
            logger.debug("Discord disabled, skipping notification")
            return False

        try:
            with httpx.Client(timeout=10) as client:
                response = client.post(self.webhook_url, json=payload)

                if response.status_code != 204:
                    logger.error(f"Discord webhook failed: {response.status_code}")
                    return False

                logger.info("Discord notification sent successfully")
                return True

        except Exception as e:
            logger.error(f"Failed to send Discord notification: {e}")
            return False

    def notify_report_ready(
        self,
        report_date: date,
        record_count: int,
        aircraft_count: int,
        pdf_url: str,
        data_url: str,
    ) -> bool:
        """
        Send notification that daily report is ready.

        Args:
            report_date: Date of the report
            record_count: Number of records in the data
            aircraft_count: Number of unique aircraft
            pdf_url: Presigned URL for PDF download
            data_url: Presigned URL for data download

        Returns:
            True if notification was sent successfully
        """
        timestamp = datetime.now(timezone.utc)

        payload = {
            "content": None,
            "embeds": [
                {
                    "title": "📊 Daily Flight Report Ready",
                    "color": 0x00FF00,  # Green
                    "fields": [
                        {
                            "name": "Date",
                            "value": report_date.strftime("%B %d, %Y"),
                            "inline": True,
                        },
                        {
                            "name": "Region",
                            "value": "Mumbai Airspace",
                            "inline": True,
                        },
                        {
                            "name": "Total Records",
                            "value": f"{record_count:,}",
                            "inline": True,
                        },
                        {
                            "name": "Unique Aircraft",
                            "value": f"{aircraft_count:,}",
                            "inline": True,
                        },
                        {
                            "name": "PDF Report",
                            "value": f"[📄 Download]({pdf_url})",
                            "inline": False,
                        },
                        {
                            "name": "Cleaned Data",
                            "value": f"[📦 Download]({data_url})",
                            "inline": False,
                        },
                    ],
                    "footer": {
                        "text": f"⏰ Generated at {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')} | Links valid for 24 hours",  # noqa: E501
                    },
                }
            ],
        }

        return self._send(payload)

    def notify_report_failed(
        self,
        report_date: date,
        error_message: str,
    ) -> bool:
        """Send notification that report generation failed."""
        payload = {
            "content": None,
            "embeds": [
                {
                    "title": "🚨 Daily Report Failed",
                    "color": 0xFF0000,  # Red
                    "fields": [
                        {
                            "name": "Date",
                            "value": report_date.strftime("%B %d, %Y"),
                            "inline": True,
                        },
                        {
                            "name": "Region",
                            "value": "Mumbai Airspace",
                            "inline": True,
                        },
                        {
                            "name": "Error",
                            "value": f"```{error_message}```",
                            "inline": False,
                        },
                    ],
                }
            ],
        }

        return self._send(payload)


def create_discord_notifier() -> DiscordNotifier:
    """Create a new Discord notifier."""
    return DiscordNotifier()


__all__ = ["DiscordNotifier", "create_discord_notifier"]

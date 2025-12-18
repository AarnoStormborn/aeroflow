"""
Slack notification sender for ingestion alerts.

Sends failure/success notifications to a Slack channel via webhook.
"""

from datetime import datetime, timezone

import httpx

from src.utils import logger
from src.notifications.config import notification_settings


class SlackNotifier:
    """
    Sends notifications to Slack via incoming webhooks.
    
    Publishes rich formatted messages for:
    - Ingestion failures (with error details)
    - Ingestion recovery (optional)
    """
    
    def __init__(
        self,
        webhook_url: str | None = None,
    ):
        """
        Initialize Slack notifier.
        
        Args:
            webhook_url: Slack incoming webhook URL
        """
        self.webhook_url = webhook_url or notification_settings.slack.webhook_url
        self.enabled = notification_settings.slack.enabled and bool(self.webhook_url)
        
        if self.enabled:
            logger.info("Slack notifier initialized")
        else:
            if not self.webhook_url:
                logger.warning("Slack webhook URL not configured, notifications disabled")
            else:
                logger.info("Slack notifier disabled")
    
    def _send(self, payload: dict) -> bool:
        """
        Send a message to Slack.
        
        Args:
            payload: Slack message payload (blocks/attachments)
            
        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.webhook_url:
            logger.debug("Slack disabled or not configured, skipping notification")
            return False
        
        try:
            with httpx.Client(timeout=10) as client:
                response = client.post(
                    self.webhook_url,
                    json=payload,
                )
                
                if response.status_code != 200:
                    logger.error(f"Slack webhook failed: {response.status_code} - {response.text}")
                    return False
                
                logger.info("Slack notification sent successfully")
                return True
                
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False
    
    def notify_failure(
        self,
        error_category: str,
        error_message: str,
        record_id: int | None = None,
        timestamp: datetime | None = None,
    ) -> bool:
        """
        Send a failure notification to Slack.
        
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
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🚨 Ingestion Failed",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Environment:*\n{notification_settings.environment}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Service:*\n{notification_settings.service_name}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Error Category:*\n`{error_category}`"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Record ID:*\n{record_id or 'N/A'}"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error Message:*\n```{error_message}```"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"⏰ {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                        }
                    ]
                }
            ]
        }
        
        return self._send(payload)
    
    def notify_success(
        self,
        record_count: int,
        s3_path: str | None = None,
        duration_seconds: float | None = None,
        timestamp: datetime | None = None,
    ) -> bool:
        """
        Send a success notification to Slack (optional, usually disabled).
        
        Args:
            record_count: Number of records ingested
            s3_path: S3 path where data was stored
            duration_seconds: Time taken for ingestion
            timestamp: When ingestion completed
            
        Returns:
            True if notification was sent successfully
        """
        if not notification_settings.slack.notify_on_success:
            return False
            
        timestamp = timestamp or datetime.now(timezone.utc)
        duration_str = f"{duration_seconds:.1f}s" if duration_seconds else "N/A"
        
        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "✅ Ingestion Successful",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Records:*\n{record_count}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Duration:*\n{duration_str}"
                        }
                    ]
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"⏰ {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                        }
                    ]
                }
            ]
        }
        
        return self._send(payload)


def create_slack_notifier() -> SlackNotifier:
    """Create a new Slack notifier."""
    return SlackNotifier()


__all__ = ["SlackNotifier", "create_slack_notifier"]

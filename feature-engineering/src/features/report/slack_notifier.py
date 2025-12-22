"""
Slack notifier for daily reports.

Sends rich notifications with report links to Slack.
"""

from datetime import date, datetime, timezone

import httpx
from loguru import logger

from src.features.config import settings


class SlackNotifier:
    """Sends notifications to Slack via webhook."""
    
    def __init__(self, webhook_url: str | None = None):
        """Initialize Slack notifier."""
        self.webhook_url = webhook_url or settings.slack_webhook_url
        self.enabled = bool(self.webhook_url)
        
        if self.enabled:
            logger.info("Slack notifier initialized")
        else:
            logger.warning("Slack webhook URL not configured")
    
    def _send(self, payload: dict) -> bool:
        """Send a message to Slack."""
        if not self.enabled:
            logger.debug("Slack disabled, skipping notification")
            return False
        
        try:
            with httpx.Client(timeout=10) as client:
                response = client.post(self.webhook_url, json=payload)
                
                if response.status_code != 200:
                    logger.error(f"Slack webhook failed: {response.status_code}")
                    return False
                
                logger.info("Slack notification sent successfully")
                return True
                
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
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
        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "📊 Daily Flight Report Ready",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Date:*\n{report_date.strftime('%B %d, %Y')}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Region:*\nMumbai Airspace"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Total Records:*\n{record_count:,}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Unique Aircraft:*\n{aircraft_count:,}"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Download Links:*"
                    }
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "📄 PDF Report",
                                "emoji": True
                            },
                            "url": pdf_url,
                            "style": "primary"
                        },
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "📦 Cleaned Data",
                                "emoji": True
                            },
                            "url": data_url
                        }
                    ]
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"⏰ Generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} | Links valid for 24 hours"
                        }
                    ]
                }
            ]
        }
        
        return self._send(payload)
    
    def notify_report_failed(
        self,
        report_date: date,
        error_message: str,
    ) -> bool:
        """Send notification that report generation failed."""
        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🚨 Daily Report Failed",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Date:*\n{report_date.strftime('%B %d, %Y')}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Region:*\nMumbai Airspace"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error:*\n```{error_message}```"
                    }
                }
            ]
        }
        
        return self._send(payload)


def create_slack_notifier() -> SlackNotifier:
    """Create a new Slack notifier."""
    return SlackNotifier()


__all__ = ["SlackNotifier", "create_slack_notifier"]

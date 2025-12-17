"""
SNS notification sender for ingestion failures.

Publishes failure notifications to an SNS topic which can be
subscribed to by email, SMS, Lambda, etc.
"""

import json
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from src.utils import logger
from src.notifications.config import notification_settings


class SNSNotifier:
    """
    Sends notifications via AWS SNS.
    
    Publishes structured messages to an SNS topic for:
    - Ingestion failures
    - Critical errors
    """
    
    def __init__(
        self,
        topic_arn: str | None = None,
        region: str | None = None,
    ):
        """
        Initialize SNS notifier.
        
        Args:
            topic_arn: SNS Topic ARN to publish to
            region: AWS region
        """
        self.enabled = notification_settings.sns.enabled
        self.topic_arn = topic_arn or notification_settings.sns.topic_arn
        self.region = region or notification_settings.sns.region
        
        if self.enabled and self.topic_arn:
            self._client = boto3.client("sns", region_name=self.region)
            logger.info(f"SNS notifier initialized (topic: {self.topic_arn})")
        else:
            self._client = None
            if not self.topic_arn:
                logger.warning("SNS topic ARN not configured, notifications disabled")
            else:
                logger.info("SNS notifier disabled")
    
    def _publish(
        self,
        subject: str,
        message: str,
        message_attributes: dict | None = None,
    ) -> bool:
        """
        Publish a message to the SNS topic.
        
        Args:
            subject: Message subject (for email)
            message: Message body
            message_attributes: Optional message attributes
            
        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self._client or not self.topic_arn:
            logger.debug("SNS disabled or not configured, skipping notification")
            return False
        
        try:
            publish_kwargs = {
                "TopicArn": self.topic_arn,
                "Subject": subject,
                "Message": message,
            }
            
            if message_attributes:
                publish_kwargs["MessageAttributes"] = {
                    k: {"DataType": "String", "StringValue": str(v)}
                    for k, v in message_attributes.items()
                }
            
            response = self._client.publish(**publish_kwargs)
            message_id = response.get("MessageId")
            
            logger.info(f"Published SNS notification: {subject} (MessageId: {message_id})")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to publish SNS notification: {e}")
            return False
    
    def notify_failure(
        self,
        error_category: str,
        error_message: str,
        record_id: int | None = None,
        timestamp: datetime | None = None,
    ) -> bool:
        """
        Send a failure notification.
        
        Args:
            error_category: Category of the error
            error_message: Detailed error message
            record_id: Database record ID (if available)
            timestamp: When the failure occurred
            
        Returns:
            True if notification was sent successfully
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        
        subject = f"[{notification_settings.environment.upper()}] Ingestion Failed: {error_category}"
        
        # Build formatted message for email
        message = f"""
🚨 INGESTION FAILURE ALERT

Environment: {notification_settings.environment}
Service: {notification_settings.service_name}
Timestamp: {timestamp.isoformat()}

Error Category: {error_category}
Error Message: {error_message}

{"Record ID: " + str(record_id) if record_id else "Record ID: N/A"}

---
This is an automated alert from the Flights Forecasting Ingestion Service.
"""
        
        attributes = {
            "environment": notification_settings.environment,
            "service": notification_settings.service_name,
            "error_category": error_category,
        }
        
        return self._publish(subject, message.strip(), attributes)
    
    def notify_recovery(
        self,
        record_count: int,
        timestamp: datetime | None = None,
    ) -> bool:
        """
        Send a recovery notification after failures.
        
        Args:
            record_count: Number of records in recovered ingestion
            timestamp: When recovery occurred
            
        Returns:
            True if notification was sent successfully
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        
        subject = f"[{notification_settings.environment.upper()}] Ingestion Recovered"
        
        message = f"""
✅ INGESTION RECOVERY

Environment: {notification_settings.environment}
Service: {notification_settings.service_name}
Timestamp: {timestamp.isoformat()}

Status: Ingestion has recovered successfully
Records Ingested: {record_count}

---
This is an automated alert from the Flights Forecasting Ingestion Service.
"""
        
        return self._publish(subject, message.strip())


def create_sns_notifier() -> SNSNotifier:
    """Create a new SNS notifier."""
    return SNSNotifier()


__all__ = ["SNSNotifier", "create_sns_notifier"]

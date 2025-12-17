"""
CloudWatch metrics publisher for ingestion service.

Publishes custom metrics to CloudWatch for monitoring:
- Ingestion success/failure counts
- Record counts
- Latency metrics
"""

from datetime import datetime, timezone
from typing import Literal

import boto3
from botocore.exceptions import ClientError

from src.utils import logger
from src.notifications.config import notification_settings


MetricUnit = Literal["Count", "Seconds", "Bytes", "None"]


class CloudWatchPublisher:
    """
    Publishes custom metrics to AWS CloudWatch.
    
    Metrics published:
    - IngestionSuccess: Count of successful ingestions
    - IngestionFailure: Count of failed ingestions
    - IngestionRecordCount: Number of records ingested
    - IngestionDuration: Time taken for ingestion (seconds)
    """
    
    def __init__(
        self,
        namespace: str | None = None,
        region: str | None = None,
    ):
        """
        Initialize CloudWatch publisher.
        
        Args:
            namespace: CloudWatch namespace for metrics
            region: AWS region
        """
        self.enabled = notification_settings.cloudwatch.enabled
        self.namespace = namespace or notification_settings.cloudwatch.namespace
        self.region = region or notification_settings.cloudwatch.region
        
        if self.enabled:
            self._client = boto3.client("cloudwatch", region_name=self.region)
            logger.info(f"CloudWatch publisher initialized (namespace: {self.namespace})")
        else:
            self._client = None
            logger.info("CloudWatch publisher disabled")
    
    def _put_metric(
        self,
        metric_name: str,
        value: float,
        unit: MetricUnit = "Count",
        dimensions: dict[str, str] | None = None,
    ) -> bool:
        """
        Publish a single metric to CloudWatch.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Metric unit
            dimensions: Optional dimensions
            
        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self._client:
            logger.debug(f"CloudWatch disabled, skipping metric: {metric_name}")
            return False
        
        try:
            metric_data = {
                "MetricName": metric_name,
                "Value": value,
                "Unit": unit,
                "Timestamp": datetime.now(timezone.utc),
            }
            
            if dimensions:
                metric_data["Dimensions"] = [
                    {"Name": k, "Value": v} for k, v in dimensions.items()
                ]
            
            self._client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[metric_data],
            )
            
            logger.debug(f"Published metric: {metric_name}={value} {unit}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to publish CloudWatch metric: {e}")
            return False
    
    def record_success(
        self,
        record_count: int,
        duration_seconds: float | None = None,
    ) -> None:
        """
        Record a successful ingestion.
        
        Args:
            record_count: Number of records ingested
            duration_seconds: Time taken for ingestion
        """
        dimensions = {
            "Environment": notification_settings.environment,
            "Service": notification_settings.service_name,
        }
        
        self._put_metric("IngestionSuccess", 1, "Count", dimensions)
        self._put_metric("IngestionRecordCount", record_count, "Count", dimensions)
        
        if duration_seconds is not None:
            self._put_metric("IngestionDuration", duration_seconds, "Seconds", dimensions)
        
        logger.info(f"Recorded success metrics: records={record_count}")
    
    def record_failure(
        self,
        error_category: str,
        duration_seconds: float | None = None,
    ) -> None:
        """
        Record a failed ingestion.
        
        Args:
            error_category: Category of the error (e.g., API_ERROR, S3_ERROR)
            duration_seconds: Time taken before failure
        """
        dimensions = {
            "Environment": notification_settings.environment,
            "Service": notification_settings.service_name,
        }
        
        self._put_metric("IngestionFailure", 1, "Count", dimensions)
        
        # Also publish with error category dimension
        dimensions_with_error = {**dimensions, "ErrorCategory": error_category}
        self._put_metric("IngestionFailureByCategory", 1, "Count", dimensions_with_error)
        
        if duration_seconds is not None:
            self._put_metric("IngestionDuration", duration_seconds, "Seconds", dimensions)
        
        logger.info(f"Recorded failure metrics: category={error_category}")


def create_cloudwatch_publisher() -> CloudWatchPublisher:
    """Create a new CloudWatch publisher."""
    return CloudWatchPublisher()


__all__ = ["CloudWatchPublisher", "create_cloudwatch_publisher"]

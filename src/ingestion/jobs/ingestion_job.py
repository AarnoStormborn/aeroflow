"""
Ingestion job that fetches state vectors and stores them.

This is the main pipeline that:
1. Fetches state vectors from OpenSky API (for configured region)
2. Converts to Parquet using Polars
3. Uploads to S3
4. Records the ingestion in SQLite

All failures are captured with appropriate error messages and status.
"""

import traceback
from datetime import datetime, timezone
from typing import Callable

from src.utils import logger
from src.utils.exceptions import (
    FlightServiceError,
    OpenSkyAPIError,
    RateLimitError,
    APIConnectionError,
    APITimeoutError,
    S3UploadError,
    S3ConfigurationError,
    ParquetError,
    DatabaseError,
    ConfigurationError,
)
from src.ingestion.config import settings
from src.ingestion.components.client import OpenSkyClient, create_client
from src.ingestion.components.s3_uploader import S3Uploader, create_uploader
from src.notifications import get_notifier
from src.ingestion.db import (
    IngestionRepository,
    IngestionStatus,
    IngestionRecord,
    create_repository,
)


class IngestionJob:
    """
    Main ingestion job that orchestrates the data pipeline.
    
    Workflow:
    1. Create pending ingestion record
    2. Fetch state vectors from OpenSky API (with bounding box)
    3. Convert to Parquet and upload to S3
    4. Update ingestion record with results
    
    All failures are captured in the database with:
    - status = FAILED
    - error_message = Detailed error description
    """
    
    def __init__(
        self,
        client: OpenSkyClient | None = None,
        uploader: S3Uploader | None = None,
        repository: IngestionRepository | None = None,
    ):
        """
        Initialize the ingestion job.
        
        Args:
            client: OpenSky API client (created if not provided)
            uploader: S3 uploader (created if not provided)
            repository: Ingestion record repository (created if not provided)
            
        Raises:
            ConfigurationError: If components cannot be initialized
        """
        try:
            self.client = client or create_client()
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize OpenSky client: {e}")
        
        try:
            self.uploader = uploader or create_uploader()
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize S3 uploader: {e}")
        
        try:
            self.repository = repository or create_repository()
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize database repository: {e}")
        
        logger.info("IngestionJob initialized successfully")
    
    def _categorize_error(self, error: Exception) -> tuple[str, str]:
        """
        Categorize an error for logging and storage.
        
        Returns:
            Tuple of (error_category, error_message)
        """
        if isinstance(error, RateLimitError):
            category = "RATE_LIMIT"
            message = f"API rate limit exceeded. Retry after {error.retry_after}s"
        elif isinstance(error, APITimeoutError):
            category = "API_TIMEOUT"
            message = f"API request timed out after {error.timeout}s"
        elif isinstance(error, APIConnectionError):
            category = "API_CONNECTION"
            message = f"Failed to connect to OpenSky API: {error}"
        elif isinstance(error, OpenSkyAPIError):
            category = "API_ERROR"
            message = f"OpenSky API error (HTTP {error.status_code}): {error.message}"
        elif isinstance(error, S3ConfigurationError):
            category = "S3_CONFIG"
            message = f"S3 configuration error: {error}"
        elif isinstance(error, S3UploadError):
            category = "S3_UPLOAD"
            message = f"Failed to upload to S3 ({error.bucket}/{error.key}): {error.message}"
        elif isinstance(error, ParquetError):
            category = "PARQUET"
            message = f"Failed to create Parquet file: {error}"
        elif isinstance(error, DatabaseError):
            category = "DATABASE"
            message = f"Database error: {error}"
        elif isinstance(error, ConfigurationError):
            category = "CONFIG"
            message = f"Configuration error: {error}"
        elif isinstance(error, FlightServiceError):
            category = "SERVICE"
            message = f"Service error: {error}"
        else:
            category = "UNEXPECTED"
            message = f"Unexpected error ({type(error).__name__}): {error}"
        
        return category, message
    
    def run(self) -> IngestionRecord:
        """
        Run the states ingestion pipeline.
        
        Returns:
            IngestionRecord with the result (status will be SUCCESS or FAILED)
            
        The method never raises exceptions - all errors are captured in
        the returned record's status and error_message fields.
        """
        logger.info("Starting states ingestion job")
        
        now = datetime.now(timezone.utc)
        bbox = settings.opensky.bounding_box
        
        logger.info(f"Fetching states for region: lamin={bbox[0]}, lomin={bbox[1]}, lamax={bbox[2]}, lomax={bbox[3]}")
        
        # Create pending record
        try:
            record = self.repository.create_record(
                time_window_start=now,
                time_window_end=now,
                status=IngestionStatus.PENDING,
            )
        except Exception as e:
            # Can't even create the tracking record - log and return a dummy record
            logger.exception(f"CRITICAL: Failed to create ingestion record: {e}")
            return IngestionRecord(
                id=None,
                created_at=now,
                time_window_start=now,
                time_window_end=now,
                s3_path=None,
                record_count=0,
                status=IngestionStatus.FAILED,
                error_message=f"Failed to create tracking record: {e}",
            )
        
        try:
            # Step 1: Fetch states from API with bounding box
            logger.info("Step 1/3: Fetching state vectors from OpenSky API...")
            states_response = self.client.get_states(bounding_box=bbox)
            
            states = states_response.get("states", [])
            if not states:
                logger.warning("No state vectors returned from API (empty response)")
                return self.repository.update_record(
                    record_id=record.id,
                    record_count=0,
                    status=IngestionStatus.SUCCESS,
                )
            
            logger.info(f"Step 1/3: Fetched {len(states)} state vectors")
            
            # Step 2: Convert to Parquet and upload to S3
            logger.info("Step 2/3: Converting to Parquet and uploading to S3...")
            s3_path, record_count = self.uploader.upload_states(
                states_response=states_response,
                timestamp=now,
            )
            
            logger.info(f"Step 2/3: Uploaded {record_count} records to {s3_path}")
            
            # Step 3: Update record with success
            logger.info("Step 3/3: Updating ingestion record...")
            updated_record = self.repository.update_record(
                record_id=record.id,
                s3_path=s3_path,
                record_count=record_count,
                status=IngestionStatus.SUCCESS,
            )
            
            logger.info(f"Ingestion complete: {record_count} records stored at {s3_path}")
            
            # Send success notification (metrics only, no alert)
            try:
                duration = (datetime.now(timezone.utc) - now).total_seconds()
                get_notifier().on_success(
                    record_id=updated_record.id,
                    record_count=record_count,
                    s3_path=s3_path,
                    duration_seconds=duration,
                )
            except Exception as notify_error:
                logger.warning(f"Failed to send success notification: {notify_error}")
            
            return updated_record
            
        except Exception as e:
            # Categorize and log the error
            category, message = self._categorize_error(e)
            
            logger.error(f"Ingestion FAILED [{category}]: {message}")
            logger.debug(f"Full traceback:\n{traceback.format_exc()}")
            
            # Send failure notification (metrics + SNS alert)
            try:
                duration = (datetime.now(timezone.utc) - now).total_seconds()
                get_notifier().on_failure(
                    record_id=record.id,
                    error_category=category,
                    error_message=message,
                    duration_seconds=duration,
                )
            except Exception as notify_error:
                logger.warning(f"Failed to send failure notification: {notify_error}")
            
            # Update record with failure
            try:
                return self.repository.update_record(
                    record_id=record.id,
                    status=IngestionStatus.FAILED,
                    error_message=f"[{category}] {message}",
                )
            except Exception as update_error:
                # Even the update failed - log both errors
                logger.exception(f"CRITICAL: Failed to update record with error: {update_error}")
                return IngestionRecord(
                    id=record.id,
                    created_at=record.created_at,
                    time_window_start=record.time_window_start,
                    time_window_end=record.time_window_end,
                    s3_path=None,
                    record_count=0,
                    status=IngestionStatus.FAILED,
                    error_message=f"[{category}] {message} (also failed to update record: {update_error})",
                )


def run_ingestion() -> IngestionRecord:
    """
    Run a single ingestion cycle.
    
    Returns:
        IngestionRecord with the result
        
    Never raises exceptions - all errors are captured in the returned record.
    """
    try:
        job = IngestionJob()
        return job.run()
    except ConfigurationError as e:
        # Failed during initialization
        logger.error(f"Failed to initialize ingestion job: {e}")
        return IngestionRecord(
            id=None,
            created_at=datetime.now(timezone.utc),
            time_window_start=datetime.now(timezone.utc),
            time_window_end=datetime.now(timezone.utc),
            s3_path=None,
            record_count=0,
            status=IngestionStatus.FAILED,
            error_message=f"[CONFIG] {e}",
        )
    except Exception as e:
        # Truly unexpected error
        logger.exception(f"Unexpected error running ingestion: {e}")
        return IngestionRecord(
            id=None,
            created_at=datetime.now(timezone.utc),
            time_window_start=datetime.now(timezone.utc),
            time_window_end=datetime.now(timezone.utc),
            s3_path=None,
            record_count=0,
            status=IngestionStatus.FAILED,
            error_message=f"[UNEXPECTED] {e}",
        )


__all__ = ["IngestionJob", "run_ingestion"]

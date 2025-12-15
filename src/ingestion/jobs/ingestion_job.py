"""
Ingestion job that fetches flight data and stores it.

This is the main pipeline that:
1. Fetches flight data from OpenSky API
2. Converts to Parquet using Polars
3. Uploads to S3
4. Records the ingestion in SQLite
"""

from datetime import datetime, timezone, timedelta
from typing import Literal

from src.utils import logger
from src.utils.exceptions import (
    OpenSkyAPIError,
    RateLimitError,
    APIConnectionError,
    APITimeoutError,
    S3UploadError,
    ParquetError,
)
from src.ingestion.config import settings
from src.ingestion.components.client import OpenSkyClient, create_client
from src.ingestion.components.s3_uploader import S3Uploader, create_uploader
from src.ingestion.db import (
    IngestionRepository,
    IngestionStatus,
    IngestionRecord,
    create_repository,
)


DataType = Literal["flights", "states"]


class IngestionJob:
    """
    Main ingestion job that orchestrates the data pipeline.
    
    Workflow:
    1. Calculate time window for data fetch
    2. Create pending ingestion record
    3. Fetch data from OpenSky API
    4. Convert to Parquet and upload to S3
    5. Update ingestion record with results
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
            client: OpenSky API client
            uploader: S3 uploader
            repository: Ingestion record repository
        """
        self.client = client or create_client()
        self.uploader = uploader or create_uploader()
        self.repository = repository or create_repository()
        
        logger.info("IngestionJob initialized")
    
    def _calculate_time_window(self) -> tuple[datetime, datetime]:
        """
        Calculate the time window for the current fetch.
        
        Returns:
            Tuple of (start_time, end_time) as datetime objects
        """
        now = datetime.now(timezone.utc)
        window_seconds = settings.scheduler.fetch_window_seconds
        
        # Round down to the previous interval boundary
        interval_seconds = settings.scheduler.polling_interval_minutes * 60
        epoch_seconds = int(now.timestamp())
        aligned_end = (epoch_seconds // interval_seconds) * interval_seconds
        aligned_start = aligned_end - window_seconds
        
        start_time = datetime.fromtimestamp(aligned_start, tz=timezone.utc)
        end_time = datetime.fromtimestamp(aligned_end, tz=timezone.utc)
        
        return start_time, end_time
    
    def run_flights_ingestion(self) -> IngestionRecord:
        """
        Run the flights ingestion pipeline.
        
        Returns:
            IngestionRecord with the result
        """
        logger.info("Starting flights ingestion job")
        
        # Calculate time window
        start_time, end_time = self._calculate_time_window()
        logger.info(f"Time window: {start_time} to {end_time}")
        
        # Create pending record
        record = self.repository.create_record(
            time_window_start=start_time,
            time_window_end=end_time,
            status=IngestionStatus.PENDING,
        )
        
        try:
            # Fetch flights from API
            flights = self.client.get_flights_by_time(
                begin=int(start_time.timestamp()),
                end=int(end_time.timestamp()),
            )
            
            if not flights:
                logger.warning("No flights returned from API")
                return self.repository.update_record(
                    record_id=record.id,
                    record_count=0,
                    status=IngestionStatus.SUCCESS,
                )
            
            logger.info(f"Fetched {len(flights)} flights from OpenSky API")
            
            # Upload to S3
            s3_path, record_count = self.uploader.upload_flights(
                flights=flights,
                timestamp=end_time,
            )
            
            # Update record with success
            updated_record = self.repository.update_record(
                record_id=record.id,
                s3_path=s3_path,
                record_count=record_count,
                status=IngestionStatus.SUCCESS,
            )
            
            logger.info(f"Ingestion complete: {record_count} records stored at {s3_path}")
            return updated_record
            
        except RateLimitError as e:
            logger.warning(f"Rate limit hit: {e.message}, retry after: {e.retry_after}s")
            return self.repository.update_record(
                record_id=record.id,
                status=IngestionStatus.FAILED,
                error_message=f"Rate limit exceeded. Retry after {e.retry_after}s",
            )
            
        except (OpenSkyAPIError, APIConnectionError, APITimeoutError) as e:
            logger.error(f"API error during ingestion: {e}")
            return self.repository.update_record(
                record_id=record.id,
                status=IngestionStatus.FAILED,
                error_message=str(e),
            )
            
        except (S3UploadError, ParquetError) as e:
            logger.error(f"Storage error during ingestion: {e}")
            return self.repository.update_record(
                record_id=record.id,
                status=IngestionStatus.FAILED,
                error_message=str(e),
            )
            
        except Exception as e:
            logger.exception(f"Unexpected error during ingestion: {e}")
            return self.repository.update_record(
                record_id=record.id,
                status=IngestionStatus.FAILED,
                error_message=f"Unexpected error: {e}",
            )
    
    def run_states_ingestion(self) -> IngestionRecord:
        """
        Run the state vectors ingestion pipeline.
        
        Returns:
            IngestionRecord with the result
        """
        logger.info("Starting states ingestion job")
        
        now = datetime.now(timezone.utc)
        
        # For states, we use current time as both start and end
        # since we're fetching real-time data
        record = self.repository.create_record(
            time_window_start=now,
            time_window_end=now,
            status=IngestionStatus.PENDING,
        )
        
        try:
            # Fetch current states
            states_response = self.client.get_states()
            
            states = states_response.get("states", [])
            if not states:
                logger.warning("No state vectors returned from API")
                return self.repository.update_record(
                    record_id=record.id,
                    record_count=0,
                    status=IngestionStatus.SUCCESS,
                )
            
            logger.info(f"Fetched {len(states)} state vectors from OpenSky API")
            
            # Upload to S3
            s3_path, record_count = self.uploader.upload_states(
                states_response=states_response,
                timestamp=now,
            )
            
            # Update record with success
            updated_record = self.repository.update_record(
                record_id=record.id,
                s3_path=s3_path,
                record_count=record_count,
                status=IngestionStatus.SUCCESS,
            )
            
            logger.info(f"States ingestion complete: {record_count} records stored at {s3_path}")
            return updated_record
            
        except RateLimitError as e:
            logger.warning(f"Rate limit hit: {e.message}")
            return self.repository.update_record(
                record_id=record.id,
                status=IngestionStatus.FAILED,
                error_message=f"Rate limit exceeded. Retry after {e.retry_after}s",
            )
            
        except (OpenSkyAPIError, APIConnectionError, APITimeoutError) as e:
            logger.error(f"API error during states ingestion: {e}")
            return self.repository.update_record(
                record_id=record.id,
                status=IngestionStatus.FAILED,
                error_message=str(e),
            )
            
        except (S3UploadError, ParquetError) as e:
            logger.error(f"Storage error during states ingestion: {e}")
            return self.repository.update_record(
                record_id=record.id,
                status=IngestionStatus.FAILED,
                error_message=str(e),
            )
            
        except Exception as e:
            logger.exception(f"Unexpected error during states ingestion: {e}")
            return self.repository.update_record(
                record_id=record.id,
                status=IngestionStatus.FAILED,
                error_message=f"Unexpected error: {e}",
            )


def run_ingestion(data_type: DataType = "flights") -> IngestionRecord:
    """
    Run a single ingestion cycle.
    
    Args:
        data_type: Type of data to ingest ("flights" or "states")
        
    Returns:
        IngestionRecord with the result
    """
    job = IngestionJob()
    
    if data_type == "flights":
        return job.run_flights_ingestion()
    else:
        return job.run_states_ingestion()


__all__ = ["IngestionJob", "run_ingestion", "DataType"]

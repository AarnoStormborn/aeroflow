"""
Test script to run full ingestion flow with S3.

Fetches flight data from OpenSky API and uploads to S3 bucket.
"""

from datetime import datetime, timezone, timedelta

from src.utils import logger
from src.utils.logger import setup_logger
from src.ingestion.components.client import OpenSkyClient
from src.ingestion.components.s3_uploader import S3Uploader
from src.ingestion.db import IngestionRepository, IngestionStatus
from src.ingestion.config import settings


def run_s3_test():
    """Run a test ingestion with S3 storage."""
    
    # Setup logger
    setup_logger(log_level="DEBUG")
    
    logger.info("=" * 60)
    logger.info("S3 INGESTION TEST")
    logger.info("=" * 60)
    
    # Log configuration
    logger.info(f"S3 Bucket: {settings.s3.bucket_name}")
    logger.info(f"S3 Prefix: {settings.s3.prefix}")
    logger.info(f"S3 Region: {settings.s3.region}")
    if settings.s3.endpoint_url:
        logger.info(f"S3 Endpoint: {settings.s3.endpoint_url}")
    
    # Initialize components
    client = OpenSkyClient()
    uploader = S3Uploader()
    repository = IngestionRepository()
    
    # Calculate time window (last 2 hours - this is what OpenSky allows)
    now = datetime.now(timezone.utc)
    # Use a 2-hour window ending at least 1 hour ago (OpenSky has ~1hr delay)
    end_time = now - timedelta(hours=1)
    start_time = end_time - timedelta(hours=2)
    
    logger.info(f"Time window: {start_time} to {end_time}")
    
    # Create pending record
    record = repository.create_record(
        time_window_start=start_time,
        time_window_end=end_time,
        status=IngestionStatus.PENDING,
    )
    logger.info(f"Created ingestion record: {record.id}")
    
    try:
        # Fetch flights from API
        logger.info("Fetching flights from OpenSky API...")
        flights = client.get_flights_by_time(
            begin=int(start_time.timestamp()),
            end=int(end_time.timestamp()),
        )
        
        if not flights:
            logger.warning("No flights returned from API")
            repository.update_record(
                record_id=record.id,
                record_count=0,
                status=IngestionStatus.SUCCESS,
            )
            return
        
        logger.info(f"Fetched {len(flights)} flights")
        
        # Show sample of data
        logger.info("Sample flight data:")
        for flight in flights[:3]:
            logger.info(f"  {flight.get('callsign', 'N/A')} | "
                       f"{flight.get('estDepartureAirport', '?')} → "
                       f"{flight.get('estArrivalAirport', '?')}")
        
        # Upload to S3
        logger.info("Uploading to S3...")
        s3_path, record_count = uploader.upload_flights(
            flights=flights,
            timestamp=end_time,
        )
        
        # Update database record
        updated_record = repository.update_record(
            record_id=record.id,
            s3_path=s3_path,
            record_count=record_count,
            status=IngestionStatus.SUCCESS,
        )
        
        logger.info("=" * 60)
        logger.info("TEST COMPLETE!")
        logger.info(f"  Records: {record_count}")
        logger.info(f"  S3 Path: {s3_path}")
        logger.info(f"  Status: {updated_record.status.value}")
        logger.info("=" * 60)
        
        # Verify by listing the uploaded file using the uploader's client
        # (which already has credentials configured)
        
        # Parse the s3 path to get bucket and key
        # s3://bucket/key -> bucket, key
        path_parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ""
        
        response = uploader._client.head_object(Bucket=bucket, Key=key)
        logger.info(f"\nVerified S3 object:")
        logger.info(f"  Size: {response['ContentLength']} bytes")
        logger.info(f"  Last Modified: {response['LastModified']}")
        
    except Exception as e:
        logger.exception(f"Test failed: {e}")
        repository.update_record(
            record_id=record.id,
            status=IngestionStatus.FAILED,
            error_message=str(e),
        )
        raise


if __name__ == "__main__":
    run_s3_test()

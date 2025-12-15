"""
Test script to run ingestion with local file storage.

Fetches flight data from OpenSky API and saves to samples/ folder.
"""

from datetime import datetime, timezone, timedelta

from src.utils import logger
from src.utils.logger import setup_logger
from src.ingestion.components.client import OpenSkyClient
from src.ingestion.components.local_storage import LocalStorage
from src.ingestion.db import IngestionRepository, IngestionStatus


def run_local_test():
    """Run a test ingestion with local storage."""
    
    # Setup logger
    setup_logger(log_level="DEBUG")
    
    logger.info("=" * 60)
    logger.info("LOCAL INGESTION TEST")
    logger.info("=" * 60)
    
    # Initialize components
    client = OpenSkyClient()
    storage = LocalStorage(base_dir="samples")
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
        
        # Save to local storage
        file_path, record_count = storage.save_flights(
            flights=flights,
            timestamp=end_time,
        )
        
        # Update database record
        updated_record = repository.update_record(
            record_id=record.id,
            s3_path=file_path,  # Actually local path in this case
            record_count=record_count,
            status=IngestionStatus.SUCCESS,
        )
        
        logger.info("=" * 60)
        logger.info("TEST COMPLETE!")
        logger.info(f"  Records: {record_count}")
        logger.info(f"  File: {file_path}")
        logger.info(f"  Status: {updated_record.status.value}")
        logger.info("=" * 60)
        
        # Verify parquet file
        import polars as pl
        df = pl.read_parquet(file_path)
        logger.info(f"\nParquet file preview:")
        print(df.head(5))
        print(f"\nSchema: {df.schema}")
        
    except Exception as e:
        logger.exception(f"Test failed: {e}")
        repository.update_record(
            record_id=record.id,
            status=IngestionStatus.FAILED,
            error_message=str(e),
        )


if __name__ == "__main__":
    run_local_test()

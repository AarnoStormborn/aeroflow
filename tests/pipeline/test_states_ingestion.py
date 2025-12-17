"""
Test script to run states ingestion with local file storage.

Fetches real-time state vectors from OpenSky API and saves to samples/ folder.
"""

from datetime import datetime, timezone

from src.utils import logger
from src.utils.logger import setup_logger
from src.ingestion.components.client import OpenSkyClient
from src.ingestion.components.local_storage import LocalStorage
from src.ingestion.db import IngestionRepository, IngestionStatus


def run_states_test():
    """Run a test states ingestion with local storage."""
    
    # Setup logger
    setup_logger(log_level="DEBUG")
    
    logger.info("=" * 60)
    logger.info("LOCAL STATES INGESTION TEST")
    logger.info("=" * 60)
    
    # Initialize components
    client = OpenSkyClient()
    storage = LocalStorage(base_dir="samples")
    repository = IngestionRepository()
    
    now = datetime.now(timezone.utc)
    
    # Create pending record
    record = repository.create_record(
        time_window_start=now,
        time_window_end=now,
        status=IngestionStatus.PENDING,
    )
    logger.info(f"Created ingestion record: {record.id}")
    
    try:
        # Mumbai airspace bounding box
        MUMBAI_BBOX = (18.0, 71.5, 20.0, 74.0)  # lamin, lomin, lamax, lomax
        
        # Fetch state vectors from API for Mumbai region
        logger.info("Fetching state vectors from OpenSky API (/states/all)...")
        logger.info(f"Bounding box: lamin={MUMBAI_BBOX[0]}, lomin={MUMBAI_BBOX[1]}, lamax={MUMBAI_BBOX[2]}, lomax={MUMBAI_BBOX[3]}")
        states_response = client.get_states(bounding_box=MUMBAI_BBOX)
        
        states = states_response.get("states", [])
        if not states:
            logger.warning("No states returned from API")
            repository.update_record(
                record_id=record.id,
                record_count=0,
                status=IngestionStatus.SUCCESS,
            )
            return
        
        logger.info(f"Fetched {len(states)} state vectors")
        logger.info(f"Capture time: {states_response.get('time')}")
        
        # Show sample of data (states are arrays, not dicts)
        # Format: [icao24, callsign, origin_country, time_position, last_contact, 
        #          longitude, latitude, baro_altitude, on_ground, velocity, ...]
        logger.info("Sample state data (icao24, callsign, origin_country, lat, lon, altitude):")
        for state in states[:5]:
            logger.info(f"  {state[0]} | {state[1] or 'N/A':10s} | {state[2]:20s} | "
                       f"lat={state[6]}, lon={state[5]}, alt={state[7]}")
        
        # Save to local storage
        file_path, record_count = storage.save_states(
            states_response=states_response,
            timestamp=now,
        )
        
        # Update database record
        updated_record = repository.update_record(
            record_id=record.id,
            s3_path=file_path,
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
        print(df.head(10))
        print(f"\nSchema: {df.schema}")
        
        # Create JSON sample
        import json
        sample = df.head(20).to_dicts()
        json_path = "samples/states_sample.json"
        with open(json_path, "w") as f:
            json.dump(sample, f, indent=2)
        logger.info(f"\nCreated JSON sample: {json_path}")
        
    except Exception as e:
        logger.exception(f"Test failed: {e}")
        repository.update_record(
            record_id=record.id,
            status=IngestionStatus.FAILED,
            error_message=str(e),
        )


if __name__ == "__main__":
    run_states_test()

"""
Ingestion service for flight data from OpenSky Network.

This module provides:
- OpenSky API client for fetching state vectors
- S3 uploader for storing Parquet files
- SQLite repository for tracking ingestion runs
- Scheduler for periodic polling

Quick start:
    from src.ingestion import start_scheduler
    start_scheduler()  # Start the scheduled ingestion
    
One-time run:
    from src.ingestion import run_ingestion
    result = run_ingestion()  # Run a single ingestion
    
Configuration (environment variables):
    SCHEDULER_INTERVAL_SECONDS: Polling interval (default: 60)
    OPENSKY_BBOX_LAMIN/LOMIN/LAMAX/LOMAX: Region bounding box
    AWS_S3_BUCKET_NAME: S3 bucket for storage
    AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY: AWS credentials
"""

from src.ingestion.config import settings, get_settings
from src.ingestion.components import (
    OpenSkyClient,
    create_client,
    S3Uploader,
    create_uploader,
)
from src.ingestion.db import (
    IngestionStatus,
    IngestionRecord,
    IngestionRepository,
    create_repository,
)
from src.ingestion.jobs import (
    IngestionJob,
    run_ingestion,
    IngestionScheduler,
    create_scheduler,
    start_scheduler,
)

__all__ = [
    # Config
    "settings",
    "get_settings",
    # Components
    "OpenSkyClient",
    "create_client",
    "S3Uploader",
    "create_uploader",
    # Database
    "IngestionStatus",
    "IngestionRecord",
    "IngestionRepository",
    "create_repository",
    # Jobs
    "IngestionJob",
    "run_ingestion",
    "IngestionScheduler",
    "create_scheduler",
    "start_scheduler",
]

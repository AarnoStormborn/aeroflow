"""
Ingestion service for flight data from OpenSky Network.

This module provides:
- OpenSky API client for fetching flight data
- S3 uploader for storing Parquet files
- SQLite repository for tracking ingestion runs
- Scheduler for periodic polling (every 15 minutes)

Quick start:
    from src.ingestion.jobs import start_scheduler
    start_scheduler("flights")  # Start the scheduled ingestion
    
One-time run:
    from src.ingestion.jobs import run_ingestion
    result = run_ingestion("flights")  # Run a single ingestion
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

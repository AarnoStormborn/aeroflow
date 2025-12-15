"""
Scheduler for periodic ingestion jobs.

Uses APScheduler to run ingestion every 15 minutes (configurable).
"""

import signal
import sys
from datetime import datetime, timezone
from typing import Callable

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobExecutionEvent

from src.utils import logger
from src.utils.logger import setup_logger
from src.ingestion.config import settings
from src.ingestion.jobs.ingestion_job import IngestionJob, run_ingestion


class IngestionScheduler:
    """
    Scheduler for running periodic ingestion jobs.
    
    Features:
    - Configurable polling interval
    - Graceful shutdown handling
    - Job execution logging
    - Error monitoring
    """
    
    def __init__(
        self,
        interval_minutes: int | None = None,
        run_on_start: bool = True,
    ):
        """
        Initialize the scheduler.
        
        Args:
            interval_minutes: Polling interval in minutes
            run_on_start: Whether to run ingestion immediately on start
        """
        self.interval_minutes = interval_minutes or settings.scheduler.polling_interval_minutes
        self.run_on_start = run_on_start
        
        self._scheduler = BlockingScheduler()
        self._setup_listeners()
        self._setup_signal_handlers()
        
        logger.info(f"IngestionScheduler initialized with {self.interval_minutes} minute interval")
    
    def _setup_listeners(self) -> None:
        """Setup job event listeners."""
        self._scheduler.add_listener(
            self._on_job_executed,
            EVENT_JOB_EXECUTED,
        )
        self._scheduler.add_listener(
            self._on_job_error,
            EVENT_JOB_ERROR,
        )
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def _shutdown(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self._scheduler.shutdown(wait=False)
        sys.exit(0)
    
    def _on_job_executed(self, event: JobExecutionEvent) -> None:
        """Handle successful job execution."""
        logger.info(f"Job {event.job_id} executed successfully at {datetime.now(timezone.utc)}")
    
    def _on_job_error(self, event: JobExecutionEvent) -> None:
        """Handle job execution errors."""
        logger.error(f"Job {event.job_id} failed with exception: {event.exception}")
        logger.exception(event.traceback)
    
    def _run_flights_job(self) -> None:
        """Wrapper for flights ingestion job."""
        logger.info("=" * 60)
        logger.info("Starting scheduled flights ingestion")
        logger.info("=" * 60)
        
        try:
            result = run_ingestion("flights")
            logger.info(f"Ingestion completed with status: {result.status.value}")
            if result.s3_path:
                logger.info(f"Data stored at: {result.s3_path}")
            if result.error_message:
                logger.warning(f"Error message: {result.error_message}")
        except Exception as e:
            logger.exception(f"Unhandled error in flights ingestion job: {e}")
    
    def _run_states_job(self) -> None:
        """Wrapper for states ingestion job."""
        logger.info("=" * 60)
        logger.info("Starting scheduled states ingestion")
        logger.info("=" * 60)
        
        try:
            result = run_ingestion("states")
            logger.info(f"Ingestion completed with status: {result.status.value}")
            if result.s3_path:
                logger.info(f"Data stored at: {result.s3_path}")
            if result.error_message:
                logger.warning(f"Error message: {result.error_message}")
        except Exception as e:
            logger.exception(f"Unhandled error in states ingestion job: {e}")
    
    def add_flights_job(self) -> None:
        """Add flights ingestion job to the scheduler."""
        trigger = IntervalTrigger(minutes=self.interval_minutes)
        
        self._scheduler.add_job(
            self._run_flights_job,
            trigger=trigger,
            id="flights_ingestion",
            name="Flights Data Ingestion",
            replace_existing=True,
        )
        
        logger.info(f"Added flights ingestion job (every {self.interval_minutes} minutes)")
    
    def add_states_job(self) -> None:
        """Add states ingestion job to the scheduler."""
        trigger = IntervalTrigger(minutes=self.interval_minutes)
        
        self._scheduler.add_job(
            self._run_states_job,
            trigger=trigger,
            id="states_ingestion",
            name="State Vectors Ingestion",
            replace_existing=True,
        )
        
        logger.info(f"Added states ingestion job (every {self.interval_minutes} minutes)")
    
    def start(self, data_type: str = "flights") -> None:
        """
        Start the scheduler.
        
        Args:
            data_type: Type of data to ingest ("flights", "states", or "both")
        """
        logger.info("=" * 60)
        logger.info("STARTING INGESTION SCHEDULER")
        logger.info(f"Data type: {data_type}")
        logger.info(f"Interval: {self.interval_minutes} minutes")
        logger.info(f"Run on start: {self.run_on_start}")
        logger.info("=" * 60)
        
        # Add jobs based on data type
        if data_type in ("flights", "both"):
            self.add_flights_job()
        if data_type in ("states", "both"):
            self.add_states_job()
        
        # Run immediately if configured
        if self.run_on_start:
            logger.info("Running initial ingestion...")
            if data_type in ("flights", "both"):
                self._run_flights_job()
            if data_type in ("states", "both"):
                self._run_states_job()
        
        # Start the scheduler (blocking)
        logger.info("Scheduler started, waiting for next scheduled run...")
        self._scheduler.start()
    
    def stop(self) -> None:
        """Stop the scheduler."""
        logger.info("Stopping scheduler...")
        self._scheduler.shutdown(wait=True)
        logger.info("Scheduler stopped")


def create_scheduler(
    interval_minutes: int | None = None,
    run_on_start: bool = True,
) -> IngestionScheduler:
    """Create a new scheduler with the given settings."""
    return IngestionScheduler(
        interval_minutes=interval_minutes,
        run_on_start=run_on_start,
    )


def start_scheduler(data_type: str = "flights") -> None:
    """
    Convenience function to create and start a scheduler.
    
    Args:
        data_type: Type of data to ingest ("flights", "states", or "both")
    """
    # Reconfigure logger with settings
    setup_logger(
        log_level=settings.logging.level,
        log_dir=settings.logging.log_dir,
        log_file=settings.logging.log_file,
        rotation=settings.logging.rotation,
        retention=settings.logging.retention,
    )
    
    scheduler = create_scheduler()
    scheduler.start(data_type)


__all__ = [
    "IngestionScheduler",
    "create_scheduler",
    "start_scheduler",
]

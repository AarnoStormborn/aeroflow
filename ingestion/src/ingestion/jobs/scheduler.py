"""
Scheduler for periodic ingestion jobs.

Uses APScheduler to run ingestion at configurable intervals (in seconds).
"""

import signal
import sys
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobExecutionEvent

from src.utils import logger
from src.utils.logger import setup_logger
from src.ingestion.config import settings
from src.ingestion.jobs.ingestion_job import run_ingestion


class IngestionScheduler:
    """
    Scheduler for running periodic ingestion jobs.
    
    Features:
    - Configurable polling interval (in seconds)
    - Graceful shutdown handling
    - Job execution logging
    - Error monitoring
    """
    
    def __init__(
        self,
        interval_seconds: int | None = None,
        run_on_start: bool | None = None,
    ):
        """
        Initialize the scheduler.
        
        Args:
            interval_seconds: Polling interval in seconds (default from settings)
            run_on_start: Whether to run ingestion immediately on start
        """
        self.interval_seconds = interval_seconds or settings.scheduler.interval_seconds
        self.run_on_start = run_on_start if run_on_start is not None else settings.scheduler.run_on_start
        
        self._scheduler = BlockingScheduler()
        self._setup_listeners()
        self._setup_signal_handlers()
        
        logger.info(f"IngestionScheduler initialized with {self.interval_seconds}s interval")
    
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
    
    def _run_states_job(self) -> None:
        """Wrapper for states ingestion job."""
        logger.info("=" * 60)
        logger.info("Starting scheduled states ingestion")
        logger.info(f"Bounding box: {settings.opensky.bounding_box}")
        logger.info("=" * 60)
        
        try:
            result = run_ingestion()
            logger.info(f"Ingestion completed with status: {result.status.value}")
            logger.info(f"Records: {result.record_count}")
            if result.s3_path:
                logger.info(f"Data stored at: {result.s3_path}")
            if result.error_message:
                logger.warning(f"Error message: {result.error_message}")
        except Exception as e:
            logger.exception(f"Unhandled error in states ingestion job: {e}")
    
    def add_states_job(self) -> None:
        """Add states ingestion job to the scheduler."""
        trigger = IntervalTrigger(seconds=self.interval_seconds)
        
        self._scheduler.add_job(
            self._run_states_job,
            trigger=trigger,
            id="states_ingestion",
            name="State Vectors Ingestion",
            replace_existing=True,
        )
        
        logger.info(f"Added states ingestion job (every {self.interval_seconds}s)")
    
    def start(self) -> None:
        """Start the scheduler."""
        logger.info("=" * 60)
        logger.info("STARTING INGESTION SCHEDULER")
        logger.info(f"Interval: {self.interval_seconds} seconds")
        logger.info(f"Run on start: {self.run_on_start}")
        logger.info(f"Region bounding box: {settings.opensky.bounding_box}")
        logger.info("=" * 60)
        
        # Add states job
        self.add_states_job()
        
        # Run immediately if configured
        if self.run_on_start:
            logger.info("Running initial ingestion...")
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
    interval_seconds: int | None = None,
    run_on_start: bool | None = None,
) -> IngestionScheduler:
    """Create a new scheduler with the given settings."""
    return IngestionScheduler(
        interval_seconds=interval_seconds,
        run_on_start=run_on_start,
    )


def start_scheduler() -> None:
    """
    Convenience function to create and start a scheduler.
    
    Uses settings from environment variables.
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
    scheduler.start()


__all__ = [
    "IngestionScheduler",
    "create_scheduler",
    "start_scheduler",
]

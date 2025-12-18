"""
Flights Forecasting Services - Entry Point

Run the ingestion scheduler:
    python main.py
    
Or import and use programmatically:
    from src.ingestion import run_ingestion, start_scheduler

Environment variables:
    SCHEDULER_INTERVAL_SECONDS: Polling interval (default: 60s)
    SCHEDULER_RUN_ON_START: Run immediately on start (default: true)
    OPENSKY_BBOX_LAMIN/LOMIN/LAMAX/LOMAX: Bounding box coordinates
"""

import argparse

from src.utils.logger import setup_logger, logger
from src.ingestion.config import settings

from dotenv import load_dotenv
load_dotenv()


def main():
    """Main entry point for the services."""
    parser = argparse.ArgumentParser(
        description="Flights Forecasting Ingestion Service"
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run a single ingestion instead of scheduling",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Polling interval in seconds (default: from settings)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Log level (default: from settings)",
    )
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = args.log_level or settings.logging.level
    setup_logger(
        log_level=log_level,
        log_dir=settings.logging.log_dir,
        log_file=settings.logging.log_file,
    )
    
    logger.info("=" * 60)
    logger.info("FLIGHTS FORECASTING INGESTION SERVICE")
    logger.info("=" * 60)
    logger.info(f"Environment: {settings.environment}")
    logger.info(f"Bounding box: {settings.opensky.bounding_box}")
    logger.info(f"Run once: {args.run_once}")
    
    if args.run_once:
        # Run a single ingestion
        from src.ingestion import run_ingestion
        
        result = run_ingestion()
        logger.info(f"Ingestion result: {result.status.value}")
        logger.info(f"Records: {result.record_count}")
        if result.s3_path:
            logger.info(f"Data stored at: {result.s3_path}")
        if result.error_message:
            logger.error(f"Error: {result.error_message}")
    else:
        # Start the scheduler
        from src.ingestion import create_scheduler
        
        interval = args.interval or settings.scheduler.interval_seconds
        logger.info(f"Starting scheduler with {interval}s interval")
        
        scheduler = create_scheduler(interval_seconds=interval)
        scheduler.start()


if __name__ == "__main__":
    main()

"""
Flights Forecasting Services - Entry Point

Run the ingestion scheduler:
    python main.py
    
Or import and use programmatically:
    from src.ingestion import run_ingestion, start_scheduler
"""

import argparse

from src.utils.logger import setup_logger, logger
from src.ingestion.config import settings


def main():
    """Main entry point for the services."""
    parser = argparse.ArgumentParser(
        description="Flights Forecasting Ingestion Service"
    )
    parser.add_argument(
        "--data-type",
        choices=["flights", "states", "both"],
        default="flights",
        help="Type of data to ingest (default: flights)",
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
        help="Polling interval in minutes (default: from settings)",
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
    logger.info(f"Data type: {args.data_type}")
    logger.info(f"Run once: {args.run_once}")
    
    if args.run_once:
        # Run a single ingestion
        from src.ingestion import run_ingestion
        
        if args.data_type == "both":
            logger.info("Running both flights and states ingestion...")
            flights_result = run_ingestion("flights")
            states_result = run_ingestion("states")
            logger.info(f"Flights: {flights_result.status.value}, States: {states_result.status.value}")
        else:
            result = run_ingestion(args.data_type)
            logger.info(f"Ingestion result: {result.status.value}")
            if result.s3_path:
                logger.info(f"Data stored at: {result.s3_path}")
            if result.error_message:
                logger.error(f"Error: {result.error_message}")
    else:
        # Start the scheduler
        from src.ingestion import create_scheduler
        
        interval = args.interval or settings.scheduler.polling_interval_minutes
        scheduler = create_scheduler(interval_minutes=interval)
        scheduler.start(args.data_type)


if __name__ == "__main__":
    main()

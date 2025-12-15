"""
Loguru-based logging configuration for the flights-forecasting services.

Usage:
    from src.utils.logger import logger
    
    logger.info("Processing data...")
    logger.error("Something went wrong", exc_info=True)
"""

import sys
from pathlib import Path

from loguru import logger

# Remove default handler
logger.remove()

# Log format with timestamp, level, module, and message
LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)

# Simple format for file logging (no color codes)
FILE_LOG_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
    "{level: <8} | "
    "{name}:{function}:{line} | "
    "{message}"
)


def setup_logger(
    log_level: str = "DEBUG",
    log_dir: str | Path = "logs",
    log_file: str = "services.log",
    rotation: str = "10 MB",
    retention: str = "7 days",
    enable_stdout: bool = True,
    enable_file: bool = True,
) -> None:
    """
    Configure the logger with stdout and file handlers.
    
    Args:
        log_level: Minimum log level to capture (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory to store log files
        log_file: Name of the log file
        rotation: When to rotate the log file (e.g., "10 MB", "1 day", "00:00")
        retention: How long to keep old log files (e.g., "7 days", "1 week")
        enable_stdout: Whether to output logs to stdout
        enable_file: Whether to output logs to file
    """
    # Remove any existing handlers
    logger.remove()
    
    # Add stdout handler with color formatting
    if enable_stdout:
        logger.add(
            sys.stdout,
            format=LOG_FORMAT,
            level=log_level,
            colorize=True,
            backtrace=True,
            diagnose=True,
        )
    
    # Add file handler with rotation
    if enable_file:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        logger.add(
            log_path / log_file,
            format=FILE_LOG_FORMAT,
            level=log_level,
            rotation=rotation,
            retention=retention,
            compression="zip",
            backtrace=True,
            diagnose=True,
            enqueue=True,  # Thread-safe logging
        )
    
    logger.info(f"Logger initialized with level={log_level}")


# Initialize with defaults on import
# Can be reconfigured by calling setup_logger() with custom parameters
setup_logger()


# Export the configured logger
__all__ = ["logger", "setup_logger"]

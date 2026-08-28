"""
Utility modules for the flights-forecasting services.

Provides:
    - logger: Loguru-based logging with stdout and file output
    - exceptions: Custom exception classes for error handling
"""

from src.utils.exceptions import (
    APIConnectionError,
    # API
    APIError,
    APITimeoutError,
    # Configuration
    ConfigurationError,
    DatabaseConnectionError,
    # Database
    DatabaseError,
    # Base
    FlightServiceError,
    IngestionRecordError,
    MissingConfigError,
    OpenSkyAPIError,
    ParquetError,
    RateLimitError,
    S3ConfigurationError,
    S3Error,
    S3UploadError,
    # Storage
    StorageError,
)
from src.utils.logger import logger, setup_logger

__all__ = [
    # Logger
    "logger",
    "setup_logger",
    # Base
    "FlightServiceError",
    # API
    "APIError",
    "OpenSkyAPIError",
    "RateLimitError",
    "APIConnectionError",
    "APITimeoutError",
    # Storage
    "StorageError",
    "S3Error",
    "S3UploadError",
    "S3ConfigurationError",
    "ParquetError",
    # Database
    "DatabaseError",
    "IngestionRecordError",
    "DatabaseConnectionError",
    # Configuration
    "ConfigurationError",
    "MissingConfigError",
]

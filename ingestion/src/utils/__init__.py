"""
Utility modules for the flights-forecasting services.

Provides:
    - logger: Loguru-based logging with stdout and file output
    - exceptions: Custom exception classes for error handling
"""

from src.utils.logger import logger, setup_logger
from src.utils.exceptions import (
    # Base
    FlightServiceError,
    # API
    APIError,
    OpenSkyAPIError,
    RateLimitError,
    APIConnectionError,
    APITimeoutError,
    # Storage
    StorageError,
    S3Error,
    S3UploadError,
    S3ConfigurationError,
    ParquetError,
    # Database
    DatabaseError,
    IngestionRecordError,
    DatabaseConnectionError,
    # Configuration
    ConfigurationError,
    MissingConfigError,
)

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

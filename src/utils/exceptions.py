"""
Custom exceptions for the flights-forecasting services.

Provides a hierarchy of exceptions for different error scenarios:
- API errors (OpenSky API)
- Storage errors (S3, Parquet)
- Database errors (SQLite ingestion records)
"""


class FlightServiceError(Exception):
    """Base exception for all flight service errors."""
    
    def __init__(self, message: str, *args, **kwargs):
        self.message = message
        super().__init__(message, *args, **kwargs)


# =============================================================================
# API Exceptions
# =============================================================================

class APIError(FlightServiceError):
    """Base exception for API-related errors."""
    pass


class OpenSkyAPIError(APIError):
    """Error when communicating with the OpenSky API."""
    
    def __init__(self, message: str, status_code: int | None = None, response_body: str | None = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


class RateLimitError(APIError):
    """Error when API rate limit is exceeded."""
    
    def __init__(self, message: str = "API rate limit exceeded", retry_after: int | None = None):
        self.retry_after = retry_after
        super().__init__(message)


class APIConnectionError(APIError):
    """Error when unable to connect to the API."""
    pass


class APITimeoutError(APIError):
    """Error when API request times out."""
    pass


# =============================================================================
# Storage Exceptions
# =============================================================================

class StorageError(FlightServiceError):
    """Base exception for storage-related errors."""
    pass


class S3Error(StorageError):
    """Base exception for S3-related errors."""
    pass


class S3UploadError(S3Error):
    """Error when uploading to S3 fails."""
    
    def __init__(self, message: str, bucket: str | None = None, key: str | None = None):
        self.bucket = bucket
        self.key = key
        super().__init__(message)


class S3ConfigurationError(S3Error):
    """Error with S3 configuration (credentials, bucket, etc.)."""
    pass


class ParquetError(StorageError):
    """Error when creating or processing Parquet files."""
    pass


# =============================================================================
# Database Exceptions
# =============================================================================

class DatabaseError(FlightServiceError):
    """Base exception for database-related errors."""
    pass


class IngestionRecordError(DatabaseError):
    """Error when creating or querying ingestion records."""
    pass


class DatabaseConnectionError(DatabaseError):
    """Error when connecting to the database."""
    pass


# =============================================================================
# Configuration Exceptions
# =============================================================================

class ConfigurationError(FlightServiceError):
    """Error with service configuration."""
    pass


class MissingConfigError(ConfigurationError):
    """Error when required configuration is missing."""
    
    def __init__(self, config_key: str):
        self.config_key = config_key
        super().__init__(f"Missing required configuration: {config_key}")


# Export all exceptions
__all__ = [
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

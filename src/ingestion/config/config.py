"""
Configuration management for the ingestion service.

Loads settings from environment variables and YAML config files.
"""

import os
from pathlib import Path
from functools import lru_cache

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load .env file before settings are initialized
load_dotenv()


class OpenSkySettings(BaseSettings):
    """OpenSky API configuration."""
    
    base_url: str = Field(default="https://opensky-network.org/api")
    timeout_seconds: int = Field(default=30)
    # Optional authentication (for higher rate limits)
    username: str | None = Field(default=None)
    password: str | None = Field(default=None)
    
    model_config = SettingsConfigDict(env_prefix="OPENSKY_")


class S3Settings(BaseSettings):
    """AWS S3 configuration."""
    
    bucket_name: str = Field(default="flights-forecasting-data")
    prefix: str = Field(default="raw/flights")
    region: str = Field(default="us-east-1", validation_alias="AWS_REGION")
    # AWS credentials - use standard AWS env var names
    access_key_id: str | None = Field(default=None, validation_alias="AWS_ACCESS_KEY_ID")
    secret_access_key: str | None = Field(default=None, validation_alias="AWS_SECRET_ACCESS_KEY")
    # For local development with LocalStack or MinIO
    endpoint_url: str | None = Field(default=None)
    
    model_config = SettingsConfigDict(
        env_prefix="AWS_S3_",
        populate_by_name=True,  # Allow both alias and prefixed names
    )


class DatabaseSettings(BaseSettings):
    """SQLite database configuration."""
    
    path: str = Field(default="data/ingestion.db")
    
    model_config = SettingsConfigDict(env_prefix="DB_")
    
    @property
    def full_path(self) -> Path:
        """Get the full path to the database file."""
        return Path(self.path)


class SchedulerSettings(BaseSettings):
    """Scheduler configuration."""
    
    # Polling interval in minutes
    polling_interval_minutes: int = Field(default=15)
    # Time window for each fetch (in seconds, matches polling interval)
    fetch_window_seconds: int = Field(default=900)  # 15 minutes
    
    model_config = SettingsConfigDict(env_prefix="SCHEDULER_")


class LoggingSettings(BaseSettings):
    """Logging configuration."""
    
    level: str = Field(default="INFO")
    log_dir: str = Field(default="logs")
    log_file: str = Field(default="services.log")
    rotation: str = Field(default="10 MB")
    retention: str = Field(default="7 days")
    
    model_config = SettingsConfigDict(env_prefix="LOG_")


class Settings(BaseSettings):
    """Main settings aggregating all configuration."""
    
    opensky: OpenSkySettings = Field(default_factory=OpenSkySettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    
    # Environment name (development, staging, production)
    environment: str = Field(default="development")
    
    model_config = SettingsConfigDict(
        env_prefix="APP_",
        env_nested_delimiter="__",
    )


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Returns:
        Settings: The application settings
    """
    return Settings()


# Export for easy access
settings = get_settings()

__all__ = [
    "Settings",
    "OpenSkySettings",
    "S3Settings", 
    "DatabaseSettings",
    "SchedulerSettings",
    "LoggingSettings",
    "get_settings",
    "settings",
]

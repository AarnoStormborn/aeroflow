"""
Configuration for feature engineering service.
"""

import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

load_dotenv()


class S3Settings(BaseSettings):
    """S3 configuration for reading data."""
    
    bucket_name: str = Field(default="flights-forecasting")
    prefix: str = Field(default="raw/flights/states")
    region: str = Field(default="us-east-1")
    access_key_id: str | None = Field(default=None, validation_alias="AWS_ACCESS_KEY_ID")
    secret_access_key: str | None = Field(default=None, validation_alias="AWS_SECRET_ACCESS_KEY")
    
    model_config = SettingsConfigDict(
        env_prefix="AWS_S3_",
        populate_by_name=True,
    )


class Settings(BaseSettings):
    """Main settings."""
    
    s3: S3Settings = Field(default_factory=S3Settings)
    
    # Local cache directory for downloaded data
    cache_dir: str = Field(default="data/cache")
    
    # Slack webhook for notifications
    slack_webhook_url: str | None = Field(default=None, validation_alias="SLACK_WEBHOOK_URL")
    
    model_config = SettingsConfigDict(env_prefix="FE_", populate_by_name=True)


settings = Settings()

__all__ = ["settings", "S3Settings", "Settings"]

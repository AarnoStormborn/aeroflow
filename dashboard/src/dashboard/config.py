"""
Configuration for the dashboard service.
"""

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


class S3Settings(BaseSettings):
    """S3 configuration."""

    bucket_name: str = Field(default="flights-forecasting")
    raw_prefix: str = Field(default="raw/flights/states")
    features_prefix: str = Field(default="features/hourly")
    forecasts_prefix: str = Field(default="forecasts/hourly")
    reports_prefix: str = Field(default="reports/daily")
    region: str = Field(default="us-east-1")
    access_key_id: str | None = Field(default=None, validation_alias="AWS_ACCESS_KEY_ID")
    secret_access_key: str | None = Field(default=None, validation_alias="AWS_SECRET_ACCESS_KEY")

    model_config = SettingsConfigDict(env_prefix="AWS_S3_", populate_by_name=True)


class Settings(BaseSettings):
    """Main settings."""

    s3: S3Settings = Field(default_factory=S3Settings)

    # MLflow tracking URI (used server-side only; NEVER exposed to the page)
    mlflow_tracking_uri: str = Field(
        default="https://harshsingh90220--aeroflow-mlflow-ui.modal.run"
    )

    model_config = SettingsConfigDict(env_prefix="DASH_", populate_by_name=True)


settings = Settings()

__all__ = ["S3Settings", "Settings", "settings"]

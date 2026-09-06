"""
Configuration for the forecasting service.
"""

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


class S3Settings(BaseSettings):
    """S3 configuration."""

    bucket_name: str = Field(default="flights-forecasting")
    # Where raw flight states live (per-day parquet files)
    raw_prefix: str = Field(default="raw/flights/states")
    # Where forecasts get written
    forecast_prefix: str = Field(default="forecasts/hourly")
    region: str = Field(default="us-east-1")
    access_key_id: str | None = Field(default=None, validation_alias="AWS_ACCESS_KEY_ID")
    secret_access_key: str | None = Field(default=None, validation_alias="AWS_SECRET_ACCESS_KEY")

    model_config = SettingsConfigDict(env_prefix="AWS_S3_", populate_by_name=True)


class ForecastSettings(BaseSettings):
    """Forecasting configuration."""

    # MLflow model to load for predictions
    mlflow_tracking_uri: str = Field(
        default="https://harshsingh90220--aeroflow-mlflow-ui.modal.run"
    )
    registered_model: str = Field(default="flight-traffic-forecaster")
    model_stage: str = Field(default="Production")

    # Forecast horizons (hours)
    hourly_horizon: int = Field(default=1)   # next hour
    quarter_day_horizon: int = Field(default=6)  # next 6 hours

    # Feature columns the model expects (order matters)
    feature_columns: list[str] = Field(default=[
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "lag_1h",
        "lag_24h",
        "rolling_mean_6h",
    ])

    model_config = SettingsConfigDict(env_prefix="FORECAST_", populate_by_name=True)


class Settings(BaseSettings):
    """Main settings."""

    s3: S3Settings = Field(default_factory=S3Settings)
    forecast: ForecastSettings = Field(default_factory=ForecastSettings)


settings = Settings()

__all__ = ["ForecastSettings", "S3Settings", "Settings", "settings"]

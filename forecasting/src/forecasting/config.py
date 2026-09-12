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
    region: str = Field(default="ap-south-1")
    access_key_id: str | None = Field(default=None, validation_alias="AWS_ACCESS_KEY_ID")
    secret_access_key: str | None = Field(default=None, validation_alias="AWS_SECRET_ACCESS_KEY")

    model_config = SettingsConfigDict(env_prefix="AWS_S3_", populate_by_name=True)


class ForecastSettings(BaseSettings):
    """Forecasting configuration."""

    # MLflow model to serve. Per-model forecasts are keyed by name in the
    # output so the storage format stays open to more models later, but we
    # deliberately serve ONE model: `flight-traffic-hourly`, which retrains on
    # the current traffic regime and decisively outscored the older
    # Dec-Jan `flight-traffic-forecaster` (paired h=1 MAPE 69% vs 204%, and it
    # won 69/69 shared target-hours). Running the old model in parallel is no
    # longer useful, so the A/B setup was retired.
    mlflow_tracking_uri: str = Field(default="https://harshsingh90220--aeroflow-mlflow-ui.modal.run")
    # (name, stage) pairs — every entry is forecast and stored per run
    models: list[tuple[str, str]] = Field(
        default=[
            ("flight-traffic-hourly", "Production"),  # retrained every 3 days
        ]
    )

    # Forecast horizons (hours)
    hourly_horizon: int = Field(default=1)  # next hour
    quarter_day_horizon: int = Field(default=6)  # next 6 hours

    # Feature columns the model expects (order matters)
    feature_columns: list[str] = Field(
        default=[
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "lag_1h",
            "lag_24h",
            "rolling_mean_6h",
        ]
    )

    model_config = SettingsConfigDict(env_prefix="FORECAST_", populate_by_name=True)


class Settings(BaseSettings):
    """Main settings."""

    s3: S3Settings = Field(default_factory=S3Settings)
    forecast: ForecastSettings = Field(default_factory=ForecastSettings)
    # Shared secret for the public forecasting HTTP API. Read from the
    # AEROFLOW_API_KEY env var (Modal secret `aeroflow-api-auth`).
    api_key: str | None = Field(default=None, validation_alias="AEROFLOW_API_KEY")

    model_config = SettingsConfigDict(populate_by_name=True)


settings = Settings()

__all__ = ["ForecastSettings", "S3Settings", "Settings", "settings"]

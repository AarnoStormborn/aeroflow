"""
Configuration for model training pipeline.
"""

import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

load_dotenv()


class S3Settings(BaseSettings):
    """S3 configuration for reading feature data."""
    
    bucket_name: str = Field(default="flights-forecasting")
    region: str = Field(default="us-east-1")
    access_key_id: str | None = Field(default=None, validation_alias="AWS_ACCESS_KEY_ID")
    secret_access_key: str | None = Field(default=None, validation_alias="AWS_SECRET_ACCESS_KEY")
    
    model_config = SettingsConfigDict(
        env_prefix="AWS_S3_",
        populate_by_name=True,
    )


class MLflowSettings(BaseSettings):
    """MLflow configuration."""
    
    tracking_uri: str = Field(default="sqlite:///mlflow.db")
    artifact_root: str = Field(default="s3://flights-forecasting/mlflow")
    experiment_name: str = Field(default="flight-traffic-forecasting")
    
    model_config = SettingsConfigDict(
        env_prefix="MLFLOW_",
        populate_by_name=True,
    )


class TrainingSettings(BaseSettings):
    """Training configuration."""
    
    # Rolling window size (days)
    window_days: int = Field(default=14)
    
    # Train/test split ratio (10% test for now, increase to 20% with more data)
    test_ratio: float = Field(default=0.1)
    
    # Feature columns
    feature_columns: list[str] = Field(default=[
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "lag_1h",
        "lag_24h",
        "rolling_mean_6h",
    ])
    
    target_column: str = Field(default="flight_count")


class Settings(BaseSettings):
    """Main settings."""
    
    s3: S3Settings = Field(default_factory=S3Settings)
    mlflow: MLflowSettings = Field(default_factory=MLflowSettings)
    training: TrainingSettings = Field(default_factory=TrainingSettings)


settings = Settings()

__all__ = ["settings", "S3Settings", "MLflowSettings", "TrainingSettings", "Settings"]

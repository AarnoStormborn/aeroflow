"""
Forecast engine: computes hourly (1h) and quarter-daily (6h) forecasts
using recursive prediction with the registered production model.

Recursive approach:
- Predict next hour from actuals (lag_1h = last actual hour).
- Feed the prediction back as lag_1h for the following hour; lag_24h uses
  actuals from 24h ago (known); rolling_mean_6h mixes actuals + predictions.

Outputs are written to S3 (forecasts/hourly/...) so they can later be
compared against actuals by an evaluation job.
"""

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import mlflow
import polars as pl
from loguru import logger
from src.forecasting.config import settings
from src.forecasting.data.loader import RecentDataLoader, build_feature_vector


class ForecastEngine:
    """Recursive forecasting with the registered MLflow model."""

    def __init__(self):
        self.model = None
        self.loader = RecentDataLoader()
        self.feature_cols = settings.forecast.feature_columns
        self.bucket = settings.s3.bucket_name
        self._s3 = boto3.client(
            "s3",
            region_name=settings.s3.region,
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
        )

    def _load_model(self):
        if self.model is None:
            model_uri = (
                f"models:/{settings.forecast.registered_model}/"
                f"{settings.forecast.model_stage}"
            )
            mlflow.set_tracking_uri(settings.forecast.mlflow_tracking_uri)
            logger.info(f"Loading model {model_uri}")
            self.model = mlflow.sklearn.load_model(model_uri)
        return self.model

    def _next_hour(self, target_hour: datetime) -> float:
        """Predict flight count for a single target hour (recursive-aware)."""
        # Build features from actuals + accumulated predictions in self._predicted
        features = build_feature_vector(
            self._hourly,
            target_hour,
            predicted_counts=self._predicted,
        )
        X = pl.DataFrame([dict(zip(self.feature_cols, features, strict=True))]).to_numpy()
        pred = float(self.model.predict(X)[0])
        return pred

    def forecast(self, now: datetime | None = None) -> dict[str, Any]:
        """
        Run a forecast: next-hour + next-6-hours, recursively.

        Returns a dict with forecast metadata + per-hour predictions.
        """
        now = now or datetime.now(timezone.utc)
        self._load_model()  # ensure model is loaded

        # Current truncated hour; we predict from the NEXT hour onward.
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        horizon = settings.forecast.quarter_day_horizon  # 6h

        # Load actuals covering enough history (lag_24h + rolling 6h + buffer)
        self._hourly = self.loader.load_recent_hourly(days=4)
        self._predicted: dict[datetime, float] = {}

        predictions: list[dict] = []
        target = current_hour + timedelta(hours=1)  # first future hour
        for step in range(1, horizon + 1):
            pred_val = self._next_hour(target)
            predictions.append({
                "hour_start": target.isoformat(),
                "horizon_hours": step,
                "predicted_flight_count": round(pred_val, 2),
            })
            # Feed prediction back for the recursion
            self._predicted[target] = pred_val
            target += timedelta(hours=1)

        # Snapshot the latest actual hour used (for eval alignment)
        last_actual = (
            self._hourly.select(pl.col("hour_start").max()).item()
            if not self._hourly.is_empty() else None
        )

        result = {
            "generated_at": now.isoformat(),
            "model": settings.forecast.registered_model,
            "model_stage": settings.forecast.model_stage,
            "last_actual_hour": last_actual.isoformat() if last_actual else None,
            "horizons": {
                "hourly": predictions[0],
                "quarter_daily": predictions,
            },
        }
        return result

    def save_forecast(self, result: dict) -> str:
        """Write forecast to S3 under forecasts/hourly/..."""
        generated = datetime.fromisoformat(result["generated_at"])
        key = (
            f"{settings.s3.forecast_prefix}/year={generated.year}/"
            f"month={generated.month:02d}/forecast_{generated.strftime('%Y%m%d_%H%M%S')}.json"
        )
        body = json.dumps(result, indent=2).encode()
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=body)
        s3_url = f"s3://{self.bucket}/{key}"
        logger.info(f"Forecast saved: {s3_url}")
        return s3_url


def run_forecast() -> dict:
    """Run a forecast and persist it. Returns the result dict."""
    engine = ForecastEngine()
    result = engine.forecast()
    engine.save_forecast(result)
    return result


def main():
    """CLI entrypoint."""
    result = run_forecast()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

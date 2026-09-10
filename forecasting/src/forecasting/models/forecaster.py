"""
Forecast engine: runs MULTIPLE registered models in parallel.

Each model (e.g. the Dec-Jan forecaster and the Sep-current hourly model)
produces its own hourly (1h) + quarter-daily (6h) recursive forecast for
the SAME target hours, so predictions can be compared against actuals to
see which model performs better.

Recursive approach (per model):
- Predict next hour from actuals (lag_1h = last actual hour).
- Feed the prediction back as lag_1h for the following hour; lag_24h uses
  actuals from 24h ago (known); rolling_mean_6h mixes actuals + predictions.

Outputs are written to S3 (forecasts/hourly/...) tagged by model name so
the evaluation job can score each model separately.
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
    """Run recursive forecasting across several registered models."""

    def __init__(self):
        self.models: dict[str, Any] = {}  # model name -> loaded model
        self.loader = RecentDataLoader()
        self.feature_cols = settings.forecast.feature_columns
        self.bucket = settings.s3.bucket_name
        self._s3 = boto3.client(
            "s3",
            region_name=settings.s3.region,
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
        )
        self.horizon = settings.forecast.quarter_day_horizon  # 6h

    def _load_model(self, name: str, stage: str):
        """Load a single model (cached)."""
        if name not in self.models:
            model_uri = f"models:/{name}/{stage}"
            mlflow.set_tracking_uri(settings.forecast.mlflow_tracking_uri)
            logger.info(f"Loading model {model_uri}")
            self.models[name] = mlflow.sklearn.load_model(model_uri)
        return self.models[name]

    def _resolve_version(self, name: str, stage: str) -> str | None:
        """Resolve the MLflow version that `models:/{name}/{stage}` points at.

        Recorded in the forecast output so a stored prediction can be traced
        back to the exact registered version that produced it. Returns None if
        the lookup fails -- provenance is desirable but must never block a
        forecast from being produced.
        """
        try:
            client = mlflow.tracking.MlflowClient()
            versions = [v for v in client.search_model_versions(f"name='{name}'") if v.current_stage == stage]
            if not versions:
                logger.warning(f"{name}: no version in stage {stage!r} to attribute")
                return None
            return max(versions, key=lambda v: int(v.version)).version
        except Exception as e:
            logger.warning(f"Could not resolve version for {name}/{stage}: {e}")
            return None

    def _predict_one_model(self, model, hourly, current_hour: datetime) -> list[dict]:
        """Recursive forecast for ONE model: h=1..6. Returns predictions."""
        # Map of future-hour predictions built up during recursion (per model,
        # so each model's recursion uses ITS OWN predictions as lag inputs).
        predicted: dict[datetime, float] = {}
        predictions: list[dict] = []

        target = current_hour + timedelta(hours=1)
        for step in range(1, self.horizon + 1):
            features = build_feature_vector(hourly, target, predicted_counts=predicted)
            X = pl.DataFrame([dict(zip(self.feature_cols, features, strict=True))]).to_numpy()
            pred_val = float(model.predict(X)[0])
            predictions.append(
                {
                    "hour_start": target.isoformat(),
                    "horizon_hours": step,
                    "predicted_flight_count": round(pred_val, 2),
                }
            )
            predicted[target] = pred_val  # feed back for recursion
            target += timedelta(hours=1)
        return predictions

    def forecast(self, now: datetime | None = None) -> dict[str, Any]:
        """Run forecasts for every configured model in parallel."""
        now = now or datetime.now(timezone.utc)

        # Load actuals once (shared across models)
        hourly = self.loader.load_recent_hourly(days=4)

        # The last COMPLETE hour: the current hour may be partial (ingestion
        # runs mid-hour), so only use hours strictly before now's hour.
        now_hour = now.replace(minute=0, second=0, microsecond=0)
        complete = hourly.filter(pl.col("hour_start") < now_hour)
        last_actual = complete.select(pl.col("hour_start").max()).item() if not complete.is_empty() else None
        if last_actual is None:
            raise ValueError("No complete actual hour available for forecasting")

        # Forecast from the hour AFTER the last complete actual hour, so all
        # lag/rolling inputs (lag_1h, lag_24h, rolling 6h) are actuals or
        # prior recursion outputs — never an incomplete current hour.
        current_hour = last_actual
        last_actual_str = last_actual.isoformat()

        # Run each configured model
        models_out = {}
        for name, stage in settings.forecast.models:
            model = self._load_model(name, stage)
            preds = self._predict_one_model(model, hourly, current_hour)
            models_out[name] = {
                "model_stage": stage,
                # Provenance: which registered version produced this forecast
                "model_version": self._resolve_version(name, stage),
                "hourly": preds[0],  # h=1
                "quarter_daily": preds,  # h=1..6
            }

        result = {
            # Schema version: bumped when the forecast document shape changes.
            # v2 adds per-model provenance (model_version / model_stage).
            "schema_version": 2,
            "generated_at": now.isoformat(),
            "last_actual_hour": last_actual_str,
            "models": models_out,  # keyed by model name
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
    """Run a multi-model forecast and persist it. Returns the result dict."""
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

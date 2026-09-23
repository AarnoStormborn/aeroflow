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


def forecast_signature(result: dict[str, Any]) -> tuple[Any, ...]:
    """
    Identity of a forecast: what it anchors on, plus which models produced it.

    Two runs sharing a signature generate byte-identical predictions, so
    re-writing them just churns the archive with copies. Model identity is part
    of the key because a promotion changes the predictions even when the anchor
    has not moved.

    Args:
        result: Forecast document produced by ForecastEngine.forecast()

    Returns:
        Hashable signature, comparable across runs
    """
    models = result.get("models") or {}
    return (
        result.get("schema_version"),
        result.get("last_actual_hour"),
        # Coerced to strings so entries stay comparable when a version is unset
        *sorted(f"{name}:{m.get('model_version')}:{m.get('model_stage')}" for name, m in models.items()),
    )


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

    def _latest_stored_signature(self) -> tuple[Any, ...] | None:
        """
        Signature of the most recently stored forecast, or None if there is none.

        Reads only the newest object: keys embed a zero-padded timestamp, so the
        lexicographically greatest key is the most recent run.
        """
        keys: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f"{settings.s3.forecast_prefix}/"):
            keys.extend(o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json"))
        if not keys:
            return None

        latest = max(keys)
        body = self._s3.get_object(Bucket=self.bucket, Key=latest)["Body"].read()
        return forecast_signature(json.loads(body))

    def save_forecast(self, result: dict, *, dedupe: bool = True) -> str | None:
        """
        Write forecast to S3 under forecasts/hourly/...

        Skips the write when it would duplicate the newest stored forecast - the
        anchor has not advanced and the served models are unchanged, which is what
        happens every hour while ingestion is stalled. During the Sep 20 outage
        this wrote ~28 hours of identical files.

        Returns:
            The S3 URL, or None if the write was skipped as a duplicate.
        """
        if dedupe:
            try:
                previous = self._latest_stored_signature()
            except Exception as e:
                # The guard is an optimisation: a duplicate costs storage, not
                # correctness, so never let a failed check block the forecast.
                logger.warning(f"Duplicate-forecast check failed, writing anyway: {e}")
                previous = None
            if previous is not None and previous == forecast_signature(result):
                logger.info(
                    "Skipping duplicate forecast: anchor still "
                    f"{result.get('last_actual_hour')} and served models unchanged"
                )
                return None

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
    """
    Run a multi-model forecast and persist it.

    Returns:
        The forecast document plus a `saved` flag saying whether a new file was
        written (False when the duplicate guard skipped it). The flag is added
        after persistence, so it never appears in the stored document.
    """
    engine = ForecastEngine()
    result = engine.forecast()
    url = engine.save_forecast(result)
    return {**result, "saved": url is not None}


__all__ = [
    "ForecastEngine",
    "forecast_signature",
    "run_forecast",
]


def main():
    """CLI entrypoint."""
    result = run_forecast()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

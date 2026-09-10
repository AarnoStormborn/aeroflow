"""
Evaluation: compare stored forecasts against actual flight counts, per model.

For each forecast JSON in S3, once the actual data for its forecast
horizon has elapsed, compute per-horizon error (h=1..6) for EACH model in
the forecast document. The document is keyed by model name, so this stays
correct for the single served model and would also cover multiple models.

Backward-compatible: also handles the older single-model format
(horizons.quarter_daily).

Run periodically (e.g. daily).
"""

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from loguru import logger
from src.forecasting.config import settings
from src.forecasting.data.loader import RecentDataLoader


class ForecastEvaluator:
    """Compare stored forecasts to actual hourly counts, per model."""

    def __init__(self):
        self.bucket = settings.s3.bucket_name
        self.prefix = settings.s3.forecast_prefix
        self.loader = RecentDataLoader()
        self._s3 = boto3.client(
            "s3",
            region_name=settings.s3.region,
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
        )
        self.quarter_horizon = settings.forecast.quarter_day_horizon  # 6

    def _list_forecasts(self) -> list[str]:
        keys = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json"):
                    keys.append(obj["Key"])
        return keys

    def _model_predictions(self, fc: dict) -> dict[str, list[dict]]:
        """Extract {model_name: [per-hour predictions]} from a forecast file,
        handling both new multi-model and old single-model formats."""
        if "models" in fc:
            # New format: {"models": {name: {"hourly":..., "quarter_daily":[...]}}}
            return {name: m["quarter_daily"] for name, m in fc["models"].items()}
        # Old format: single model with horizons.quarter_daily
        return {
            fc.get("model", "unknown"): fc["horizons"]["quarter_daily"],
        }

    def _load_actuals(self, min_hour: datetime, max_hour: datetime) -> dict[datetime, float]:
        """Load actual hourly counts covering [min_hour, max_hour] with buffer."""
        # Load from min_hour - 2 days (for rolling) through max_hour + 1 day
        start_day = (min_hour - timedelta(days=2)).date()
        end_day = (max_hour + timedelta(days=1)).date()
        days = (end_day - start_day).days + 1
        hourly = self.loader.load_recent_hourly(days=max(days, 3))
        actuals: dict[datetime, float] = {}
        for row in hourly.iter_rows(named=True):
            actuals[row["hour_start"]] = float(row["flight_count"])
        return actuals

    def evaluate_forecast(self, key: str, actuals: dict[datetime, float], now: datetime | None = None) -> dict | None:
        """Evaluate one forecast file per model. Returns None if actuals
        haven't elapsed yet.

        Args:
            key: S3 key of the forecast
            actuals: preloaded {hour_start: flight_count} lookup
            now: current time (defaults to now)
        """
        now = now or datetime.now(timezone.utc)
        resp = self._s3.get_object(Bucket=self.bucket, Key=key)
        fc = json.loads(resp["Body"].read())

        generated = datetime.fromisoformat(fc["generated_at"])
        needed = generated + timedelta(hours=self.quarter_horizon + 1)
        if now < needed:
            return None  # not enough actuals yet

        models_preds = self._model_predictions(fc)
        per_model: dict[str, dict[str, Any]] = {}

        for model_name, steps in models_preds.items():
            rows = []
            for step in steps:
                target = datetime.fromisoformat(step["hour_start"])
                actual = actuals.get(target)
                if actual is None:
                    continue
                pred = step["predicted_flight_count"]
                error = pred - actual
                mape = abs(error) / actual * 100 if actual else None
                rows.append(
                    {
                        "hour_start": target.isoformat(),
                        "horizon_hours": step["horizon_hours"],
                        "actual": round(actual, 2),
                        "predicted": pred,
                        "error": round(error, 2),
                        "mape_pct": round(mape, 2) if mape is not None else None,
                    }
                )

            if not rows:
                continue

            # Aggregate by horizon
            agg: dict[int, list[float]] = {}
            for r in rows:
                if r["mape_pct"] is not None:
                    agg.setdefault(r["horizon_hours"], []).append(r["mape_pct"])
            per_horizon = {str(h): round(sum(v) / len(v), 2) for h, v in sorted(agg.items())}
            all_mape = [r["mape_pct"] for r in rows if r["mape_pct"] is not None]

            per_model[model_name] = {
                "samples": len(rows),
                "per_horizon_mean_mape": per_horizon,
                "overall_mean_mape": (round(sum(all_mape) / len(all_mape), 2) if all_mape else None),
            }

        if not per_model:
            return None

        return {
            "forecast_key": key,
            "generated_at": fc["generated_at"],
            "models": per_model,
        }

    def evaluate_all(self) -> list[dict]:
        """Evaluate all forecasts old enough to have actuals.

        Loads actuals ONCE (not per forecast) for efficiency.
        """
        now = datetime.now(timezone.utc)
        keys = self._list_forecasts()
        evals = []

        # Find the oldest forecast to size the actuals window
        min_generated = now
        parseable = []
        for key in keys:
            try:
                fc = json.loads(self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())
                gen = datetime.fromisoformat(fc["generated_at"])
                parseable.append((key, gen))
                if gen < min_generated:
                    min_generated = gen
            except Exception as e:
                logger.warning(f"Skipping unreadable forecast {key}: {e}")

        if not parseable:
            return []

        # Only forecasts old enough matter; compute actuals window once
        eligible = [
            (key, gen) for key, gen in parseable if (now - gen).total_seconds() >= (self.quarter_horizon + 1) * 3600
        ]
        if not eligible:
            logger.info("No forecasts old enough to evaluate yet")
            return []

        # Latest target hour among eligible forecasts determines how much
        # actual history we need (from oldest eligible's first prediction)
        oldest = min(gen for _, gen in eligible)
        min_hour = oldest
        max_hour = now
        actuals = self._load_actuals(min_hour, max_hour)
        logger.info(f"Loaded actuals once: {len(actuals)} hours for {len(eligible)} forecasts")

        for key, _ in sorted(eligible, key=lambda x: x[1]):
            try:
                ev = self.evaluate_forecast(key, actuals, now)
                if ev:
                    evals.append(ev)
            except Exception as e:
                logger.warning(f"Eval failed for {key}: {e}")
        return evals


def run_eval() -> list[dict]:
    ev = ForecastEvaluator()
    results = ev.evaluate_all()
    if results:
        print(f"\n=== EVAL SUMMARY ({len(results)} forecasts) ===")
        # Aggregate per model across forecasts (per-horizon h=1 and overall)
        model_h1: dict[str, list[float]] = {}
        model_overall: dict[str, list[float]] = {}
        for r in results:
            for name, m in r["models"].items():
                if "1" in m["per_horizon_mean_mape"]:
                    model_h1.setdefault(name, []).append(m["per_horizon_mean_mape"]["1"])
                if m["overall_mean_mape"] is not None:
                    model_overall.setdefault(name, []).append(m["overall_mean_mape"])
        for name in sorted(model_h1):
            h1 = model_h1[name]
            ov = model_overall.get(name, [])
            print(f"  {name}:")
            print(
                f"    h=1 MAPE: {sum(h1) / len(h1):.2f}% over {len(h1)} forecasts"
                + (f" | overall: {sum(ov) / len(ov):.2f}%" if ov else "")
            )
    else:
        print("No forecasts old enough to evaluate yet.")
    return results


if __name__ == "__main__":
    run_eval()

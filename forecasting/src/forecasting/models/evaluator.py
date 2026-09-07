"""
Evaluation: compare stored forecasts against actual flight counts, per model.

For each forecast JSON in S3, once the actual data for its forecast
horizon has elapsed, compute per-horizon error (h=1..6) for EACH model in
the forecast (the multi-model format stores predictions from every model).
This lets us compare which model performs better.

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
            return {
                name: m["quarter_daily"]
                for name, m in fc["models"].items()
            }
        # Old format: single model with horizons.quarter_daily
        return {
            fc.get("model", "unknown"): fc["horizons"]["quarter_daily"],
        }

    def evaluate_forecast(self, key: str) -> dict | None:
        """Evaluate one forecast file per model. Returns None if actuals
        haven't elapsed yet."""
        resp = self._s3.get_object(Bucket=self.bucket, Key=key)
        fc = json.loads(resp["Body"].read())

        generated = datetime.fromisoformat(fc["generated_at"])
        needed = generated + timedelta(hours=self.quarter_horizon + 1)
        if datetime.now(timezone.utc) < needed:
            return None  # not enough actuals yet

        hourly = self.loader.load_recent_hourly(days=8)
        actuals: dict[datetime, float] = {}
        for row in hourly.iter_rows(named=True):
            actuals[row["hour_start"]] = float(row["flight_count"])

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
                rows.append({
                    "hour_start": target.isoformat(),
                    "horizon_hours": step["horizon_hours"],
                    "actual": round(actual, 2),
                    "predicted": pred,
                    "error": round(error, 2),
                    "mape_pct": round(mape, 2) if mape is not None else None,
                })

            if not rows:
                continue

            # Aggregate by horizon
            agg: dict[int, list[float]] = {}
            for r in rows:
                if r["mape_pct"] is not None:
                    agg.setdefault(r["horizon_hours"], []).append(r["mape_pct"])
            per_horizon = {
                str(h): round(sum(v) / len(v), 2) for h, v in sorted(agg.items())
            }
            all_mape = [r["mape_pct"] for r in rows if r["mape_pct"] is not None]

            per_model[model_name] = {
                "samples": len(rows),
                "per_horizon_mean_mape": per_horizon,
                "overall_mean_mape": (
                    round(sum(all_mape) / len(all_mape), 2) if all_mape else None
                ),
            }

        if not per_model:
            return None

        return {
            "forecast_key": key,
            "generated_at": fc["generated_at"],
            "models": per_model,
        }

    def evaluate_all(self) -> list[dict]:
        """Evaluate all forecasts old enough to have actuals."""
        evals = []
        for key in self._list_forecasts():
            try:
                ev = self.evaluate_forecast(key)
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
                    model_h1.setdefault(name, []).append(
                        m["per_horizon_mean_mape"]["1"])
                if m["overall_mean_mape"] is not None:
                    model_overall.setdefault(name, []).append(m["overall_mean_mape"])
        for name in sorted(model_h1):
            h1 = model_h1[name]
            ov = model_overall.get(name, [])
            print(f"  {name}:")
            print(f"    h=1 MAPE: {sum(h1)/len(h1):.2f}% over {len(h1)} forecasts"
                  + (f" | overall: {sum(ov)/len(ov):.2f}%" if ov else ""))
    else:
        print("No forecasts old enough to evaluate yet.")
    return results


if __name__ == "__main__":
    run_eval()

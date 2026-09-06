"""
Evaluation: compare stored forecasts against actual flight counts.

For each forecast JSON in S3, once the actual data for its forecast
horizon has elapsed, compute per-horizon error (h=1..6) and aggregate
summary stats. This tells us whether recursive prediction degrades with
horizon.

Run periodically (e.g. daily) — it picks up forecasts old enough to have
actuals.
"""

import json
from datetime import datetime, timedelta, timezone

import boto3
from loguru import logger
from src.forecasting.config import settings
from src.forecasting.data.loader import RecentDataLoader


class ForecastEvaluator:
    """Compare stored forecasts to actual hourly counts."""

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

    def evaluate_forecast(self, key: str) -> dict | None:
        """Evaluate one forecast file. Returns per-horizon errors, or None if
        actuals haven't elapsed yet."""
        resp = self._s3.get_object(Bucket=self.bucket, Key=key)
        fc = json.loads(resp["Body"].read())

        generated = datetime.fromisoformat(fc["generated_at"])
        # Only evaluate once the full 6h horizon has elapsed (+1h buffer)
        needed = generated + timedelta(hours=self.quarter_horizon + 1)
        if datetime.now(timezone.utc) < needed:
            return None  # not enough actuals yet

        hourly = self.loader.load_recent_hourly(days=8)
        actuals: dict[datetime, float] = {}
        for row in hourly.iter_rows(named=True):
            actuals[row["hour_start"]] = float(row["flight_count"])

        results = []
        for step in fc["horizons"]["quarter_daily"]:
            target = datetime.fromisoformat(step["hour_start"])
            actual = actuals.get(target)
            if actual is None:
                continue
            pred = step["predicted_flight_count"]
            error = pred - actual
            mape = abs(error) / actual * 100 if actual else None
            results.append({
                "hour_start": target.isoformat(),
                "horizon_hours": step["horizon_hours"],
                "actual": round(actual, 2),
                "predicted": pred,
                "error": round(error, 2),
                "mape_pct": round(mape, 2) if mape is not None else None,
            })

        if not results:
            return None

        # Aggregate by horizon
        agg: dict[int, list[float]] = {}
        for r in results:
            agg.setdefault(r["horizon_hours"], []).append(r["mape_pct"])
        per_horizon = {
            str(h): round(sum(v) / len(v), 2) for h, v in sorted(agg.items())
        }

        return {
            "forecast_key": key,
            "generated_at": fc["generated_at"],
            "samples": len(results),
            "per_horizon_mean_mape": per_horizon,
            "overall_mean_mape": round(
                sum(r["mape_pct"] for r in results if r["mape_pct"] is not None)
                / sum(1 for r in results if r["mape_pct"] is not None), 2
            ) if results else None,
            "details": results,
        }

    def evaluate_all(self, min_age_hours: int = 7) -> list[dict]:
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
        # Aggregate per-horizon across forecasts
        horizon_sums: dict[str, list[float]] = {}
        for r in results:
            for h, m in r["per_horizon_mean_mape"].items():
                horizon_sums.setdefault(h, []).append(m)
        for h in sorted(horizon_sums, key=int):
            vals = horizon_sums[h]
            print(f"  h={h}: mean MAPE {sum(vals)/len(vals):.2f}% "
                  f"over {len(vals)} forecasts")
    else:
        print("No forecasts old enough to evaluate yet.")
    return results


if __name__ == "__main__":
    run_eval()

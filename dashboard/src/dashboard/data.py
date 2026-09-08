"""
S3 data access + aggregation for the dashboard.

Reads raw flight-state parquet, hourly features, and forecast JSONs from
S3, aggregates them into the shapes the dashboard API needs, and caches
results in-memory with a TTL so we don't hammer S3 on every page refresh.
"""

import io
import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
import polars as pl
from loguru import logger
from src.dashboard.config import settings

# In-memory cache: key -> (expires_at, value)
_CACHE: dict[str, tuple[float, Any]] = {}
_CACHE_TTL = 45.0  # seconds


def _cached(key: str, loader, ttl: float = _CACHE_TTL):
    """Return cached value or compute via loader (thread-safe enough for uvicorn)."""
    now = __import__("time").time()
    hit = _CACHE.get(key)
    if hit and hit[0] > now:
        return hit[1]
    value = loader()
    _CACHE[key] = (now + ttl, value)
    return value


class S3Store:
    def __init__(self):
        self.bucket = settings.s3.bucket_name
        self._client = boto3.client(
            "s3",
            region_name=settings.s3.region,
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
        )

    # ---------- raw parquet ----------

    def list_raw_files(self, dt: date) -> list[str]:
        prefix = (f"{settings.s3.raw_prefix}/year={dt.year}/month={dt.month:02d}/"
                  f"day={dt.day:02d}/")
        keys = []
        for page in self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        ):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])
        return keys

    def read_parquet(self, key: str) -> pl.DataFrame | None:
        try:
            resp = self._client.get_object(Bucket=self.bucket, Key=key)
            return pl.read_parquet(io.BytesIO(resp["Body"].read()))
        except Exception as e:
            logger.warning(f"read failed {key}: {e}")
            return None

    def _day_hourly(self, dt: date) -> pl.DataFrame:
        """Aggregate one raw day into hourly unique-aircraft counts.

        Past days are immutable -> cache longer (15 min). Today changes ->
        shorter TTL (60 s).
        """
        ttl = 60.0 if dt == datetime.now(timezone.utc).date() else 900.0

        def load():
            dfs = []
            for key in self.list_raw_files(dt):
                df = self.read_parquet(key)
                if df is not None and not df.is_empty():
                    dfs.append(df)
            if not dfs:
                return pl.DataFrame(schema={
                    "hour_start": pl.Datetime("us", "UTC"),
                    "flight_count": pl.Float64,
                    "active_now": pl.Float64,
                })
            raw = pl.concat(dfs, how="diagonal_relaxed")
            hourly = (
                raw.with_columns(
                    pl.from_epoch(pl.col("capture_time"), time_unit="s")
                    .dt.truncate("1h")
                    .alias("hour_start")
                )
                .group_by("hour_start")
                .agg(pl.col("icao24").n_unique().alias("flight_count"))
                .sort("hour_start")
            )
            # Latest aircraft set (most recent file) for 'active now'
            latest = dfs[-1]
            active = (
                latest.select(pl.col("icao24").n_unique()).item()
                if not latest.is_empty() else 0
            )
            return hourly.with_columns(pl.lit(float(active)).alias("active_now"))

        return _cached(f"day_hourly:{dt.isoformat()}", load, ttl=ttl)

    # ---------- hourly features (canonical daily curve) ----------

    def feature_keys(self) -> list[str]:
        keys = []
        for page in self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=settings.s3.features_prefix
        ):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])
        return keys

    def read_feature_day(self, key: str) -> pl.DataFrame | None:
        # Avoid noisy NoSuchKey errors when probing days without features
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
        except Exception:
            return None
        return self.read_parquet(key)

    def feature_days(self) -> set[date]:
        days = set()
        for key in self.feature_keys():
            m = re.search(r"features_(\d{4})-(\d{2})-(\d{2})", key)
            if m:
                days.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
        return days

    # ---------- forecasts ----------

    def forecast_files(self) -> list[str]:
        keys = []
        for page in self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=settings.s3.forecasts_prefix
        ):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json"):
                    keys.append(obj["Key"])
        return sorted(keys)

    def read_forecast(self, key: str) -> dict | None:
        try:
            resp = self._client.get_object(Bucket=self.bucket, Key=key)
            return json.loads(resp["Body"].read())
        except Exception as e:
            logger.warning(f"forecast read failed {key}: {e}")
            return None

    # ---------- reports ----------

    def report_files(self) -> list[dict]:
        out = []
        for page in self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=settings.s3.reports_prefix
        ):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".pdf"):
                    out.append({
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                    })
        return sorted(out, key=lambda r: r["key"], reverse=True)


def recent_raw_days(days: int = 10) -> list[date]:
    """Last N days as date objects (UTC), oldest first."""
    today = datetime.now(timezone.utc).date()
    return [today - timedelta(days=i) for i in range(days - 1, -1, -1)]


def hourly_by_day(store: S3Store, days: int = 7) -> dict[str, pl.DataFrame]:
    """Hourly curves for the last N days (feature files preferred, raw fallback)."""
    out: dict[str, pl.DataFrame] = {}
    for dt in recent_raw_days(days):
        dkey = dt.isoformat()
        feat = store.read_feature_day(
            f"{settings.s3.features_prefix}/year={dt.year}/month={dt.month:02d}/"
            f"features_{dkey}.parquet"
        )
        if feat is not None and not feat.is_empty():
            out[dkey] = feat.select(["hour_start", "flight_count"])
        else:
            raw = store._day_hourly(dt)
            if not raw.is_empty():
                out[dkey] = raw.select(["hour_start", "flight_count"])
    return out


# ---- dashboard aggregations (each returns plain JSON-safe dicts) ----


def live_snapshot() -> dict:
    store = S3Store()

    def load():
        today = datetime.now(timezone.utc).date()
        today_df = store._day_hourly(today)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        yest_df = store._day_hourly(yesterday)

        def series(df):
            return [
                {"hour": int(r["hour_start"].hour), "count": round(float(r["flight_count"]), 1)}
                for r in (df.iter_rows(named=True) if df is not None and not df.is_empty() else [])
            ]

        active = (
            int(today_df["active_now"].max())
            if today_df is not None and not today_df.is_empty() else 0
        )
        return {
            "now_utc": datetime.now(timezone.utc).isoformat(),
            "active_aircraft_now": active,
            "today": series(today_df),
            "yesterday": series(yest_df),
        }

    return _cached("live", load)


def patterns_snapshot() -> dict:
    store = S3Store()

    def load():
        by_day = hourly_by_day(store, days=14)

        # hour-of-day profile: avg count per hour across weekdays
        hod = defaultdict(list)
        for df in by_day.values():
            for r in df.iter_rows(named=True):
                hod[int(r["hour_start"].hour)].append(float(r["flight_count"]))
        hour_profile = [
            {"hour": h, "mean": round(sum(v) / len(v), 1), "n": len(v)}
            for h, v in sorted(hod.items())
        ]

        # weekday pattern: mean daily total per weekday
        wd_total = defaultdict(list)
        for dstr, df in by_day.items():
            total = df.select(pl.col("flight_count").sum()).item()
            wd = datetime.fromisoformat(dstr).isoweekday()
            wd_total[wd].append(float(total))
        weekday_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        weekday_profile = [
            {"weekday": weekday_names[w - 1], "mean_total": round(sum(v) / len(v))}
            for w, v in sorted(wd_total.items())
        ]

        # anomaly detection: last 3 days mean vs trailing-7 mean
        days_sorted = sorted(by_day.keys())
        anomalies = []
        if len(days_sorted) >= 8:
            trail = days_sorted[-8:-3]
            trail_mean = sum(
                by_day[d].select(pl.col("flight_count").mean()).item() for d in trail
            ) / len(trail)
            for d in days_sorted[-3:]:
                day_mean = by_day[d].select(pl.col("flight_count").mean()).item()
                if trail_mean > 0:
                    dev = (day_mean - trail_mean) / trail_mean * 100
                    if abs(dev) > 25:
                        anomalies.append({
                            "date": d,
                            "mean": round(day_mean, 1),
                            "trail_mean": round(trail_mean, 1),
                            "deviation_pct": round(dev, 1),
                        })
        return {
            "hour_profile": hour_profile,
            "weekday_profile": weekday_profile,
            "anomalies": anomalies,
            "days": days_sorted[-7:],
        }

    return _cached("patterns", load)


def forecasts_snapshot() -> dict:
    store = S3Store()

    def load():
        files = store.forecast_files()
        latest = files[-1] if files else None
        latest_fc = store.read_forecast(latest) if latest else None

        # Actual vs predicted history: iterate forecasts that have elapsed,
        # align to actuals via raw hourly counts.
        # (Fetch last 2 days raw once for actuals)
        actuals: dict[str, float] = {}
        for dt in recent_raw_days(2):
            df = store._day_hourly(dt)
            if df is not None and not df.is_empty():
                for r in df.iter_rows(named=True):
                    actuals[r["hour_start"].isoformat()] = float(r["flight_count"])

        # per-model h1 time series
        model_h1: dict[str, list] = defaultdict(list)
        for fkey in files:
            fc = store.read_forecast(fkey)
            if not fc or "models" not in fc:
                continue
            datetime.fromisoformat(fc["generated_at"])
            for name, m in fc["models"].items():
                h1 = m.get("hourly")
                if not h1:
                    continue
                target = datetime.fromisoformat(h1["hour_start"])
                actual = actuals.get(h1["hour_start"])
                model_h1[name].append({
                    "target": target.strftime("%m-%d %H:%M"),
                    "pred": round(float(h1["predicted_flight_count"]), 1),
                    "actual": actual,
                })

        # recent eval (best-effort aggregate of MAPE per model per horizon)
        return {
            "latest_generated": latest_fc["generated_at"] if latest_fc else None,
            "latest_models": (
                {n: m["hourly"]["predicted_flight_count"]
                 for n, m in latest_fc["models"].items()}
                if latest_fc and "models" in latest_fc else {}
            ),
            "h1_history": {n: v for n, v in model_h1.items()},
            "num_forecasts": len(files),
        }

    return _cached("forecasts", load)


def health_snapshot() -> dict:
    store = S3Store()

    def load():
        now = datetime.now(timezone.utc)
        today = now.date()
        files_today = store.list_raw_files(today)
        # last raw file time: use S3 listing of today's prefix sorted by key
        files_today[-1] if files_today else None
        # freshness: minutes since a raw file was written (approx from filename ts)
        freshness_min = None
        if files_today:
            m = re.search(r"(\d{8})_(\d{6})", files_today[-1])
            if m:
                fname_ts = datetime.strptime(m.group(0), "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
                freshness_min = round((now - fname_ts).total_seconds() / 60, 1)

        feat_days = store.feature_days()
        # coverage gaps in last 10 days
        coverage = []
        for dt in recent_raw_days(10):
            has_raw = bool(store.list_raw_files(dt))
            has_feat = dt in feat_days
            coverage.append({"date": dt.isoformat(), "raw": has_raw, "features": has_feat})

        fc_count = len(store.forecast_files())
        return {
            "now_utc": now.isoformat(),
            "raw_files_today": len(files_today),
            "data_freshness_min": freshness_min,
            "feature_days": len(feat_days),
            "forecast_count": fc_count,
            "coverage": coverage,
        }

    return _cached("health", load)

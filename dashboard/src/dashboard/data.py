"""
S3 data access + aggregation for the dashboard.

Reads raw flight-state parquet, hourly features, and forecast JSONs from
S3, aggregates into the shapes the dashboard API needs, and caches
results in-memory with a TTL so we don't hammer S3 on every page refresh.

Breakdown views (country, airline, altitude) come from the most recent
raw snapshots so the dashboard feels "live".
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
# How many most-recent raw files to combine for 'live breakdown' views
LIVE_TICKS = 6


def _cached(key: str, loader, ttl: float = _CACHE_TTL):
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
            latest = dfs[-1]
            active = (
                latest.select(pl.col("icao24").n_unique()).item()
                if not latest.is_empty() else 0
            )
            return hourly.with_columns(pl.lit(float(active)).alias("active_now"))

        return _cached(f"day_hourly:{dt.isoformat()}", load, ttl=ttl)

    def latest_ticks(self, n: int = LIVE_TICKS) -> pl.DataFrame:
        """Combine the most recent raw snapshots (across today, falling back
        to yesterday if today has none yet)."""
        today = datetime.now(timezone.utc).date()

        def load():
            files = self.list_raw_files(today)
            if not files:
                files = self.list_raw_files(today - timedelta(days=1))
            dfs = []
            for f in files[-n:]:
                df = self.read_parquet(f)
                if df is not None and not df.is_empty():
                    dfs.append(df)
            if not dfs:
                return pl.DataFrame()
            # Deduplicate aircraft across ticks (keep latest sighting)
            all_df = pl.concat(dfs, how="diagonal_relaxed")
            return (all_df
                    .sort("capture_time")
                    .unique(subset=["icao24"], keep="last"))

        return _cached("latest_ticks", load, ttl=30.0)

    # ---------- hourly features ----------

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


def recent_raw_days(days: int = 10) -> list[date]:
    today = datetime.now(timezone.utc).date()
    return [today - timedelta(days=i) for i in range(days - 1, -1, -1)]


def hourly_by_day(store: S3Store, days: int = 7) -> dict[str, pl.DataFrame]:
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


# ---- JSON-safe aggregations ----


def live_snapshot() -> dict:
    store = S3Store()

    def load():
        today = datetime.now(timezone.utc).date()
        today_df = store._day_hourly(today)
        yesterday = today - timedelta(days=1)
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

        # Live breakdown from latest ticks
        ticks = store.latest_ticks()
        breakdown = _breakdown(ticks)

        return {
            "now_utc": datetime.now(timezone.utc).isoformat(),
            "active_aircraft_now": active,
            "today": series(today_df),
            "yesterday": series(yest_df),
            "breakdown": breakdown,
        }

    return _cached("live", load, ttl=30.0)


def _breakdown(df: pl.DataFrame) -> dict:
    """Country / airline / altitude breakdown of current aircraft."""
    if df.is_empty():
        return {"countries": [], "airlines": [], "altitudes": [], "total": 0}

    # unique aircraft already ensured by latest_ticks
    def top_counts(col_expr, n=8):
        try:
            vc = (df.group_by(col_expr)
                  .agg(pl.len().alias("n"))
                  .sort("n", descending=True)
                  .head(n))
            return [
                {"label": (r[col_expr] if r[col_expr] is not None else "unknown"),
                 "n": int(r["n"])}
                for r in vc.iter_rows(named=True)
            ]
        except Exception:
            return []

    countries = top_counts("origin_country")
    # airline = callsign prefix (strip, first 3 chars)
    airlines = []
    try:
        vc = (df.with_columns(pl.col("callsign").str.strip_chars().str.slice(0, 3).alias("al"))
              .group_by("al").agg(pl.len().alias("n"))
              .sort("n", descending=True).head(8))
        airlines = [
            {"label": r["al"] or "n/a", "n": int(r["n"])}
            for r in vc.iter_rows(named=True)
        ]
    except Exception:
        pass

    # altitude buckets (ft): <5k (low/approach), 5-20k (climb/descent), 20-35k, >35k
    alt_buckets = {"< 5k": 0, "5-20k": 0, "20-35k": 0, "> 35k": 0}
    try:
        for alt in df["baro_altitude"].to_list():
            a = float(alt) if alt is not None else 0.0
            if a < 5000:
                alt_buckets["< 5k"] += 1
            elif a < 20000:
                alt_buckets["5-20k"] += 1
            elif a < 35000:
                alt_buckets["20-35k"] += 1
            else:
                alt_buckets["> 35k"] += 1
    except Exception:
        pass

    return {
        "total": len(df),
        "countries": countries,
        "airlines": airlines,
        "altitudes": [
            {"label": k, "n": v} for k, v in alt_buckets.items() if v > 0
        ],
    }


def patterns_snapshot() -> dict:
    store = S3Store()

    def load():
        by_day = hourly_by_day(store, days=14)

        # hour-of-day profile
        hod = defaultdict(list)
        for df in by_day.values():
            for r in df.iter_rows(named=True):
                hod[int(r["hour_start"].hour)].append(float(r["flight_count"]))
        hour_profile = [
            {"hour": h, "mean": round(sum(v) / len(v), 1), "n": len(v)}
            for h, v in sorted(hod.items())
        ]

        # weekday totals + day-part doughnut
        wd_total = defaultdict(list)
        day_parts = {"Night (0-6)": 0, "Morning (6-12)": 0,
                     "Afternoon (12-18)": 0, "Evening (18-24)": 0}
        for dstr, df in by_day.items():
            total = df.select(pl.col("flight_count").sum()).item()
            wd = datetime.fromisoformat(dstr).isoweekday()
            wd_total[wd].append(float(total))
            # day-part = average hourly count per part (sum/6 per part)
            for r in df.iter_rows(named=True):
                h = int(r["hour_start"].hour)
                part = "Night (0-6)" if h < 6 else (
                    "Morning (6-12)" if h < 12 else (
                        "Afternoon (12-18)" if h < 18 else "Evening (18-24)"))
                day_parts[part] += float(r["flight_count"])
        weekday_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        weekday_profile = [
            {"weekday": weekday_names[w - 1], "mean_total": round(sum(v) / len(v))}
            for w, v in sorted(wd_total.items())
        ]
        part_profile = [
            {"part": k, "total": round(v)} for k, v in day_parts.items()
        ]

        # anomaly detection (last 3 days vs trailing)
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
                    if abs(dev) > 20:
                        anomalies.append({
                            "date": d,
                            "mean": round(day_mean, 1),
                            "trail_mean": round(trail_mean, 1),
                            "deviation_pct": round(dev, 1),
                        })

        # last-7-day overlay of hourly curves for the trend chart
        days7 = days_sorted[-7:]
        overlay = [
            {
                "date": d,
                "weekday": datetime.fromisoformat(d).strftime("%a"),
                "hours": [
                    {"hour": int(r["hour_start"].hour),
                     "count": round(float(r["flight_count"]), 1)}
                    for r in by_day[d].iter_rows(named=True)
                ],
            }
            for d in days7
        ]
        return {
            "hour_profile": hour_profile,
            "weekday_profile": weekday_profile,
            "day_parts": part_profile,
            "anomalies": anomalies,
            "overlay": overlay,
        }

    return _cached("patterns", load)


def forecasts_snapshot() -> dict:
    store = S3Store()

    def load():
        files = store.forecast_files()
        latest_fc = store.read_forecast(files[-1]) if files else None

        actuals: dict[str, float] = {}
        for dt in recent_raw_days(2):
            df = store._day_hourly(dt)
            if df is not None and not df.is_empty():
                for r in df.iter_rows(named=True):
                    actuals[r["hour_start"].isoformat()] = float(r["flight_count"])

        model_h1: dict[str, list] = defaultdict(list)
        for fkey in files:
            fc = store.read_forecast(fkey)
            if not fc or "models" not in fc:
                continue
            for name, m in fc["models"].items():
                h1 = m.get("hourly")
                if h1:
                    model_h1[name].append({
                        "target": h1["hour_start"],
                        "pred": round(float(h1["predicted_flight_count"]), 1),
                        "actual": actuals.get(h1["hour_start"]),
                    })

        # Only report accuracy for models that are still being served. The
        # archive contains history from the retired A/B model
        # (flight-traffic-forecaster); plotting it would imply it still runs.
        active_models = set(latest_fc.get("models", {})) if latest_fc else set()

        return {
            "latest_generated": latest_fc["generated_at"] if latest_fc else None,
            "latest_models": (
                {
                    n: {
                        "h1": m["hourly"]["predicted_flight_count"],
                        "series": [p["predicted_flight_count"] for p in m["quarter_daily"]],
                        "hours": [p["hour_start"][11:16] for p in m["quarter_daily"]],
                        # Registered version that produced this forecast (provenance)
                        "version": m.get("model_version"),
                    }
                    for n, m in latest_fc["models"].items()
                }
                if latest_fc and "models" in latest_fc else {}
            ),
            "h1_history": {n: v for n, v in model_h1.items() if n in active_models},
            "num_forecasts": len(files),
        }

    return _cached("forecasts", load)


def health_snapshot() -> dict:
    store = S3Store()

    def load():
        now = datetime.now(timezone.utc)
        today = now.date()
        files_today = store.list_raw_files(today)
        freshness_min = None
        if files_today:
            m = re.search(r"(\d{8})_(\d{6})", files_today[-1])
            if m:
                fname_ts = datetime.strptime(
                    m.group(0), "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
                freshness_min = round((now - fname_ts).total_seconds() / 60, 1)

        feat_days = store.feature_days()
        coverage = []
        for dt in recent_raw_days(10):
            has_raw = bool(store.list_raw_files(dt))
            coverage.append({"date": dt.isoformat(),
                             "raw": has_raw,
                             "features": dt in feat_days})

        return {
            "now_utc": now.isoformat(),
            "raw_files_today": len(files_today),
            "data_freshness_min": freshness_min,
            "feature_days": len(feat_days),
            "forecast_count": len(store.forecast_files()),
            "coverage": coverage,
        }

    return _cached("health", load)

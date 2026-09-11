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
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
import polars as pl
from loguru import logger
from src.dashboard.config import settings

# In-memory cache: key -> (expires_at, value)
_CACHE: dict[str, tuple[float, Any]] = {}
# Aggregates are derived from data that only changes when ingestion runs (every
# 15 min), so recomputing more often than that is pure waste. This TTL is the
# ceiling for derived values; individual entries may override it.
_CACHE_TTL = 180.0
# How many most-recent raw files to combine for 'live breakdown' views
LIVE_TICKS = 6
# Forecasts are written hourly, so the archive grows forever. Only the newest
# files can influence the 60-point accuracy chart, so cap the scan to stop the
# endpoint getting slower every hour.
FORECAST_FILES_MAX = 120

# Immutable-object cache: S3 objects under our prefixes are timestamped and
# never rewritten, so a parsed value can be reused for the container's life.
# This is the main cost fix: without it every request re-downloaded and
# re-parsed the entire archive (100+ forecast JSONs, ~95 raw files per day).
_OBJECTS: dict[str, tuple[Any, float | None]] = {}
_OBJECTS_MAX = 5000
# A missing object may simply not be written yet (e.g. today's feature file is
# produced the following day), so absence is cached only briefly.
_NEGATIVE_TTL = 300.0


def _cached(key: str, loader, ttl: float = _CACHE_TTL):
    now = time.time()
    hit = _CACHE.get(key)
    if hit and hit[0] > now:
        return hit[1]
    value = loader()
    _CACHE[key] = (now + ttl, value)
    return value


# Aggregates are published as a small JSON payload in S3. A freshly started
# container has no warm S3/parquet cache, so without this every visit paid the
# full rebuild -- measured at ~159s of execution across the four endpoints, and
# repeated on every page load. Serving a ~21 KiB payload instead lets
# containers scale to zero between visits and answer a cold request in one
# small GET.
_PAYLOAD_PREFIX = "dashboard/cache"
# Rebuilding is cheap now (~2.5s, down from ~159s), so this can be short enough
# to stay close to the 15-minute ingestion cadence without meaningful cost.
_PAYLOAD_TTL = 300.0
_PAYLOAD_MEMO_TTL = 60.0


def _payload(store, name: str, compute):
    """Serve a precomputed payload, rebuilding at most once per _PAYLOAD_TTL.

    Publishing expires globally rather than per container, so total rebuild
    work is bounded by time instead of by traffic or container churn.
    """
    now = time.time()
    memo = _CACHE.get(f"payload:{name}")
    if memo and memo[0] > now:
        return memo[1]

    key = f"{_PAYLOAD_PREFIX}/{name}.json"
    published = store.get_payload(key)
    if published and (now - published.get("computed_at", 0)) < _PAYLOAD_TTL:
        _CACHE[f"payload:{name}"] = (now + _PAYLOAD_MEMO_TTL, published["data"])
        return published["data"]

    value = compute()
    store.put_payload(key, {"computed_at": now, "data": value})
    _CACHE[f"payload:{name}"] = (now + _PAYLOAD_MEMO_TTL, value)
    return value


def _cached_object(key: str, loader):
    """Return a parsed S3 object, cached by key.

    Present objects never change, so they are cached indefinitely (bounded by
    _OBJECTS_MAX with oldest-first eviction). Absent objects are cached only
    for _NEGATIVE_TTL.
    """
    now = time.time()
    hit = _OBJECTS.get(key)
    if hit is not None:
        value, expires = hit
        if expires is None or expires > now:
            return value

    value = loader()
    if value is None:
        _OBJECTS[key] = (None, now + _NEGATIVE_TTL)
        return None
    if len(_OBJECTS) >= _OBJECTS_MAX:
        for stale in list(_OBJECTS)[: _OBJECTS_MAX // 10]:
            _OBJECTS.pop(stale, None)
    _OBJECTS[key] = (value, None)
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

        def load():
            keys = []
            for page in self._client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=prefix
            ):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".parquet"):
                        keys.append(obj["Key"])
            return keys

        # Listing must be refetched as new files land, but not on every request.
        return _cached(f"raw_list:{dt.isoformat()}", load, ttl=120.0)

    def read_parquet(self, key: str) -> pl.DataFrame | None:
        def load():
            try:
                resp = self._client.get_object(Bucket=self.bucket, Key=key)
                return pl.read_parquet(io.BytesIO(resp["Body"].read()))
            except Exception as e:
                logger.warning(f"read failed {key}: {e}")
                return None

        return _cached_object(key, load)

    def _day_hourly(self, dt: date) -> pl.DataFrame:
        ttl = 120.0 if dt == datetime.now(timezone.utc).date() else 900.0

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
                })
            raw = pl.concat(dfs, how="diagonal_relaxed")
            return (
                raw.with_columns(
                    pl.from_epoch(pl.col("capture_time"), time_unit="s")
                    .dt.truncate("1h")
                    .alias("hour_start")
                )
                .group_by("hour_start")
                .agg(pl.col("icao24").n_unique().alias("flight_count"))
                .sort("hour_start")
            )

        return _cached(f"day_hourly:{dt.isoformat()}", load, ttl=ttl)

    def latest_snapshot(self) -> dict:
        """Aircraft visible in the most recent raw snapshot, and its timestamp.

        Each raw parquet file is a single OpenSky poll (exactly one
        capture_time), so this count is an INSTANTANEOUS observation, not an
        hourly aggregate. Returns {} when no snapshot is available.
        """
        today = datetime.now(timezone.utc).date()

        def load():
            files = self.list_raw_files(today)
            if not files:
                files = self.list_raw_files(today - timedelta(days=1))
            if not files:
                return {}
            df = self.read_parquet(files[-1])
            if df is None or df.is_empty() or "capture_time" not in df.columns:
                return {}
            captured = df["capture_time"].max()
            return {
                "count": int(df["icao24"].n_unique()),
                "captured_at": datetime.fromtimestamp(
                    int(captured), timezone.utc
                ).isoformat(),
            }

        return _cached("latest_snapshot", load, ttl=120.0)

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

        return _cached("latest_ticks", load, ttl=120.0)

    # ---------- hourly features ----------

    def feature_keys(self) -> list[str]:
        def load():
            keys = []
            for page in self._client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=settings.s3.features_prefix
            ):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".parquet"):
                        keys.append(obj["Key"])
            return keys

        return _cached("feature_keys", load, ttl=300.0)

    def read_feature_day(self, key: str) -> pl.DataFrame | None:
        def load():
            if key not in self.feature_keys():
                return None
            return self.read_parquet(key)

        return _cached_object(key, load)

    def feature_days(self) -> set[date]:
        days = set()
        for key in self.feature_keys():
            m = re.search(r"features_(\d{4})-(\d{2})-(\d{2})", key)
            if m:
                days.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
        return days

    # ---------- forecasts ----------

    def get_payload(self, key: str) -> dict | None:
        try:
            resp = self._client.get_object(Bucket=self.bucket, Key=key)
            return json.loads(resp["Body"].read())
        except Exception:
            return None

    def put_payload(self, key: str, value: dict) -> None:
        try:
            self._client.put_object(
                Bucket=self.bucket, Key=key,
                Body=json.dumps(value).encode(),
                ContentType="application/json",
            )
        except Exception as e:
            logger.warning(f"payload write failed {key}: {e}")

    def forecast_files(self) -> list[str]:
        def load():
            keys = []
            for page in self._client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=settings.s3.forecasts_prefix
            ):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        keys.append(obj["Key"])
            # Keys embed a timestamp, so lexical order is chronological.
            return sorted(keys)[-FORECAST_FILES_MAX:]

        return _cached("forecast_files", load, ttl=120.0)

    def read_forecast(self, key: str) -> dict | None:
        def load():
            try:
                resp = self._client.get_object(Bucket=self.bucket, Key=key)
                return json.loads(resp["Body"].read())
            except Exception as e:
                logger.warning(f"forecast read failed {key}: {e}")
                return None

        return _cached_object(key, load)


def _hour_key(value: object) -> str:
    """Canonical hour key so forecasts and actuals can be joined.

    The parquet actuals carry NAIVE datetimes (polars reads hour_start with
    time_zone=None) while forecast JSON stores tz-aware ISO strings
    ("2026-09-11T13:00:00+00:00"). Keying both through this function makes the
    two match; without it every actual lookup misses and the forecast charts
    show no actuals at all.
    """
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime) and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    if isinstance(value, datetime):
        value = value.astimezone(timezone.utc)
        return value.strftime("%Y-%m-%dT%H:%M")
    return str(value)


def recent_raw_days(days: int = 10) -> list[date]:
    today = datetime.now(timezone.utc).date()
    return [today - timedelta(days=i) for i in range(days - 1, -1, -1)]


def feature_key_for(dt: date) -> str:
    return (f"{settings.s3.features_prefix}/year={dt.year}/month={dt.month:02d}/"
            f"features_{dt.isoformat()}.parquet")


def hourly_day(store: S3Store, dt: date) -> pl.DataFrame:
    """Hourly counts for one day: feature file when available, else raw.

    A feature file holds a complete day of hourly counts in ONE object, whereas
    the raw path must list and read ~95 parquet files. Preferring features is
    the difference between 1 and 95 S3 reads per day, which matters because the
    live and forecast views span several days. Today's features are not written
    until the next day, so today still falls back to raw.
    """
    feat = store.read_feature_day(feature_key_for(dt))
    if feat is not None and not feat.is_empty():
        return feat.select(["hour_start", "flight_count"])
    return store._day_hourly(dt)


def hourly_by_day(store: S3Store, days: int = 7) -> dict[str, pl.DataFrame]:
    out: dict[str, pl.DataFrame] = {}
    for dt in recent_raw_days(days):
        df = hourly_day(store, dt)
        if df is not None and not df.is_empty():
            out[dt.isoformat()] = df
    return out


# ---- JSON-safe aggregations ----


def live_snapshot() -> dict:
    store = S3Store()

    def load():
        today = datetime.now(timezone.utc).date()
        today_df = hourly_day(store, today)
        yesterday = today - timedelta(days=1)
        yest_df = hourly_day(store, yesterday)

        def series(df):
            return [
                {"hour": int(r["hour_start"].hour), "count": round(float(r["flight_count"]), 1)}
                for r in (df.iter_rows(named=True) if df is not None and not df.is_empty() else [])
            ]

        active = store.latest_snapshot()

        # Live breakdown from latest ticks
        ticks = store.latest_ticks()
        breakdown = _breakdown(ticks)

        return {
            "now_utc": datetime.now(timezone.utc).isoformat(),
            # Instantaneous count from the newest poll (not an hourly average)
            "active_aircraft_now": active.get("count", 0),
            "active_captured_at": active.get("captured_at"),
            "today": series(today_df),
            "yesterday": series(yest_df),
            "breakdown": breakdown,
        }

    return _payload(store, "live", load)


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

    return _payload(store, "patterns", load)


def _parse_utc(value: str) -> datetime:
    """Parse an ISO timestamp, treating a missing offset as UTC.

    fromisoformat() returns a naive datetime for offset-less strings, and
    .astimezone() would then assume local time -- wrong on any non-UTC host.
    """
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_complete_actuals(start: date, end: date) -> dict[str, float]:
    """{canonical hour: flight_count} for COMPLETE hours in [start, end].

    The current UTC hour is still being ingested, so its count is partial.
    Including it would make the accuracy line dip and unfairly penalise the
    forecast, so only finished hours are returned.
    """
    store = S3Store()
    current_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    actuals: dict[str, float] = {}
    day = start
    while day <= end:
        df = hourly_day(store, day)
        if df is not None and not df.is_empty():
            for r in df.iter_rows(named=True):
                hour = r["hour_start"]
                if hour.tzinfo is None:
                    hour = hour.replace(tzinfo=timezone.utc)
                if hour >= current_hour:
                    continue
                actuals[_hour_key(hour)] = float(r["flight_count"])
        day += timedelta(days=1)
    return actuals


def _error_stats(pairs: list[tuple[float, float]]) -> dict:
    """MAPE / MAE / bias for (predicted, actual) pairs.

    MAPE is the mean absolute percentage error; MAE is in aircraft. Both are
    computed over scored hours only (actuals that have completed).
    """
    if not pairs:
        return {"n": 0, "mape": None, "mae": None, "bias_pct": None}
    maes = [abs(p - a) for p, a in pairs]
    pcts = [abs(p - a) / a * 100 for p, a in pairs if a]
    biases = [(p - a) / a * 100 for p, a in pairs if a]
    return {
        "n": len(pairs),
        "mape": round(sum(pcts) / len(pcts), 1) if pcts else None,
        "mae": round(sum(maes) / len(maes), 2),
        "bias_pct": round(sum(biases) / len(biases), 1) if biases else None,
    }


def forecasts_snapshot() -> dict:
    store = S3Store()

    def load():
        files = store.forecast_files()
        latest_fc = store.read_forecast(files[-1]) if files else None

        model_h1: dict[str, list] = defaultdict(list)
        for fkey in files:
            fc = store.read_forecast(fkey)
            if not fc or "models" not in fc:
                continue
            generated = fc.get("generated_at", "")
            for name, m in fc["models"].items():
                h1 = m.get("hourly")
                if h1:
                    model_h1[name].append({
                        "target": h1["hour_start"],
                        "pred": round(float(h1["predicted_flight_count"])),
                        "actual": None,
                        "generated": generated,
                    })

        # Several forecast runs can target the same hour (when the last actual
        # hour hasn't advanced between runs), which would repeat x-axis labels
        # and zig-zag the line. Keep the most recent prediction per target hour.
        model_h1 = {
            name: sorted(
                {row["target"]: row for row in rows}.values(),
                key=lambda r: r["target"],
            )
            for name, rows in model_h1.items()
        }

        # Only report accuracy for models that are still being served. The
        # archive contains history from the retired A/B model
        # (flight-traffic-forecaster); plotting it would imply it still runs.
        active_models = set(latest_fc.get("models", {})) if latest_fc else set()
        model_h1 = {n: v for n, v in model_h1.items() if n in active_models}

        # Load actuals spanning exactly the hours we display. Using a fixed
        # 2-day window left every older point without an actual (63 of 101),
        # so the accuracy line only covered the tail of the chart.
        targets = [r["target"] for rows in model_h1.values() for r in rows]
        if targets:
            parsed = [_parse_utc(t) for t in targets]
            actuals = _load_complete_actuals(
                min(parsed).date(), max(parsed).date()
            )
            for rows in model_h1.values():
                for r in rows:
                    a = actuals.get(_hour_key(r["target"]))
                    r["actual"] = round(a) if a is not None else None

        return {
            "latest_generated": latest_fc["generated_at"] if latest_fc else None,
            "latest_models": (
                {
                    n: {
                        "h1": round(m["hourly"]["predicted_flight_count"]),
                        "series": [
                            round(p["predicted_flight_count"]) for p in m["quarter_daily"]
                        ],
                        "hours": [p["hour_start"][11:16] for p in m["quarter_daily"]],
                        # Registered version that produced this forecast (provenance)
                        "version": m.get("model_version"),
                    }
                    for n, m in latest_fc["models"].items()
                }
                if latest_fc and "models" in latest_fc else {}
            ),
            "h1_history": model_h1,
            # Accuracy summary per model, over the scored hours above.
            "accuracy": {
                name: _error_stats(
                    [
                        (float(r["pred"]), float(r["actual"]))
                        for r in rows
                        if r.get("actual") is not None
                    ]
                )
                for name, rows in model_h1.items()
            },
            "num_forecasts": len(files),
        }

    return _payload(store, "forecasts", load)


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

    return _payload(store, "health", load)

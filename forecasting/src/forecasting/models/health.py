"""Pipeline health checks and Discord alerting.

Two failures in this project went unnoticed for days because nothing was
watching the pipeline:

  - the scheduled retrain crashed on a missing dependency (found 3 days later)
  - the upstream capture rate halved overnight and stayed halved (found a week
    later)

Both were plainly visible in data already in S3, so these checks read S3 only
and cost one listing plus a handful of small objects.

They run as part of the hourly forecast rather than as their own scheduled
function, because Modal's plan allows 5 scheduled functions and all 5 are used.
"""

import io
import json
import os
from datetime import datetime, timedelta, timezone
from statistics import median

import boto3
import polars as pl
from loguru import logger
from src.forecasting.config import settings

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
DISCORD_ENABLED = os.environ.get("DISCORD_ENABLED", "true").lower() == "true"

STATE_KEY = "alerts/health_state.json"

# Ingestion runs every 15 minutes.
STALE_RAW_WARN_MIN = 45
STALE_RAW_CRIT_MIN = 120
# Forecasts run hourly.
STALE_FORECAST_WARN_MIN = 150
# Alert if the latest day's hourly density falls this far below the trailing
# Alert if the latest day's hourly density falls below this fraction of the
# trailing median. This is the check that would have caught 2026-09-07, when the
# capture rate halved overnight (17 -> 8 aircraft per poll, ~55 -> ~29 per hour)
# and stayed there. Calibration matters: that event measured 29/55 = 0.53, so a
# 0.5 threshold would have missed it. Post-change days vary by only ~3% day to
# day, so 0.70 catches a real collapse while staying clear of normal variation.
COVERAGE_MIN_RATIO = 0.70
COVERAGE_LOOKBACK_DAYS = 8
# Repeat an unresolved alert at most this often, so a persistent problem does
# not post every single hour.
REALERT_AFTER = timedelta(hours=6)


def _s3():
    return boto3.client(
        "s3",
        region_name=settings.s3.region,
        aws_access_key_id=settings.s3.access_key_id,
        aws_secret_access_key=settings.s3.secret_access_key,
    )


def _newest_age_min(keys_with_times: list[tuple[datetime, str]], now: datetime) -> float | None:
    if not keys_with_times:
        return None
    newest = max(t for t, _ in keys_with_times)
    return (now - newest).total_seconds() / 60


def _list_times(prefix: str) -> list[tuple[datetime, str]]:
    """[(LastModified, key)] for a prefix."""
    s3 = _s3()
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=settings.s3.bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            out.append((obj["LastModified"], obj["Key"]))
    return out


def _daily_hourly_means(days: int, now: datetime) -> list[tuple[str, float]]:
    """Mean hourly flight_count per day for the most recent `days` feature files."""
    s3 = _s3()
    out = []
    for i in range(days):
        d = (now - timedelta(days=i + 1)).date()
        key = f"{settings.s3.features_prefix}/year={d.year}/month={d.month:02d}/features_{d.isoformat()}.parquet"
        try:
            body = s3.get_object(Bucket=settings.s3.bucket_name, Key=key)["Body"].read()
            df = pl.read_parquet(io.BytesIO(body))
            if not df.is_empty():
                out.append((d.isoformat(), float(df["flight_count"].mean())))
        except Exception:
            continue
    return sorted(out)


def coverage_ratio(daily: list[tuple[str, float]]) -> float | None:
    """Latest day's mean density as a fraction of the trailing median.

    Returns None when there is not enough history to judge.
    """
    if len(daily) < 4:
        return None
    *trailing, (_, latest) = daily
    base = median(m for _, m in trailing)
    if base <= 0:
        return None
    return latest / base


def gather(now: datetime | None = None) -> dict:
    """Collect health issues and the metrics behind them. Never raises."""
    now = now or datetime.now(timezone.utc)
    issues: list[dict] = []
    metrics: dict = {}

    # 1. Is ingestion still writing?
    try:
        day = now.date()
        raw = _list_times(f"{settings.s3.raw_prefix}/year={day.year}/month={day.month:02d}/")
        if not raw:
            day = day - timedelta(days=1)
            raw = _list_times(f"{settings.s3.raw_prefix}/year={day.year}/month={day.month:02d}/")
        age = _newest_age_min(raw, now)
        metrics["raw_age_min"] = None if age is None else round(age, 1)
        if age is None:
            issues.append(
                {
                    "key": "ingestion_missing",
                    "severity": "critical",
                    "detail": "no raw files found for today or yesterday",
                }
            )
        elif age >= STALE_RAW_CRIT_MIN:
            issues.append(
                {
                    "key": "ingestion_stale",
                    "severity": "critical",
                    "detail": f"newest raw file is {age:.0f} min old (runs every 15 min)",
                }
            )
        elif age >= STALE_RAW_WARN_MIN:
            issues.append(
                {"key": "ingestion_stale", "severity": "warn", "detail": f"newest raw file is {age:.0f} min old"}
            )
    except Exception as e:
        logger.warning(f"health: raw check failed: {e}")

    # 2. Has the amount of traffic being captured collapsed?
    try:
        daily = _daily_hourly_means(COVERAGE_LOOKBACK_DAYS, now)
        ratio = coverage_ratio(daily)
        metrics["coverage_days"] = len(daily)
        metrics["coverage_ratio"] = None if ratio is None else round(ratio, 2)
        if ratio is not None and ratio < COVERAGE_MIN_RATIO:
            latest_day, latest_mean = daily[-1]
            trailing = [m for _, m in daily[:-1]]
            issues.append(
                {
                    "key": "coverage_drop",
                    "severity": "critical",
                    "detail": (
                        f"{latest_day} averaged {latest_mean:.1f} aircraft/hour, "
                        f"{ratio * 100:.0f}% of the trailing median "
                        f"({median(trailing):.1f}) - check the capture rate "
                        f"before trusting forecasts"
                    ),
                }
            )
    except Exception as e:
        logger.warning(f"health: coverage check failed: {e}")

    # 3. Is the forecast pipeline still producing?
    try:
        fc = _list_times(f"{settings.s3.forecast_prefix}/year={now.year}/month={now.month:02d}/")
        age = _newest_age_min(fc, now)
        metrics["forecast_age_min"] = None if age is None else round(age, 1)
        if age is None:
            issues.append(
                {"key": "forecast_missing", "severity": "critical", "detail": "no forecasts found for this month"}
            )
        elif age >= STALE_FORECAST_WARN_MIN:
            issues.append(
                {
                    "key": "forecast_stale",
                    "severity": "warn",
                    "detail": f"newest forecast is {age:.0f} min old (runs hourly)",
                }
            )
    except Exception as e:
        logger.warning(f"health: forecast check failed: {e}")

    return {"ok": not issues, "issues": issues, "metrics": metrics}


def should_alert(previous: set[str], current: set[str], last_sent: datetime | None, now: datetime) -> bool:
    """Alert on a change of state, on recovery, or periodically while unresolved."""
    if current != previous:
        return True  # new problem, changed problem, or recovery (current empty)
    if current and (last_sent is None or now - last_sent >= REALERT_AFTER):
        return True  # still broken; remind
    return False


def _read_state() -> dict:
    try:
        raw = _s3().get_object(Bucket=settings.s3.bucket_name, Key=STATE_KEY)["Body"].read()
        return json.loads(raw)
    except Exception:
        return {}


def _write_state(state: dict) -> None:
    try:
        _s3().put_object(
            Bucket=settings.s3.bucket_name,
            Key=STATE_KEY,
            Body=json.dumps(state).encode(),
            ContentType="application/json",
        )
    except Exception as e:
        logger.warning(f"health: could not persist state: {e}")


def _send(issues: list[dict], metrics: dict, now: datetime) -> bool:
    if not DISCORD_ENABLED or not DISCORD_WEBHOOK_URL:
        print("health: Discord disabled or no webhook — not sending")
        return False
    if issues:
        worst = "critical" if any(i["severity"] == "critical" for i in issues) else "warn"
        lines = "\n".join(f"• **{i['key']}** — {i['detail']}" for i in issues)
        embed = {
            "title": "🚨 Aeroflow pipeline health",
            "color": 0xE01B24 if worst == "critical" else 0xF5A623,
            "description": lines,
            "footer": {"text": f"{now:%Y-%m-%d %H:%M UTC}"},
        }
        content = None
    else:
        embed = {
            "title": "✅ Aeroflow pipeline recovered",
            "color": 0x2ECC71,
            "description": "All health checks passing again.",
            "footer": {"text": f"{now:%Y-%m-%d %H:%M UTC}"},
        }
        content = None
    try:
        import httpx

        with httpx.Client(timeout=20) as client:
            r = client.post(DISCORD_WEBHOOK_URL, json={"content": content, "embeds": [embed]})
        if r.status_code in (200, 204):
            print("health: alert sent to Discord")
            return True
        print(f"health: Discord webhook failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        logger.warning(f"health: alert send failed: {e}")
    return False


def alert_if_unhealthy(now: datetime | None = None) -> dict:
    """Run the checks and alert on state change / persistently. Never raises."""
    now = now or datetime.now(timezone.utc)
    try:
        result = gather(now)
        state = _read_state()
        previous = set(state.get("issues", []))
        current = {i["key"] for i in result["issues"]}
        last_sent = None
        if state.get("last_sent"):
            try:
                last_sent = datetime.fromisoformat(state["last_sent"])
            except ValueError:
                last_sent = None

        sent = False
        if should_alert(previous, current, last_sent, now):
            sent = _send(result["issues"], result["metrics"], now)
            if sent:
                _write_state({"issues": sorted(current), "last_sent": now.isoformat()})
        else:
            # keep metrics fresh without alerting
            _write_state({**state, "issues": sorted(current)})

        print(f"health: ok={result['ok']} issues={sorted(current)} metrics={result['metrics']}")
        return {**result, "alerted": sent}
    except Exception as e:
        logger.warning(f"health check failed: {e}")
        return {"ok": True, "issues": [], "metrics": {}, "alerted": False, "error": str(e)}

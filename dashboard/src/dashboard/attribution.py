"""Grounded, keyless explanations for genuine traffic deviations.

Deliberately not an LLM lookup. The largest source of apparent "traffic
anomalies" in this feed is a change in how traffic is MEASURED rather than real
traffic: the 2026-09-07 capture-rate collapse made a whole week of days look
like a 30% shortfall. Asking a search engine why those days were quiet returns
plausible nonsense - weather, a strike - for what was really an upstream
coverage change. A confident wrong answer is worse than no answer.

So this speaks only for days the data layer has already judged to be genuine
transient deviations, and it offers verifiable numbers from two keyless sources
instead of prose:

  Open-Meteo   daily rainfall and peak wind gusts at Mumbai
  holidays     Indian (Maharashtra) public holidays

Both fail soft. No network means no attribution, which is fine: an anomaly with
no external evidence is still a perfectly good anomaly.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import httpx
from loguru import logger

# Mumbai (Chhatrapati Shivaji Maharaj International).
MUMBAI_LAT, MUMBAI_LON = 19.09, 72.87

# External evidence is only offered for deviations the data layer called
# transient. A measurement change - which dominates the flags in this feed - has
# no weather or calendar explanation, so attributing one would be fabrication.
ATTRIBUTABLE_CAUSE = "transient deviation"

# Mumbai context: through the June-September monsoon, a few mm of rain in a day
# is unremarkable, so a low threshold would produce noise rather than evidence.
NOTABLE_RAIN_MM = 10.0
NOTABLE_GUST_KMH = 60.0

WEATHER_TTL = 6 * 3600.0
CALENDAR_TTL = 24 * 3600.0

# Open-Meteo serves up to 92 past days in one call, which comfortably covers the
# window the anomaly detector looks at.
_LOOKBACK_DAYS = 92

# Local TTL memo. Deliberately not data.py's cache: that module imports this
# one, so importing back would be a cycle.
_MEMO: dict[str, tuple[float, Any]] = {}


def _memo(key: str, loader, ttl: float):
    now = time.time()
    hit = _MEMO.get(key)
    if hit and hit[0] > now:
        return hit[1]
    value = loader()
    _MEMO[key] = (now + ttl, value)
    return value


def _at(values: list | None, index: int):
    if not values or index >= len(values):
        return None
    return values[index]


def _fetch_weather() -> dict[str, dict[str, float | None]]:
    """Daily Mumbai weather for the last ~3 months, keyed by ISO date.

    Returns an empty mapping on any failure - attribution is a nicety, never a
    reason for the dashboard to break.
    """
    try:
        resp = httpx.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": MUMBAI_LAT,
                "longitude": MUMBAI_LON,
                "daily": "precipitation_sum,wind_gusts_10m_max",
                "past_days": _LOOKBACK_DAYS,
                "forecast_days": 2,
                "timezone": "UTC",
            },
            timeout=20.0,
        )
        resp.raise_for_status()
        daily = resp.json().get("daily") or {}
    except Exception as e:
        logger.warning(f"weather lookup failed (attribution skipped): {e}")
        return {}

    out: dict[str, dict[str, float | None]] = {}
    for i, day in enumerate(daily.get("time", [])):
        out[day] = {
            "rain_mm": _at(daily.get("precipitation_sum"), i),
            "gust_kmh": _at(daily.get("wind_gusts_10m_max"), i),
        }
    return out


def weather_by_day(days: list[date]) -> dict[str, dict[str, float | None]]:
    """Mumbai weather for the requested days (one upstream call for all of them)."""
    if not days:
        return {}
    all_days = _memo("weather:mumbai", _fetch_weather, WEATHER_TTL)
    wanted = {d.isoformat() for d in days}
    return {k: v for k, v in all_days.items() if k in wanted}


def holiday_note(day: date) -> str | None:
    """Indian (Maharashtra) public holiday name for the day, if any."""

    def load():
        import holidays as hol

        return hol.country_holidays("IN", years=[day.year], subdiv="MH")

    try:
        calendar = _memo(f"holidays:IN:MH:{day.year}", load, CALENDAR_TTL)
        name = calendar.get(day)
    except Exception as e:
        logger.warning(f"holiday lookup failed for {day}: {e}")
        return None

    if not name:
        return None
    # The calendar joins several observances with "; " - the first is the day's
    # primary name.
    return f"{str(name).split(';')[0].strip()} (public holiday, Maharashtra)"


def _weather_note(weather: dict[str, float | None], deviation_pct: float) -> str | None:
    """Notable weather, and only where it could plausibly explain the direction.

    Rain suppresses traffic; it does not produce a surge. Reporting it against a
    rise would invite the reader to infer a cause the evidence does not support.
    """
    if deviation_pct >= 0:
        return None

    bits = []
    rain = weather.get("rain_mm")
    gust = weather.get("gust_kmh")
    if rain is not None and rain >= NOTABLE_RAIN_MM:
        bits.append(f"{rain:.0f}mm rain")
    if gust is not None and gust >= NOTABLE_GUST_KMH:
        bits.append(f"peak gusts {gust:.0f}km/h")
    if not bits:
        return None
    return "Mumbai recorded " + " with ".join(bits)


def attribute(day: date, deviation_pct: float, cause: str) -> dict[str, Any] | None:
    """External evidence for a flagged day, or None when there is none to offer.

    Args:
        day: The flagged calendar day
        deviation_pct: Signed deviation from the trailing baseline
        cause: The data layer's own classification of the deviation

    Returns:
        {"text": str, "sources": list[str]}, or None
    """
    if cause != ATTRIBUTABLE_CAUSE:
        return None

    notes: list[str] = []
    sources: list[str] = []

    holiday = holiday_note(day)
    if holiday:
        notes.append(holiday)
        sources.append("public holidays (IN/MH)")

    weather = weather_by_day([day]).get(day.isoformat())
    if weather:
        note = _weather_note(weather, deviation_pct)
        if note:
            notes.append(note)
            sources.append("open-meteo.com")

    if not notes:
        return None
    return {"text": " · ".join(notes), "sources": sources}


__all__ = [
    "ATTRIBUTABLE_CAUSE",
    "NOTABLE_GUST_KMH",
    "NOTABLE_RAIN_MM",
    "attribute",
    "holiday_note",
    "weather_by_day",
]

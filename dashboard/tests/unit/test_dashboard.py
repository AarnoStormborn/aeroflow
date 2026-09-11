"""Unit tests for dashboard data helpers (pure logic, no live S3)."""

from datetime import datetime, timezone

from src.dashboard.data import _cached, _hour_key, recent_raw_days


def test_recent_raw_days_length_and_order():
    days = recent_raw_days(7)
    assert len(days) == 7
    assert days == sorted(days)
    today = datetime.now(timezone.utc).date()
    assert days[-1] == today


def test_hour_key_matches_naive_and_aware():
    """Regression: forecast targets are tz-aware ISO strings, but polars reads
    hour_start from parquet as a NAIVE datetime. Keying both through _hour_key
    is what makes actuals join to forecasts at all.

    Before this, every actual lookup returned None, so the forecast-accuracy
    chart drew the predicted line with no actuals.
    """
    naive = datetime(2026, 9, 11, 13, 0)  # what parquet yields
    aware = datetime(2026, 9, 11, 13, 0, tzinfo=timezone.utc)  # what the API stores
    iso = "2026-09-11T13:00:00+00:00"  # what forecast JSON contains

    assert _hour_key(naive) == _hour_key(aware) == _hour_key(iso)


def test_hour_key_normalises_non_utc_offsets():
    """An offset timestamp must collapse onto the same UTC hour bucket."""
    from datetime import timedelta

    utc = datetime(2026, 9, 11, 13, 0, tzinfo=timezone.utc)
    plus_two = datetime(2026, 9, 11, 15, 0, tzinfo=timezone(timedelta(hours=2)))
    assert _hour_key(utc) == _hour_key(plus_two)


def test_hour_key_distinguishes_hours():
    a = datetime(2026, 9, 11, 13, 0, tzinfo=timezone.utc)
    b = datetime(2026, 9, 11, 14, 0, tzinfo=timezone.utc)
    assert _hour_key(a) != _hour_key(b)
    calls = {"n": 0}

    def loader():
        calls["n"] += 1
        return "value"

    from src.dashboard.data import _CACHE
    _CACHE.clear()

    assert _cached("t", loader) == "value"
    assert _cached("t", loader) == "value"
    assert calls["n"] == 1  # only computed once (within TTL)


def test_cached_respects_ttl():
    import time as time_mod

    from src.dashboard.data import _CACHE
    _CACHE.clear()

    calls = {"n": 0}

    def loader():
        calls["n"] += 1
        return calls["n"]

    _cached("ttl", loader, ttl=0.01)
    first = _cached("ttl", loader, ttl=0.01)
    assert first == 1  # fresh within ttl
    time_mod.sleep(0.02)
    second = _cached("ttl", loader, ttl=0.01)
    assert second == 2  # recomputed after ttl

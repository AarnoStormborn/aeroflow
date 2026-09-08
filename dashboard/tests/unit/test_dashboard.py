"""Unit tests for dashboard data helpers (pure logic, no live S3)."""

from datetime import datetime, timezone

from src.dashboard.data import _cached, recent_raw_days


def test_recent_raw_days_length_and_order():
    days = recent_raw_days(7)
    assert len(days) == 7
    assert days == sorted(days)
    today = datetime.now(timezone.utc).date()
    assert days[-1] == today


def test_cached_computes_once():
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

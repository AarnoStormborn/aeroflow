"""Unit tests for dashboard data helpers (pure logic, no live S3)."""

from datetime import datetime, timezone

from src.dashboard.data import _cached, _error_stats, _hour_key, recent_raw_days


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


def test_error_stats_basic():
    """MAPE/MAE over (predicted, actual) pairs."""
    # abs errs 2, 4, 3 ; pct errs 20%, 20%, 10%
    stats = _error_stats([(8.0, 10.0), (24.0, 20.0), (27.0, 30.0)])
    assert stats["n"] == 3
    assert stats["mae"] == 3.0
    assert stats["mape"] == 16.7


def test_error_stats_empty_is_null_not_zero():
    """No scored hours must read as 'unknown', not as a perfect score."""
    assert _error_stats([]) == {"n": 0, "mape": None, "mae": None, "bias_pct": None}


def test_error_stats_ignores_zero_actuals_for_percentage():
    """MAPE is undefined when actual is 0, but MAE still counts that hour."""
    stats = _error_stats([(5.0, 0.0), (10.0, 10.0)])
    assert stats["n"] == 2
    assert stats["mae"] == 2.5
    assert stats["mape"] == 0.0


def test_error_stats_bias_sign():
    """Positive bias means over-prediction."""
    assert _error_stats([(20.0, 10.0)])["bias_pct"] == 100.0
    assert _error_stats([(10.0, 20.0)])["bias_pct"] == -50.0


def test_security_headers_present_on_every_response():
    """Hardening headers, including a strict CSP with no 'unsafe-inline'."""
    from fastapi.testclient import TestClient
    from src.dashboard.app import _SECURITY_HEADERS, app

    c = TestClient(app)
    for path in ("/", "/static/app.js", "/api/health"):
        r = c.get(path)
        for header in _SECURITY_HEADERS:
            assert r.headers.get(header), f"{header} missing on {path}"


def test_csp_forbids_inline_script_and_framing():
    from fastapi.testclient import TestClient
    from src.dashboard.app import app

    csp = TestClient(app).get("/").headers["content-security-policy"]
    assert "script-src 'self'" in csp
    # the theme bootstrap was moved to /static/theme.js to make this possible
    assert "'unsafe-inline'" not in csp
    assert "frame-ancestors 'none'" in csp
    # fonts are the only external origin allowed
    assert "https://fonts.googleapis.com" in csp


def test_html_has_no_inline_script_or_style():
    """A single inline block would silently force 'unsafe-inline' back into CSP."""
    from src.dashboard.app import _STATIC

    html = (_STATIC / "index.html").read_text()
    assert "<script>" not in html, "inline <script> found — move it to a static file"
    assert 'style="' not in html, "inline style attribute found — CSP would need 'unsafe-inline'"


def test_js_has_no_inline_style_attributes():
    """innerHTML templates must not set style="..." — a strict CSP blocks those
    (style-src-attr), which silently drops the styling."""
    import re

    from src.dashboard.app import _STATIC

    js = (_STATIC / "app.js").read_text()
    found = re.findall(r'style=\\?["\']', js)
    assert not found, f"inline style attribute(s) in app.js: {found}"

"""Tests for the pipeline health checks and alerting policy.

Motivated by two failures that went unnoticed for days: a retrain that crashed
on a missing dependency, and a capture rate that halved overnight and stayed
halved. Both were visible in S3 the whole time.
"""

from datetime import datetime, timedelta, timezone

from src.forecasting.models.health import (
    COVERAGE_MIN_RATIO,
    REALERT_AFTER,
    coverage_ratio,
    should_alert,
)

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)


# ---- coverage ------------------------------------------------------------

def test_coverage_ratio_flat_is_one():
    daily = [("d1", 30.0), ("d2", 30.0), ("d3", 30.0), ("d4", 30.0)]
    assert coverage_ratio(daily) == 1.0


def test_coverage_ratio_detects_the_sep7_style_halving():
    """The real case: 17 -> 8 aircraft per poll, ~55 -> ~29 per hour."""
    trailing = [("d", 55.0 + i) for i in range(7)]  # median 58
    daily = [*trailing, ("2026-09-07", 29.0)]        # 29/58 = 0.50
    ratio = coverage_ratio(daily)
    assert ratio is not None
    assert ratio < COVERAGE_MIN_RATIO  # must definitely fire


def test_coverage_ratio_stays_clear_of_normal_variation():
    """Post-change days vary by only ~3%, so no false positive."""
    values = [29.8, 28.3, 28.5, 28.7, 28.7, 27.9]
    daily = [(f"d{i}", v) for i, v in enumerate(values)]
    ratio = coverage_ratio(daily)
    assert ratio is not None
    assert ratio >= COVERAGE_MIN_RATIO


def test_coverage_ratio_none_without_enough_history():
    assert coverage_ratio([("d1", 30.0), ("d2", 30.0)]) is None


def test_coverage_ratio_ignores_zero_baseline():
    daily = [("d1", 0.0), ("d2", 0.0), ("d3", 0.0), ("d4", 5.0)]
    assert coverage_ratio(daily) is None


# ---- alerting policy -----------------------------------------------------

def test_alerts_on_new_problem():
    assert should_alert(set(), {"ingestion_stale"}, None, NOW) is True


def test_alerts_on_changed_problem():
    assert should_alert({"ingestion_stale"}, {"coverage_drop"}, None, NOW) is True


def test_alerts_on_recovery():
    """Recovery must be announced, otherwise an alert looks like it never cleared."""
    assert should_alert({"ingestion_stale"}, set(), None, NOW) is True


def test_does_not_repeat_a_quiet_all_clear():
    assert should_alert(set(), set(), None, NOW) is False


def test_does_not_respam_before_the_realert_window():
    last = NOW - timedelta(hours=1)
    assert should_alert({"coverage_drop"}, {"coverage_drop"}, last, NOW) is False


def test_repeats_an_unresolved_problem_after_the_window():
    last = NOW - REALERT_AFTER - timedelta(minutes=1)
    assert should_alert({"coverage_drop"}, {"coverage_drop"}, last, NOW) is True


def test_repeats_when_never_sent_before():
    assert should_alert({"coverage_drop"}, {"coverage_drop"}, None, NOW) is True


# ---- alert_if_unhealthy flow (S3 + Discord monkeypatched out) -------------

def test_alert_flow_alerts_and_persists_state(monkeypatch):
    import src.forecasting.models.health as h

    storage = {}
    monkeypatch.setattr(h, "DISCORD_ENABLED", "true")
    monkeypatch.setattr(h, "gather", lambda now: {
        "ok": False,
        "issues": [{"key": "coverage_drop", "severity": "critical", "detail": "t"}],
        "metrics": {},
    })
    monkeypatch.setattr(h, "_read_state", lambda: {})
    monkeypatch.setattr(h, "_write_state", lambda s: storage.update(s))
    monkeypatch.setattr(h, "_send", lambda issues, metrics, now: True)

    r = h.alert_if_unhealthy(NOW)
    assert r["alerted"] is True
    assert storage["issues"] == ["coverage_drop"]
    assert storage["last_sent"] == NOW.isoformat()


def test_alert_flow_recovery_is_announced_once_then_quiet(monkeypatch):
    import src.forecasting.models.health as h

    sent = []
    storage = {"issues": ["coverage_drop"], "last_sent": (NOW - timedelta(hours=1)).isoformat()}
    monkeypatch.setattr(h, "DISCORD_ENABLED", "true")
    monkeypatch.setattr(h, "gather", lambda now: {"ok": True, "issues": [], "metrics": {}})
    monkeypatch.setattr(h, "_read_state", lambda: dict(storage))
    monkeypatch.setattr(h, "_write_state", lambda s: storage.update(s))
    monkeypatch.setattr(h, "_send", lambda issues, metrics, now: sent.append(issues) or True)

    r1 = h.alert_if_unhealthy(NOW)
    assert r1["alerted"] is True and sent[-1] == []   # recovery announced

    r2 = h.alert_if_unhealthy(NOW + timedelta(hours=1))
    assert r2["alerted"] is False and len(sent) == 1  # no repeat for quiet-all-clear


def test_alert_flow_never_raises(monkeypatch):
    """A failing check must not take down the forecast run it rides along with."""
    import src.forecasting.models.health as h

    def boom(*a, **k):
        raise RuntimeError("s3 down")

    monkeypatch.setattr(h, "gather", boom)
    monkeypatch.setattr(h, "_read_state", boom)
    r = h.alert_if_unhealthy(NOW)
    assert r.get("alerted") is False
    assert "error" in r

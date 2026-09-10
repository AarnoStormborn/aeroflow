"""Unit tests for the forecasting service.

Covers the pure-logic pieces that don't need live S3/MLflow:
- feature vector construction (lag/rolling/calendar semantics)
- quality-report aggregation and rendering
"""

from datetime import datetime, timezone

import polars as pl
from src.forecasting.data.loader import build_feature_vector
from src.forecasting.models.quality_report import (
    aggregate_by_model_horizon,
    render_graph,
)


def _hourly_df():
    """72h of hourly counts: 2026-09-05 00:00 .. 2026-09-07 23:00 UTC.

    Count = 50 + (hour_index mod 24), so hour 00:00 -> 50 .. 23:00 -> 73,
    repeating daily. Covers Fri 9/5, Sat 9/6, Sun 9/7 for calendar tests.
    """
    from datetime import timedelta

    base = datetime(2026, 9, 5, 0, 0, tzinfo=timezone.utc)
    rows = [(base + timedelta(hours=i), float(50 + (i % 24))) for i in range(72)]
    return pl.DataFrame(rows, schema=["hour_start", "flight_count"], orient="row")


def test_build_feature_vector_basic():
    """Feature vector uses actual lags and correct calendar semantics."""
    df = _hourly_df()
    # Target 2026-09-07 12:00 — Sep 7 2026 is a Monday (isoweekday 1)
    target = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)
    feats = build_feature_vector(df, target)

    assert feats[0] == 12.0  # hour_of_day
    assert feats[1] == 1.0  # day_of_week (Mon)
    assert feats[2] == 0.0  # is_weekend (Mon -> not weekend)
    # lag_1h: count at 11:00 = 50 + (11 % 24) = 61
    assert feats[3] == 61.0
    # lag_24h: count at 12:00 prev day (Sep 6) = 50 + (12 % 24) = 62
    assert feats[4] == 62.0
    # rolling_mean_6h: mean of 11:00..06:00 counts = 50+[11,10,9,8,7,6]/6
    expected_roll = sum(50 + (i % 24) for i in range(6, 12)) / 6
    assert abs(feats[5] - expected_roll) < 1e-6


def test_build_feature_vector_weekend():
    """Saturday/Sunday target is flagged weekend (polars weekday>=5 -> Fri-Sun)."""
    df = _hourly_df()
    # Sep 6 2026 is a Sunday; in range with lag_24h (Sep 5) available
    target = datetime(2026, 9, 6, 8, 0, tzinfo=timezone.utc)
    feats = build_feature_vector(df, target)
    assert feats[1] == 7.0  # day_of_week: Sunday = isoweekday 7
    assert feats[2] == 1.0  # is_weekend


def test_build_feature_vector_insufficient_history():
    """Raises when not enough history for lags."""
    df = _hourly_df().head(5)  # only 5 hours
    target = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)
    try:
        build_feature_vector(df, target)
        raise AssertionError("should have raised")
    except ValueError:
        pass


def test_recursive_prediction_uses_predicted_counts():
    """build_feature_vector falls back to predicted counts for future hours."""
    df = _hourly_df()
    # Target beyond actuals (say Sep 9 00:00, after our 48h df ends Sep 7 23:00)
    from datetime import timedelta

    target = df["hour_start"][-1] + timedelta(hours=1)  # first missing hour
    predicted = {target: 77.0}
    # lag_1h is df[-1]; but rolling needs 6h before target - 5 are actual, 1 (target-1h?) no.
    # Provide predicted for the 5 missing rolling hours too
    for i in range(1, 6):
        predicted[target - timedelta(hours=i)] = 60.0
    # But those override actuals... simpler: use a far-future target where
    # only predicted counts exist
    far = target + timedelta(days=3)
    for i in range(1, 25):
        predicted[far - timedelta(hours=i)] = 55.0
    feats = build_feature_vector(df, far, predicted_counts=predicted)
    assert feats[3] == 55.0  # lag_1h from predicted
    assert feats[4] == 55.0  # lag_24h from predicted


def test_quality_aggregation():
    """Aggregates per-model per-horizon MAPE across evals."""
    evals = [
        {
            "models": {
                "m1": {"per_horizon_mean_mape": {"1": 5.0, "2": 6.0}},
                "m2": {"per_horizon_mean_mape": {"1": 8.0}},
            }
        },
        {
            "models": {
                "m1": {"per_horizon_mean_mape": {"1": 7.0, "2": 8.0}},
            }
        },
    ]
    agg = aggregate_by_model_horizon(evals)
    assert agg["m1"]["1"]["mean_mape"] == 6.0  # avg of 5,7
    assert agg["m1"]["1"]["n"] == 2
    assert agg["m2"]["1"]["mean_mape"] == 8.0


def test_render_graph_png():
    """Graph renders a valid PNG."""
    agg = {
        "m1": {"1": {"mean_mape": 5.0, "n": 2}, "2": {"mean_mape": 7.0, "n": 2}},
        "m2": {"1": {"mean_mape": 8.0, "n": 2}, "2": {"mean_mape": 10.0, "n": 2}},
    }
    png = render_graph(agg)
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert len(png) > 1000

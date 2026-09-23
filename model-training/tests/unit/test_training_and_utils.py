"""Unit tests for model training logic, evaluation metrics, and plotting."""

import random

import numpy as np
import pytest
from sklearn.linear_model import LinearRegression
from src.training.utils import (
    plot_feature_importance,
    plot_forecast_with_ci,
    plot_predictions,
    plot_residuals,
)


def test_linear_regression_fit_and_predict():
    """Test model fitting on synthetic time-series feature matrix."""
    np.random.seed(42)

    # 50 samples, 6 features: [hour_of_day, day_of_week, is_weekend, lag_1h, lag_24h, rolling_mean_6h]
    X = np.random.rand(50, 6) * 10
    true_coeffs = np.array([1.5, 0.5, 2.0, 0.8, 0.3, 0.4])
    y = X @ true_coeffs + 5.0 + np.random.normal(0, 0.1, 50)

    model = LinearRegression()
    model.fit(X, y)

    preds = model.predict(X)
    assert len(preds) == 50

    # R2 should be high on synthetic linear data
    r2 = model.score(X, y)
    assert r2 > 0.95


def test_plot_predictions():
    """Test generating prediction scatter plot figure."""
    y_true = np.array([10.0, 20.0, 30.0, 40.0])
    y_pred = np.array([11.0, 19.5, 30.5, 39.0])

    fig = plot_predictions(y_true, y_pred, title="Test Predictions")
    assert fig is not None
    assert len(fig.axes) == 1
    ax = fig.axes[0]
    assert ax.get_title() == "Test Predictions"


def test_plot_residuals():
    """Test generating residual distribution and scatter plot figures."""
    y_true = np.array([10.0, 20.0, 30.0, 40.0])
    y_pred = np.array([12.0, 19.0, 31.0, 38.0])

    fig = plot_residuals(y_true, y_pred, title="Test Residuals")
    assert fig is not None
    assert len(fig.axes) == 2  # Histogram + Residual vs Pred scatter


def test_plot_feature_importance():
    """Test plotting feature coefficients."""
    feature_names = ["feat1", "feat2", "feat3"]
    coefficients = np.array([0.5, -1.2, 0.8])

    fig = plot_feature_importance(feature_names, coefficients, title="Test Feature Importance")
    assert fig is not None
    assert len(fig.axes) == 1


def test_plot_forecast_with_ci():
    """Test confidence interval forecast visualization."""
    y_true = np.linspace(10, 20, 10)
    y_pred = y_true + np.random.normal(0, 0.5, 10)

    fig = plot_forecast_with_ci(y_true, y_pred, mape=0.106, title="Test Forecast CI")
    assert fig is not None
    assert len(fig.axes) == 1


# ---- champion guard ------------------------------------------------------
# train_production retrains every 3 days and used to promote unconditionally.
# A backtest showed retrains scoring WORSE than the incumbent, so these pin the
# decision that stops a regression from being served.
#
# The guard now compares the two models hour by hour with a paired bootstrap
# instead of comparing two aggregate MAPEs. The reason is the 2026-09-22 run:
# it scored 24.34 against the incumbent's 24.33 and was declined for failing to
# beat it by 5%, a decision resting on 0.01 of a MAPE point on a ~75-hour block.

from src.training.train_production import (  # noqa: E402
    MIN_IMPROVEMENT,
    _pct_errors,
    decide_promotion,
    paired_improvement,
)


def test_decides_on_a_clear_win():
    new = [10.0] * 40
    incumbent = [20.0] * 40
    promote, reason = decide_promotion(paired_improvement(new, incumbent))
    assert promote is True
    assert "better" in reason


def test_keeps_incumbent_when_clearly_worse():
    new = [25.0] * 40
    incumbent = [20.0] * 40
    promote, reason = decide_promotion(paired_improvement(new, incumbent))
    assert promote is False
    assert "no measurable improvement" in reason


def test_the_v9_case_is_reported_as_indistinguishable_not_worse():
    """The real incident, in miniature.

    24.34 against 24.33 must read as "could not tell them apart", because that
    is what the data said. Calling it a loss is what made the decision
    indefensible.
    """
    rng = random.Random(7)
    incumbent = [24.33 + rng.uniform(-8, 8) for _ in range(75)]
    scale = 24.34 / (sum(incumbent) / len(incumbent))
    # Same shape, nudged so the mean lands on 24.34
    candidate = [e * scale for e in incumbent]

    stats = paired_improvement(candidate, incumbent)
    promote, reason = decide_promotion(stats)

    assert promote is False
    assert "no measurable improvement" in reason
    assert "worse" not in reason


def test_no_improvement_is_not_promoted():
    promote, reason = decide_promotion(paired_improvement([20.0] * 40, [20.0] * 40))
    assert promote is False
    assert "no measurable improvement" in reason


def test_a_win_below_the_floor_is_named_as_such():
    """Real but trivial gains must not churn the registry."""
    incumbent = [20.0] * 60
    new = [20.0 * (1 - MIN_IMPROVEMENT / 2)] * 60
    promote, reason = decide_promotion(paired_improvement(new, incumbent))
    assert promote is False
    assert "floor" in reason


def test_a_noisy_edge_does_not_promote():
    """A point-estimate win whose interval spans zero is not a win."""
    rng = random.Random(3)
    incumbent = [20.0 + rng.uniform(-15, 15) for _ in range(60)]
    candidate = [e * 0.95 for e in incumbent]  # consistently 5% better
    candidate[::2] = [e * 1.30 for e in incumbent[::2]]  # ... except half of them

    stats = paired_improvement(candidate, incumbent)
    promote, _ = decide_promotion(stats)
    assert stats["lower"] is not None
    assert promote is False


def test_unscorable_comparison_is_not_promoted():
    promote, reason = decide_promotion(paired_improvement([], []))
    assert promote is False
    assert "could not be scored" in reason


def test_paired_improvement_pairs_rather_than_resamples_freely():
    """Both models see the same hours, so a shared hard hour cancels out."""
    incumbent = [5.0, 5.0, 5.0, 5.0, 90.0]  # one disastrous hour
    candidate = [5.0, 5.0, 5.0, 5.0, 90.0]  # identical on that hour
    stats = paired_improvement(candidate, incumbent)
    assert stats["improvement"] == pytest.approx(0.0)
    assert stats["n"] == 5


def test_paired_improvement_reports_an_interval():
    stats = paired_improvement([10.0] * 30, [20.0] * 30)
    assert stats["lower"] is not None and stats["upper"] is not None
    assert stats["lower"] > 0
    assert stats["lower"] <= stats["improvement"] <= stats["upper"]


def test_paired_improvement_handles_a_zero_baseline():
    assert paired_improvement([1.0, 2.0], [0.0, 0.0])["improvement"] is None


def test_paired_improvement_truncates_to_the_shorter_series():
    stats = paired_improvement([10.0] * 30, [20.0] * 10)
    assert stats["n"] == 10


def test_pct_errors_skips_hours_with_no_traffic():
    """A zero actual makes the percentage undefined, so that hour drops out."""
    errors = _pct_errors([0.0, 10.0, 20.0], [5.0, 12.0, 18.0])
    assert len(errors) == 2
    assert errors[0] == pytest.approx(20.0)
    assert errors[1] == pytest.approx(10.0)


def test_pct_errors_is_empty_when_nothing_is_scoreable():
    assert _pct_errors([0.0], [1.0]) == []

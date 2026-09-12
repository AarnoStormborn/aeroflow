"""Unit tests for model training logic, evaluation metrics, and plotting."""

import numpy as np
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

from src.training.train_production import MIN_IMPROVEMENT, should_promote  # noqa: E402


def test_promotes_when_no_incumbent():
    """Fresh registry: nothing to beat, so bootstrap."""
    assert should_promote(42.0, None) is True


def test_promotes_on_clear_improvement():
    assert should_promote(10.0, 20.0) is True


def test_keeps_incumbent_when_worse():
    assert should_promote(25.0, 20.0) is False


def test_keeps_incumbent_when_equal():
    assert should_promote(20.0, 20.0) is False


def test_requires_a_margin_not_just_a_win():
    """A marginal win is noise on this dataset and must not cause churn."""
    just_inside = 20.0 * (1 - MIN_IMPROVEMENT) + 0.01   # 1pp shy of the margin
    just_outside = 20.0 * (1 - MIN_IMPROVEMENT) - 0.01  # just past it
    assert should_promote(just_inside, 20.0) is False
    assert should_promote(just_outside, 20.0) is True


def test_never_promotes_a_nan_score():
    nan = float("nan")
    assert should_promote(nan, 20.0) is False
    assert should_promote(10.0, 20.0) is True  # sanity: guard only blocks NaN new

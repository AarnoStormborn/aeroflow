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

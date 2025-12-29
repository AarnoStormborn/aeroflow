"""
Plotting utilities for MLflow logging.
"""

from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


def plot_predictions(y_true, y_pred, title: str = "Predictions vs Actual") -> plt.Figure:
    """
    Create scatter plot of predictions vs actual values.
    
    Args:
        y_true: Actual values
        y_pred: Predicted values
        title: Plot title
        
    Returns:
        Matplotlib figure
    """
    fig, ax = plt.subplots(figsize=(8, 6))
    
    ax.scatter(y_true, y_pred, alpha=0.6, edgecolors='black', linewidth=0.5)
    
    # Perfect prediction line
    min_val = min(min(y_true), min(y_pred))
    max_val = max(max(y_true), max(y_pred))
    ax.plot([min_val, max_val], [min_val, max_val], 'r--', label='Perfect Prediction')
    
    ax.set_xlabel("Actual Flight Count")
    ax.set_ylabel("Predicted Flight Count")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig


def plot_residuals(y_true, y_pred, title: str = "Residual Distribution") -> plt.Figure:
    """
    Create residual distribution plot.
    
    Args:
        y_true: Actual values
        y_pred: Predicted values
        title: Plot title
        
    Returns:
        Matplotlib figure
    """
    residuals = y_true - y_pred
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    # Histogram
    axes[0].hist(residuals, bins=20, edgecolor='black', alpha=0.7)
    axes[0].axvline(0, color='r', linestyle='--', label='Zero')
    axes[0].set_xlabel("Residual (Actual - Predicted)")
    axes[0].set_ylabel("Frequency")
    axes[0].set_title("Residual Distribution")
    axes[0].legend()
    
    # Residuals vs Predicted
    axes[1].scatter(y_pred, residuals, alpha=0.6, edgecolors='black', linewidth=0.5)
    axes[1].axhline(0, color='r', linestyle='--')
    axes[1].set_xlabel("Predicted Flight Count")
    axes[1].set_ylabel("Residual")
    axes[1].set_title("Residuals vs Predictions")
    
    plt.suptitle(title)
    plt.tight_layout()
    return fig


def plot_feature_importance(feature_names: list, coefficients: np.ndarray, title: str = "Feature Coefficients") -> plt.Figure:
    """
    Plot feature importance/coefficients.
    
    Args:
        feature_names: List of feature names
        coefficients: Model coefficients
        title: Plot title
        
    Returns:
        Matplotlib figure
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Sort by absolute value
    sorted_idx = np.argsort(np.abs(coefficients))[::-1]
    
    colors = ['green' if c >= 0 else 'red' for c in coefficients[sorted_idx]]
    
    ax.barh([feature_names[i] for i in sorted_idx], coefficients[sorted_idx], color=colors)
    ax.axvline(0, color='black', linewidth=0.5)
    ax.set_xlabel("Coefficient Value")
    ax.set_title(title)
    ax.grid(True, alpha=0.3, axis='x')
    
    plt.tight_layout()
    return fig


def plot_forecast_with_ci(
    y_true: np.ndarray,
    y_pred: np.ndarray, 
    mape: float,
    title: str = "Forecast",
    xlabel: str = "Time Index (Hours)",
) -> plt.Figure:
    """
    Plot forecast time series with ±MAPE confidence interval.
    
    Args:
        y_true: Actual values
        y_pred: Predicted values
        mape: Mean Absolute Percentage Error (as decimal, e.g., 0.106 for 10.6%)
        title: Plot title
        xlabel: X-axis label
        
    Returns:
        Matplotlib figure
    """
    fig, ax = plt.subplots(figsize=(14, 6))
    
    x = np.arange(len(y_true))
    
    # Calculate confidence interval (±MAPE)
    ci_lower = y_pred * (1 - mape)
    ci_upper = y_pred * (1 + mape)
    
    # Plot confidence interval band
    ax.fill_between(
        x, ci_lower, ci_upper, 
        alpha=0.3, color='blue', 
        label=f'±{mape*100:.1f}% Confidence Interval'
    )
    
    # Plot actual values
    ax.plot(x, y_true, 'o-', color='green', linewidth=2, markersize=6, 
            label='Actual', alpha=0.8)
    
    # Plot predictions
    ax.plot(x, y_pred, 's--', color='red', linewidth=2, markersize=5,
            label='Predicted', alpha=0.8)
    
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Flight Count")
    ax.set_title(title)
    ax.legend(loc='upper right')
    ax.grid(True, alpha=0.3)
    
    # Add MAPE annotation
    ax.annotate(
        f'MAPE: {mape*100:.2f}%',
        xy=(0.02, 0.98), xycoords='axes fraction',
        fontsize=12, fontweight='bold',
        verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='white', alpha=0.8)
    )
    
    plt.tight_layout()
    return fig


def plot_train_test_forecast(
    y_train: np.ndarray,
    y_train_pred: np.ndarray,
    y_test: np.ndarray,
    y_test_pred: np.ndarray,
    train_mape: float,
    test_mape: float,
    title: str = "Train & Test Forecast",
) -> plt.Figure:
    """
    Plot combined train and test forecast with confidence intervals.
    
    Args:
        y_train: Training actual values
        y_train_pred: Training predictions
        y_test: Test actual values
        y_test_pred: Test predictions
        train_mape: Training MAPE (decimal)
        test_mape: Test MAPE (decimal)
        title: Plot title
        
    Returns:
        Matplotlib figure
    """
    fig, axes = plt.subplots(2, 1, figsize=(14, 10))
    
    # --- Training Data ---
    ax1 = axes[0]
    x_train = np.arange(len(y_train))
    
    ci_lower = y_train_pred * (1 - train_mape)
    ci_upper = y_train_pred * (1 + train_mape)
    
    ax1.fill_between(x_train, ci_lower, ci_upper, alpha=0.3, color='blue',
                     label=f'±{train_mape*100:.1f}% CI')
    ax1.plot(x_train, y_train, 'o-', color='green', linewidth=1.5, markersize=4,
             label='Actual', alpha=0.8)
    ax1.plot(x_train, y_train_pred, 's--', color='red', linewidth=1.5, markersize=3,
             label='Predicted', alpha=0.8)
    
    ax1.set_xlabel("Hour Index")
    ax1.set_ylabel("Flight Count")
    ax1.set_title(f"Training Data (MAPE: {train_mape*100:.2f}%)")
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)
    
    # --- Test Data ---
    ax2 = axes[1]
    x_test = np.arange(len(y_test))
    
    ci_lower = y_test_pred * (1 - test_mape)
    ci_upper = y_test_pred * (1 + test_mape)
    
    ax2.fill_between(x_test, ci_lower, ci_upper, alpha=0.3, color='orange',
                     label=f'±{test_mape*100:.1f}% CI')
    ax2.plot(x_test, y_test, 'o-', color='green', linewidth=2, markersize=6,
             label='Actual', alpha=0.8)
    ax2.plot(x_test, y_test_pred, 's--', color='red', linewidth=2, markersize=5,
             label='Predicted', alpha=0.8)
    
    ax2.set_xlabel("Hour Index")
    ax2.set_ylabel("Flight Count")
    ax2.set_title(f"Test Data (MAPE: {test_mape*100:.2f}%)")
    ax2.legend(loc='upper right')
    ax2.grid(True, alpha=0.3)
    
    plt.suptitle(title, fontsize=14, fontweight='bold')
    plt.tight_layout()
    return fig


__all__ = [
    "plot_predictions", 
    "plot_residuals", 
    "plot_feature_importance",
    "plot_forecast_with_ci",
    "plot_train_test_forecast",
]


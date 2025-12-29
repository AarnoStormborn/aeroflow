"""
Model training pipeline with MLflow.

Trains Linear Regression model using 14-day rolling window of features.
Logs all metrics, parameters, plots, and model to MLflow.

Usage:
    uv run python -m src.training.train
    uv run python -m src.training.train --end-date 2025-12-28
"""

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
import tempfile
import os

import numpy as np
import matplotlib.pyplot as plt
import polars as pl
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score
import mlflow
import mlflow.sklearn
from loguru import logger

from src.training.config import settings
from src.training.data_loader import create_loader
from src.training.utils import (
    plot_predictions, 
    plot_residuals, 
    plot_feature_importance,
    plot_forecast_with_ci,
    plot_train_test_forecast,
)


def setup_mlflow():
    """Configure MLflow tracking."""
    mlflow.set_tracking_uri(settings.mlflow.tracking_uri)
    
    # Create or get experiment
    experiment = mlflow.get_experiment_by_name(settings.mlflow.experiment_name)
    if experiment is None:
        experiment_id = mlflow.create_experiment(
            settings.mlflow.experiment_name,
            artifact_location=settings.mlflow.artifact_root,
        )
    else:
        experiment_id = experiment.experiment_id
    
    mlflow.set_experiment(settings.mlflow.experiment_name)
    logger.info(f"MLflow experiment: {settings.mlflow.experiment_name}")
    
    return experiment_id


def train_model(end_date: date) -> dict:
    """
    Train model with rolling window data and log to MLflow.
    
    Args:
        end_date: Last day of training window
        
    Returns:
        Dictionary with run info and metrics
    """
    print("=" * 60)
    print("MODEL TRAINING PIPELINE")
    print(f"End Date: {end_date}")
    print(f"Window: {settings.training.window_days} days")
    print("=" * 60)
    
    # Setup MLflow
    experiment_id = setup_mlflow()
    
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        logger.info(f"MLflow run: {run_id}")
        
        # 1. Log parameters
        mlflow.log_params({
            "window_days": settings.training.window_days,
            "test_ratio": settings.training.test_ratio,
            "end_date": str(end_date),
            "model_type": "LinearRegression",
            "features": ",".join(settings.training.feature_columns),
            "target": settings.training.target_column,
        })
        
        # 2. Load data
        logger.info("Step 1: Loading feature data...")
        loader = create_loader()
        df = loader.load_rolling_window(end_date, settings.training.window_days)
        
        mlflow.log_param("samples_total", len(df))
        
        # 3. Prepare features
        logger.info("Step 2: Preparing features...")
        X = df.select(settings.training.feature_columns).to_numpy()
        y = df.select(settings.training.target_column).to_numpy().flatten()
        
        # 4. Time-based train/test split
        split_idx = int(len(X) * (1 - settings.training.test_ratio))
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]
        
        mlflow.log_params({
            "samples_train": len(X_train),
            "samples_test": len(X_test),
        })
        
        print(f"\nData: {len(df)} samples (train: {len(X_train)}, test: {len(X_test)})")
        
        # 5. Train model
        logger.info("Step 3: Training model...")
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        # 6. Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # 7. Compute metrics
        logger.info("Step 4: Computing metrics...")
        
        train_mae = mean_absolute_error(y_train, y_train_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        train_mape = mean_absolute_percentage_error(y_train, y_train_pred) * 100
        test_mape = mean_absolute_percentage_error(y_test, y_test_pred) * 100
        train_r2 = r2_score(y_train, y_train_pred)
        test_r2 = r2_score(y_test, y_test_pred)
        
        metrics = {
            "train_mae": train_mae,
            "test_mae": test_mae,
            "train_mape": train_mape,
            "test_mape": test_mape,
            "train_r2": train_r2,
            "test_r2": test_r2,
        }
        
        mlflow.log_metrics(metrics)
        
        print(f"\nMetrics:")
        print(f"  Train MAE: {train_mae:.2f}, Test MAE: {test_mae:.2f}")
        print(f"  Train MAPE: {train_mape:.2f}%, Test MAPE: {test_mape:.2f}%")
        print(f"  Train R²: {train_r2:.3f}, Test R²: {test_r2:.3f}")
        
        # 8. Log coefficients
        for i, name in enumerate(settings.training.feature_columns):
            mlflow.log_metric(f"coef_{name}", model.coef_[i])
        mlflow.log_metric("intercept", model.intercept_)
        
        # 9. Create and log plots
        logger.info("Step 5: Creating plots...")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Forecast plot with confidence intervals (train + test)
            fig = plot_train_test_forecast(
                y_train, y_train_pred,
                y_test, y_test_pred,
                train_mape / 100,  # Convert to decimal
                test_mape / 100,
                title="Flight Traffic Forecast with ±MAPE Confidence Interval"
            )
            fig.savefig(f"{tmpdir}/forecast_train_test.png", dpi=150)
            mlflow.log_artifact(f"{tmpdir}/forecast_train_test.png", "plots")
            plt.close(fig)
            
            # Test forecast only
            fig = plot_forecast_with_ci(
                y_test, y_test_pred,
                test_mape / 100,
                title="Test Set Forecast with ±MAPE Confidence Interval"
            )
            fig.savefig(f"{tmpdir}/forecast_test.png", dpi=150)
            mlflow.log_artifact(f"{tmpdir}/forecast_test.png", "plots")
            plt.close(fig)
            
            # Predictions scatter plot
            fig = plot_predictions(y_test, y_test_pred, "Test Set: Predictions vs Actual")
            fig.savefig(f"{tmpdir}/predictions.png", dpi=150)
            mlflow.log_artifact(f"{tmpdir}/predictions.png", "plots")
            plt.close(fig)
            
            # Residuals plot
            fig = plot_residuals(y_test, y_test_pred, "Test Set: Residual Analysis")
            fig.savefig(f"{tmpdir}/residuals.png", dpi=150)
            mlflow.log_artifact(f"{tmpdir}/residuals.png", "plots")
            plt.close(fig)
            
            # Feature importance
            fig = plot_feature_importance(
                settings.training.feature_columns,
                model.coef_,
                "Linear Regression Coefficients"
            )
            fig.savefig(f"{tmpdir}/feature_importance.png", dpi=150)
            mlflow.log_artifact(f"{tmpdir}/feature_importance.png", "plots")
            plt.close(fig)
        
        # 10. Log model
        logger.info("Step 6: Logging model...")
        mlflow.sklearn.log_model(
            model,
            "model",
            registered_model_name="flight-traffic-forecaster",
        )
        
        print("=" * 60)
        print("TRAINING COMPLETE")
        print(f"Run ID: {run_id}")
        print(f"Test MAE: {test_mae:.2f} flights")
        print(f"Test MAPE: {test_mape:.2f}%")
        print("=" * 60)
        
        return {
            "run_id": run_id,
            "experiment_id": experiment_id,
            "metrics": metrics,
        }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Train flight traffic forecasting model")
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date of training window (YYYY-MM-DD). Defaults to yesterday.",
    )
    
    args = parser.parse_args()
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        end_date = date.today() - timedelta(days=1)
    
    train_model(end_date)


if __name__ == "__main__":
    main()

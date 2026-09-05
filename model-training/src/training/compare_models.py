"""
Model comparison — Linear vs XGBoost vs Polynomial on historical data.

Trains multiple model types on the same train/test split (time-ordered)
and logs each run to MLflow with MAPE metrics, optimized for low test MAPE.

Usage:
    uv run python -m src.training.compare_models --end-date 2026-01-13
"""

import argparse
import math
import os
from datetime import date, datetime

# Must set backend before importing pyplot to avoid IPython dependency
os.environ.setdefault("MPLBACKEND", "Agg")
import matplotlib

matplotlib.use("Agg")

# IPython 9.x removed module-level get_ipython/version_info; matplotlib's
# repl display hook needs them on figure creation. Shim both (headless-safe).
try:
    import IPython
    if not hasattr(IPython, "get_ipython"):
        IPython.get_ipython = lambda: None
    if not hasattr(IPython, "version_info"):
        IPython.version_info = (8, 24, 0)  # satisfies matplotlib's check
except ImportError:
    pass

import matplotlib.pyplot as plt
import mlflow
import numpy as np
import polars as pl
import xgboost as xgb
from loguru import logger
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import PolynomialFeatures, StandardScaler
from src.training.config import settings
from src.training.data_loader import create_loader

# MLflow tracking — point at Modal server (env set), fall back to local
TRACKING_URI = settings.mlflow.tracking_uri  # from env or local sqlite
EXPERIMENT = "flight-traffic-forecasting"


def log_metrics_safe(metrics: dict) -> None:
    """Log metrics, filtering NaN values (SQLite backend errors on NaN)."""
    loggable = {k: v for k, v in metrics.items()
                if not (isinstance(v, float) and math.isnan(v))}
    mlflow.log_metrics(loggable)


def train_and_log(model, model_name: str, X_train, X_test, y_train, y_test,
                  end_date: date, extra_params: dict | None = None) -> dict:
    """Train a model, log run to MLflow, return metrics."""
    with mlflow.start_run(run_name=model_name) as run:
        run_id = run.info.run_id

        # Params
        params = {
            "model_type": model_name,
            "end_date": str(end_date),
            "window_days": settings.training.window_days,
            "features": ",".join(settings.training.feature_columns),
            "target": settings.training.target_column,
            "samples_total": len(X_train) + len(X_test),
            "samples_train": len(X_train),
            "samples_test": len(X_test),
        }
        if extra_params:
            params.update(extra_params)
        mlflow.log_params(params)

        # Train
        model.fit(X_train, y_train)

        # Predict
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)

        # Metrics
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
        log_metrics_safe(metrics)

        # Plot: actual vs predicted with CI (±MAPE band) for the test set
        fig, ax = plt.subplots(figsize=(12, 5))
        x = np.arange(len(y_test))
        ci_lower = y_test_pred * (1 - test_mape / 100)
        ci_upper = y_test_pred * (1 + test_mape / 100)
        ax.fill_between(x, ci_lower, ci_upper, alpha=0.25, color="blue",
                        label=f"±{test_mape:.1f}% CI")
        ax.plot(x, y_test, "o-", color="green", lw=1.5, ms=4, label="Actual")
        ax.plot(x, y_test_pred, "s--", color="red", lw=1.5, ms=4, label="Predicted")
        ax.set_xlabel("Hour index (test)")
        ax.set_ylabel("Flight count")
        ax.set_title(f"{model_name} — Test (MAPE {test_mape:.1f}%)")
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        mlflow.log_figure(fig, f"plots/{model_name}_forecast_ci.png")
        plt.close(fig)

        # Log the model
        try:
            if model_name == "XGBoost":
                # xgboost types need explicit trust for skops serialization
                mlflow.sklearn.log_model(
                    model,
                    f"model_{model_name.replace(' ', '_').lower()}",
                    skops_trusted_types=("xgboost.core.Booster", "xgboost.sklearn.XGBRegressor"),
                )
            else:
                mlflow.sklearn.log_model(model, f"model_{model_name.replace(' ', '_').lower()}")
        except Exception as e:
            logger.warning(f"Model log failed for {model_name}: {e}")

        print(f"[{model_name}] Test MAPE: {test_mape:.2f}% | Test MAE: {test_mae:.2f} | "
              f"Train MAPE: {train_mape:.2f}% | R²: {test_r2:.3f}")
        return {"run_id": run_id, "model_name": model_name, "metrics": metrics}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default="2026-01-13",
                        help="Last day of training window (old data block)")
    parser.add_argument("--test-days", type=int, default=4,
                        help="Number of days to hold out as test")
    args = parser.parse_args()

    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    # Load data (rolling window ending at end_date)
    loader = create_loader()
    df = loader.load_rolling_window(end_date, settings.training.window_days)
    df = df.sort("hour_start")

    # Time-based split: hold out the last N *full days* as test
    df = df.with_columns(pl.col("hour_start").dt.date().alias("day"))
    unique_days = df.select("day").unique().sort("day")
    n_days = unique_days.height
    test_n = min(args.test_days, max(1, n_days - 7))  # keep >=7 days train
    split_day = unique_days["day"][n_days - test_n]

    train_df = df.filter(df["day"] < split_day)
    test_df = df.filter(df["day"] >= split_day)

    X_train = train_df.select(settings.training.feature_columns).to_numpy()
    y_train = train_df.select(settings.training.target_column).to_numpy().flatten()
    X_test = test_df.select(settings.training.feature_columns).to_numpy()
    y_test = test_df.select(settings.training.target_column).to_numpy().flatten()

    print(f"Window: {len(df)} samples over {n_days} days")
    print(f"Split: train {len(X_train)} ({n_days - test_n}d) / test {len(X_test)} ({test_n}d)")
    print(f"Test period starts: {split_day}")

    # MLflow setup
    mlflow.set_tracking_uri(TRACKING_URI)
    exp = mlflow.get_experiment_by_name(EXPERIMENT)
    if exp is None:
        mlflow.create_experiment(EXPERIMENT, artifact_location=settings.mlflow.artifact_root)
    mlflow.set_experiment(EXPERIMENT)

    # Compare models
    models = {
        "LinearRegression": (LinearRegression(), {}),
        "Ridge": (Ridge(alpha=1.0), {}),
        "Poly2+Linear": (
            make_pipeline(PolynomialFeatures(degree=2), StandardScaler(), LinearRegression()),
            {"degree": 2},
        ),
        "XGBoost": (
            xgb.XGBRegressor(
                n_estimators=200, max_depth=4, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8, random_state=42,
            ),
            {"n_estimators": 200, "max_depth": 4, "learning_rate": 0.05},
        ),
    }

    results = {}
    for name, (model, extra) in models.items():
        results[name] = train_and_log(model, name, X_train, X_test, y_train, y_test,
                                      end_date, extra)

    # Summary
    print("\n" + "=" * 50)
    print("MODEL COMPARISON SUMMARY (sorted by test MAPE)")
    print("=" * 50)
    sorted_results = sorted(results.values(), key=lambda r: r["metrics"]["test_mape"])
    for r in sorted_results:
        m = r["metrics"]
        print(f"  {r['model_name']:<18} test MAPE {m['test_mape']:6.2f}%  "
              f"test MAE {m['test_mae']:6.2f}  test R² {m['test_r2']:.3f}")


if __name__ == "__main__":
    main()

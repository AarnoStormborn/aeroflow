"""
XGBoost hyperparameter tuning for flight traffic forecasting.

Grid-searches XGBoost params on the same time-based split as
compare_models.py, logging each trial to MLflow. Reports the config
with the lowest test MAPE.

Usage:
    uv run python -m src.training.tune_xgboost --end-date 2026-01-13
"""

import argparse
import itertools
import math
import os
from datetime import date, datetime

# Headless matplotlib before pyplot import
os.environ.setdefault("MPLBACKEND", "Agg")
import matplotlib

matplotlib.use("Agg")
try:
    import IPython
    if not hasattr(IPython, "get_ipython"):
        IPython.get_ipython = lambda: None
    if not hasattr(IPython, "version_info"):
        IPython.version_info = (8, 24, 0)
except ImportError:
    pass
import mlflow
import polars as pl
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score
from src.training.config import settings
from src.training.data_loader import create_loader

EXPERIMENT = "flight-traffic-forecasting"
TRACKING_URI = settings.mlflow.tracking_uri


def log_metrics_safe(metrics: dict) -> None:
    loggable = {k: v for k, v in metrics.items()
                if not (isinstance(v, float) and math.isnan(v))}
    mlflow.log_metrics(loggable)


def load_split(end_date: date, test_days: int = 3):
    """Load rolling window and do a time-based day split."""
    loader = create_loader()
    df = loader.load_rolling_window(end_date, settings.training.window_days)
    df = df.sort("hour_start")
    df = df.with_columns(pl.col("hour_start").dt.date().alias("day"))
    unique_days = df.select("day").unique().sort("day")
    n_days = unique_days.height
    test_n = min(test_days, max(1, n_days - 7))
    split_day = unique_days["day"][n_days - test_n]

    train_df = df.filter(df["day"] < split_day)
    test_df = df.filter(df["day"] >= split_day)

    X_train = train_df.select(settings.training.feature_columns).to_numpy()
    y_train = train_df.select(settings.training.target_column).to_numpy().flatten()
    X_test = test_df.select(settings.training.feature_columns).to_numpy()
    y_test = test_df.select(settings.training.target_column).to_numpy().flatten()
    return X_train, X_test, y_train, y_test, n_days, test_n


def evaluate_model(model, X_train, X_test, y_train, y_test, params: dict) -> dict:
    """Train and evaluate, logging to MLflow. Returns metrics."""
    with mlflow.start_run(run_name=(f"xgb-{params.get('max_depth','?')}d-"
                                      f"{params.get('n_estimators','?')}t-"
                                      f"{params.get('learning_rate','?')}lr")):
        run_id = mlflow.active_run().info.run_id
        mlflow.log_params(params)

        model.fit(X_train, y_train)
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)

        train_mae = mean_absolute_error(y_train, y_train_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        train_mape = mean_absolute_percentage_error(y_train, y_train_pred) * 100
        test_mape = mean_absolute_percentage_error(y_test, y_test_pred) * 100
        train_r2 = r2_score(y_train, y_train_pred)
        test_r2 = r2_score(y_test, y_test_pred)

        metrics = {
            "train_mae": train_mae, "test_mae": test_mae,
            "train_mape": train_mape, "test_mape": test_mape,
            "train_r2": train_r2, "test_r2": test_r2,
        }
        log_metrics_safe(metrics)

        print(f"  [trial] {params} → test MAPE {test_mape:.2f}%")
        return {"run_id": run_id, "params": params, "metrics": metrics}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default="2026-01-13")
    parser.add_argument("--test-days", type=int, default=3)
    parser.add_argument("--max-trials", type=int, default=24,
                        help="Limit number of trials (grid could be large)")
    args = parser.parse_args()

    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    X_train, X_test, y_train, y_test, n_days, test_n = load_split(end_date, args.test_days)
    print(f"Data: {len(X_train) + len(X_test)} samples, "
          f"train {len(X_train)} / test {len(X_test)} "
          f"({n_days}d window, {test_n}d holdout)")

    mlflow.set_tracking_uri(TRACKING_URI)
    exp = mlflow.get_experiment_by_name(EXPERIMENT)
    if exp is None:
        mlflow.create_experiment(EXPERIMENT, artifact_location=settings.mlflow.artifact_root)
    mlflow.set_experiment(EXPERIMENT)

    # Grid definition (kept modest to bound runtime)
    grid = {
        "n_estimators": [100, 300, 600],
        "max_depth": [3, 4, 6],
        "learning_rate": [0.01, 0.05, 0.1],
        "subsample": [0.8],
        "colsample_bytree": [0.8],
        "min_child_weight": [1, 3],
    }

    keys = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))
    print(f"Full grid: {len(combos)} combos, running up to {args.max_trials}")

    results = []
    for combo in combos[: args.max_trials]:
        params = dict(zip(keys, combo, strict=True))
        params.update({
            "model_type": "XGBoost",
            "end_date": str(end_date),
            "window_days": settings.training.window_days,
            "random_state": 42,
            "objective": "reg:squarederror",
        })
        model = xgb.XGBRegressor(
            n_estimators=params["n_estimators"],
            max_depth=params["max_depth"],
            learning_rate=params["learning_rate"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            min_child_weight=params["min_child_weight"],
            random_state=42,
            objective="reg:squarederror",
        )
        res = evaluate_model(model, X_train, X_test, y_train, y_test, params)
        results.append(res)

    # Report best
    best = min(results, key=lambda r: r["metrics"]["test_mape"])
    print("\n" + "=" * 60)
    print("BEST CONFIG (lowest test MAPE):")
    print("=" * 60)
    for k, v in best["params"].items():
        print(f"  {k}: {v}")
    m = best["metrics"]
    print(f"  → test MAPE {m['test_mape']:.2f}% | test MAE {m['test_mae']:.2f} | test R² {m['test_r2']:.3f}")
    print(f"  run_id: {best['run_id']}")


if __name__ == "__main__":
    main()

"""
Train an hourly-prediction model on CURRENT (September) data and register
it as a distinct model, deployed alongside the older full-range model.

The old model was trained on Dec-Jan data (mean ~81 flights/hr) and
over-predicts the current lower-traffic regime (~40-57/hr). This model
trains only on fresh Sep data so its predictions match today's traffic
levels.

Registered model name: flight-traffic-hourly (separate from the existing
flight-traffic-forecaster, which stays available).

Usage:
    uv run python -m src.training.train_hourly_current --end-date 2026-09-06
"""

import argparse
import math
import os
from datetime import date, datetime

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
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score
from src.training.config import settings
from src.training.data_loader import create_loader

EXPERIMENT = "flight-traffic-forecasting"
MODEL_NAME = "flight-traffic-hourly"  # distinct model for hourly-on-current

# Small model — limited data, so keep it simple to avoid overfitting
PARAMS = {
    "n_estimators": 60,
    "max_depth": 3,
    "learning_rate": 0.1,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "min_child_weight": 1,
    "random_state": 42,
    "objective": "reg:squarederror",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default="2026-09-06",
                        help="Last day of fresh data window")
    args = parser.parse_args()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    # Load fresh Sep data only (>= Sep 3)
    loader = create_loader()
    df = loader.load_rolling_window(end_date, settings.training.window_days)
    fresh_start = date(2026, 9, 3)
    df = df.filter(df["hour_start"].dt.date() >= fresh_start)
    df = df.sort("hour_start")

    if len(df) < 30:
        raise ValueError(f"Only {len(df)} fresh samples — need >=30")

    X = df.select(settings.training.feature_columns).to_numpy()
    y = df.select(settings.training.target_column).to_numpy().flatten()

    # Time-based split: last 20% as validation
    split = int(len(X) * 0.8)
    X_tr, X_va = X[:split], X[split:]
    y_tr, y_va = y[:split], y[split:]

    print(f"Fresh data: {len(X)} samples (train {len(X_tr)} / val {len(X_va)})")
    print(f"Traffic: mean {y.mean():.1f} | range {y.min()}-{y.max()}")

    mlflow.set_tracking_uri(settings.mlflow.tracking_uri)
    exp = mlflow.get_experiment_by_name(EXPERIMENT)
    if exp is None:
        mlflow.create_experiment(EXPERIMENT, artifact_location=settings.mlflow.artifact_root)
    mlflow.set_experiment(EXPERIMENT)

    model = xgb.XGBRegressor(**PARAMS)
    with mlflow.start_run(run_name="hourly-current-xgb") as run:
        run_id = run.info.run_id
        mlflow.log_params({
            **PARAMS,
            "model_type": "XGBoost-hourly-current",
            "end_date": str(end_date),
            "data_source": "fresh-sep-2026",
            "samples": len(X),
        })

        model.fit(X_tr, y_tr)
        y_tr_pred = model.predict(X_tr)
        y_va_pred = model.predict(X_va)

        metrics = {
            "train_mae": mean_absolute_error(y_tr, y_tr_pred),
            "val_mae": mean_absolute_error(y_va, y_va_pred),
            "train_mape": mean_absolute_percentage_error(y_tr, y_tr_pred) * 100,
            "val_mape": mean_absolute_percentage_error(y_va, y_va_pred) * 100,
            "train_r2": r2_score(y_tr, y_tr_pred),
            "val_r2": r2_score(y_va, y_va_pred),
        }
        loggable = {k: v for k, v in metrics.items()
                    if not (isinstance(v, float) and math.isnan(v))}
        mlflow.log_metrics(loggable)

        mlflow.sklearn.log_model(
            model,
            "model",
            registered_model_name=MODEL_NAME,
            skops_trusted_types=("xgboost.core.Booster", "xgboost.sklearn.XGBRegressor"),
        )

        print(f"\nRegistered {MODEL_NAME} from run {run_id}")
        print(f"Val MAPE: {metrics['val_mape']:.2f}% | Val MAE: {metrics['val_mae']:.2f} | "
              f"Val R²: {metrics['val_r2']:.3f}")


if __name__ == "__main__":
    main()

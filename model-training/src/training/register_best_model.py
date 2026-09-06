"""
Register the best-tuned XGBoost model as the production forecasting model.

Trains XGBoost with the tuned hyperparameters (from tune_xgboost.py sweep)
on the full available feature window and registers it in MLflow as
'flight-traffic-forecaster' (new version).

Usage:
    uv run python -m src.training.register_best_model --end-date 2026-01-13
"""

import argparse
import math
import os
from datetime import datetime

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
MODEL_NAME = "flight-traffic-forecaster"

# Best config from tune_xgboost.py sweep (36 trials, old 27-day block)
BEST_PARAMS = {
    "n_estimators": 100,
    "max_depth": 4,
    "learning_rate": 0.1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 3,
    "random_state": 42,
    "objective": "reg:squarederror",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default="2026-01-13",
                        help="Last day of the training window")
    args = parser.parse_args()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    # Load ALL available features (not just a holdout — this is the prod model)
    loader = create_loader()
    df = loader.load_rolling_window(end_date, settings.training.window_days)
    df = df.sort("hour_start")
    X = df.select(settings.training.feature_columns).to_numpy()
    y = df.select(settings.training.target_column).to_numpy().flatten()
    print(f"Training on {len(X)} samples (end date {end_date})")

    mlflow.set_tracking_uri(settings.mlflow.tracking_uri)
    exp = mlflow.get_experiment_by_name(EXPERIMENT)
    if exp is None:
        mlflow.create_experiment(EXPERIMENT, artifact_location=settings.mlflow.artifact_root)
    mlflow.set_experiment(EXPERIMENT)

    model = xgb.XGBRegressor(**BEST_PARAMS)

    with mlflow.start_run(run_name="xgb-best-prod") as run:
        run_id = run.info.run_id
        # Log params (drop pure xgboost kwargs that aren't informative + add source)
        log_params = {**BEST_PARAMS,
                      "model_type": "XGBoost",
                      "end_date": str(end_date),
                      "window_days": settings.training.window_days,
                      "samples": len(X),
                      "selection": "best from tune_xgboost sweep"}
        mlflow.log_params(log_params)

        model.fit(X, y)
        y_pred = model.predict(X)
        train_mae = mean_absolute_error(y, y_pred)
        train_mape = mean_absolute_percentage_error(y, y_pred) * 100
        train_r2 = r2_score(y, y_pred)
        metrics = {"train_mae": train_mae, "train_mape": train_mape, "train_r2": train_r2}
        loggable = {k: v for k, v in metrics.items()
                    if not (isinstance(v, float) and math.isnan(v))}
        mlflow.log_metrics(loggable)

        # Log + register the model (skops trust needed for xgboost)
        mlflow.sklearn.log_model(
            model,
            "model",
            registered_model_name=MODEL_NAME,
            skops_trusted_types=("xgboost.core.Booster", "xgboost.sklearn.XGBRegressor"),
        )

        print(f"Registered {MODEL_NAME} from run {run_id}")
        print(f"Train MAPE: {train_mape:.2f}% | Train MAE: {train_mae:.2f} | R²: {train_r2:.3f}")


if __name__ == "__main__":
    main()

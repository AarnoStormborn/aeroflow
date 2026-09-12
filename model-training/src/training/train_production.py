"""
Production training: retrain the current-regime hourly model.

This is what Modal's `run_training` invokes (every 3 days). It:
1. Loads the freshest available feature data (the most recent contiguous run
   of days that actually have features — NOT a hardcoded window).
2. Guards on data sufficiency: if there aren't enough recent days/samples, it
   SKIPS (returns a status) rather than training a garbage model.
3. Trains the tuned XGBoost config and registers it as a new Production
   version of `flight-traffic-hourly`.

Kept separate from the older `train.py` (LinearRegression-only) which is
retained for reference/history.
"""

import argparse
import math
import os
from datetime import date, datetime, timedelta

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
from loguru import logger
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score
from src.training.config import settings
from src.training.data_loader import create_loader

EXPERIMENT = "flight-traffic-forecasting"
MODEL_NAME = "flight-traffic-hourly"

# Min data to train a useful model (tuned params; keep small for limited data)
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

# Data sufficiency guard: need at least this many recent DAYS with features
MIN_DAYS = 3
# and at least this many total samples
MIN_SAMPLES = 40

# A retrain must beat the incumbent Production model by at least this much
# (relative, on the same validation split) before it is promoted. Without a
# margin, noise on a small dataset causes the served model to flap between
# runs. Raise it to demand a clearer win.
MIN_IMPROVEMENT = 0.05


def should_promote(new_mape: float | None, incumbent_mape: float | None,
                   min_improvement: float = MIN_IMPROVEMENT) -> bool:
    """Decide whether a retrain should replace the served model.

    `incumbent_mape=None` means there is no Production model to beat (a fresh
    registry), which is the only case where promoting blind is correct.
    """
    if incumbent_mape is None:
        return True
    if new_mape is None or math.isnan(new_mape):
        return False
    return new_mape < incumbent_mape * (1 - min_improvement)


def incumbent_mape(X_va, y_va) -> tuple[float | None, str]:
    """Validation MAPE of the model currently in Production, on the SAME split.

    Returns (mape, status):
      "absent" - nothing in Production, so there is no incumbent to beat
      "ok"     - mape holds the incumbent's score on this exact validation set
      "error"  - a Production version exists but could not be loaded; we do not
                 replace a model we cannot measure
    """
    try:
        client = mlflow.tracking.MlflowClient()
        versions = [v for v in client.search_model_versions(f"name='{MODEL_NAME}'")
                    if v.current_stage == "Production"]
    except Exception as e:
        logger.warning(f"Could not list versions for {MODEL_NAME}: {e}")
        return None, "error"

    if not versions:
        return None, "absent"

    # `models:/<name>/Production` resolves to the highest version in the stage,
    # so score exactly that one rather than an arbitrary pick.
    target = max(versions, key=lambda v: int(v.version))
    try:
        model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{target.version}")
        pred = model.predict(X_va)
        return mean_absolute_percentage_error(y_va, pred) * 100, "ok"
    except Exception as e:
        logger.warning(f"Could not evaluate incumbent v{target.version}: {e}")
        return None, "error"


def load_freshest_features(end_date: date | None = None,
                           lookback_days: int = 21) -> pl.DataFrame:
    """Load the freshest contiguous block of feature days.

    Walks back from `end_date` (default yesterday) collecting days that have
    feature files, stopping at the first gap. Skips leading days with no file
    (e.g. today's features aren't built until tomorrow's run_feature).
    """
    loader = create_loader()
    end_date = end_date or (datetime.now().date() - timedelta(days=1))
    frames = []
    current = end_date
    days_found = 0
    # Walk back; tolerate a few missing leading days (today unprocessed)
    skipped = 0
    while current >= date(2020, 1, 1) and skipped < 3:
        df = loader.load_day(current)
        if df is not None and not df.is_empty():
            frames.append(df)
            days_found += 1
            skipped = 0
            current -= timedelta(days=1)
        else:
            if frames:
                break  # gap after a contiguous block ends the walk
            skipped += 1
            current -= timedelta(days=1)
    if not frames:
        return pl.DataFrame()
    combined = pl.concat(frames, how="diagonal_relaxed").sort("hour_start")
    logger.info(f"Freshest features: {days_found} days ending {end_date}, "
                f"{len(combined)} samples")
    return combined


def train_production(end_date: date | None = None) -> dict:
    """Retrain the production hourly model with a data-sufficiency guard."""
    df = load_freshest_features(end_date)

    if df.is_empty():
        return {"status": "skipped", "reason": "no fresh feature data available"}

    # Count distinct days present
    n_days = df.select(pl.col("hour_start").dt.date().alias("d")).unique().height
    n_samples = len(df)

    if n_days < MIN_DAYS or n_samples < MIN_SAMPLES:
        reason = (f"insufficient fresh data: {n_days} days / {n_samples} samples "
                  f"(need >= {MIN_DAYS}d / {MIN_SAMPLES}s)")
        logger.warning(reason)
        return {"status": "skipped", "reason": reason}

    X = df.select(settings.training.feature_columns).to_numpy()
    y = df.select(settings.training.target_column).to_numpy().flatten()

    # Time-based split: last 20% as validation
    split = int(len(X) * 0.8)
    X_tr, X_va = X[:split], X[split:]
    y_tr, y_va = y[:split], y[split:]

    print(f"Training on {n_days} days / {n_samples} samples "
          f"(train {len(X_tr)} / val {len(X_va)})")
    print(f"Traffic: mean {y.mean():.1f} | range {y.min():.0f}-{y.max():.0f}")

    mlflow.set_tracking_uri(settings.mlflow.tracking_uri)
    exp = mlflow.get_experiment_by_name(EXPERIMENT)
    if exp is None:
        mlflow.create_experiment(EXPERIMENT,
                                 artifact_location=settings.mlflow.artifact_root)
    mlflow.set_experiment(EXPERIMENT)

    model = xgb.XGBRegressor(**PARAMS)
    with mlflow.start_run(run_name="production-hourly-retrain") as run:
        run_id = run.info.run_id
        mlflow.log_params({
            **PARAMS,
            "model_type": "XGBoost-hourly-production",
            "data_days": n_days,
            "samples": n_samples,
            "end_date": str(end_date or (datetime.now().date() - timedelta(days=1))),
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

        logged = mlflow.sklearn.log_model(
            model,
            "model",
            registered_model_name=MODEL_NAME,
            skops_trusted_types=("xgboost.core.Booster",
                                 "xgboost.sklearn.XGBRegressor"),
        )

        # Promote only if the retrain actually beats what is being served.
        # Regression here is real: a backtest of v4 against retrains on the same
        # held-out days scored the retrains 3-5 points WORSE (MAPE 20.0-21.9%
        # vs 16.9%), so unconditional promotion would have degraded production.
        client = mlflow.tracking.MlflowClient()
        new_v = logged.registered_model_version
        incumbent, inc_status = incumbent_mape(X_va, y_va)
        promote = should_promote(metrics["val_mape"], incumbent)
        if inc_status == "error":
            # A Production model exists but we could not score it. Keep it.
            promote = False

        if incumbent is not None:
            mlflow.log_metric("incumbent_val_mape", incumbent)

        if new_v and promote:
            # archive_existing_versions keeps exactly one Production version, so
            # `models:/<name>/Production` can never silently fall back to an
            # older model that was left in the stage.
            client.transition_model_version_stage(
                MODEL_NAME, new_v, "Production", archive_existing_versions=True
            )
            logger.info(f"Promoted {MODEL_NAME} v{new_v} to Production")
            decision = "promoted"
            reason = ("no incumbent" if incumbent is None else
                      f"{metrics['val_mape']:.2f}% beats incumbent {incumbent:.2f}%")
        else:
            decision = "kept_incumbent"
            if inc_status == "error":
                reason = "incumbent could not be evaluated"
            elif incumbent is None:
                reason = "no registered version to promote"
            else:
                reason = (f"{metrics['val_mape']:.2f}% did not beat incumbent "
                          f"{incumbent:.2f}% by {MIN_IMPROVEMENT:.0%}")
            logger.warning(f"Not promoting {MODEL_NAME} v{new_v}: {reason}")

        print(f"\nRegistered {MODEL_NAME} v{new_v} from run {run_id} ({decision}: {reason})")
        print(f"Val MAPE: {metrics['val_mape']:.2f}%"
              + (f" | incumbent: {incumbent:.2f}%" if incumbent is not None else ""))

        return {
            "status": "trained",
            "decision": decision,
            "reason": reason,
            "run_id": run_id,
            "version": new_v,
            "val_mape": metrics["val_mape"],
            "incumbent_mape": incumbent,
            "n_days": n_days,
            "n_samples": n_samples,
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default=None)
    args = parser.parse_args()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else None
    result = train_production(end_date)
    print(result)


if __name__ == "__main__":
    main()

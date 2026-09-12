"""Backtest the production model against freshly retrained candidates.

Compares, on the same held-out days and with the same recursive path the
forecaster uses in production:

  - the current Production model (v4)
  - a candidate retrained before each test day using the SAME params as v4
  - a candidate retrained before each test day using the TUNED params

Walk-forward folds so the comparison is not decided by a single day, and every
model is scored only on days it never trained on.

Usage (repo root):
    set -a && source .env && set +a
    PYTHONPATH=forecasting:forecasting/src uv run --project forecasting \
        python scripts/backtest_models.py
"""

from __future__ import annotations

import io
import os
from datetime import date, datetime, timedelta

import boto3
import mlflow
import numpy as np
import polars as pl
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score
from src.forecasting.config import settings
from src.forecasting.data.loader import build_feature_vector
from xgboost import XGBRegressor

BUCKET = "flights-forecasting"
PREFIX = "features/hourly"
FEATURES = ["hour_of_day", "day_of_week", "is_weekend", "lag_1h", "lag_24h", "rolling_mean_6h"]
TARGET = "flight_count"
MODEL_NAME = "flight-traffic-hourly"
HORIZON = 6

# Exactly what train_production.py uses today.
PARAMS_BASE = {
    "n_estimators": 60,
    "max_depth": 3,
    "learning_rate": 0.1,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "min_child_weight": 1,
    "random_state": 42,
    "objective": "reg:squarederror",
}
# From tune_xgboost.py (best on the older Dec-Jan data).
PARAMS_TUNED = {
    "n_estimators": 100,
    "max_depth": 4,
    "learning_rate": 0.1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 3,
    "random_state": 42,
    "objective": "reg:squarederror",
}

CONTIGUOUS = [date(2026, 9, d) for d in range(3, 12)]  # Sep 3..Sep 11
# Fold A: train ->Sep 9,  test Sep 10
# Fold B: train ->Sep 10, test Sep 11
FOLDS = [
    (date(2026, 9, 9), date(2026, 9, 10)),
    (date(2026, 9, 10), date(2026, 9, 11)),
]
V4_TRAINED_THROUGH = date(2026, 9, 9)  # train_production ran Sep 10 11:22 with end_date=yesterday


def _s3():
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "ap-south-1"))


def feature_key(d: date) -> str:
    return f"{PREFIX}/year={d.year}/month={d.month:02d}/features_{d.isoformat()}.parquet"


def load_days(days: list[date]) -> pl.DataFrame:
    s3 = _s3()
    frames = []
    for d in days:
        try:
            raw = s3.get_object(Bucket=BUCKET, Key=feature_key(d))["Body"].read()
            frames.append(pl.read_parquet(io.BytesIO(raw)))
        except Exception as e:
            print(f"  ! missing {d}: {e}")
    df = pl.concat(frames, how="diagonal_relaxed").sort("hour_start")
    # hour_start comes back naive from parquet; treat as UTC everywhere
    return df.with_columns(pl.col("hour_start").dt.replace_time_zone("UTC"))


def fit(df: pl.DataFrame, params: dict) -> XGBRegressor:
    X = df.select(FEATURES).to_numpy()
    y = df.select(TARGET).to_numpy().flatten()
    m = XGBRegressor(**params)
    m.fit(X, y)
    return m


def stats(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    y_true, y_pred = np.asarray(y_true, float), np.asarray(y_pred, float)
    return {
        "n": len(y_true),
        "mape": float(mean_absolute_percentage_error(y_true, y_pred) * 100),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(np.mean((y_pred - y_true) ** 2))),
        "bias": float(np.mean((y_pred - y_true) / y_true) * 100),
        "r2": float(r2_score(y_true, y_pred)) if len(y_true) > 1 else float("nan"),
    }


def recursive_forecast(model, hourly: pl.DataFrame, origin: datetime) -> list[tuple[datetime, int, float]]:
    """Same recursion the production forecaster uses: predict h=1..6, feeding
    each prediction back in as lag_1h / rolling input."""
    predicted: dict[datetime, float] = {}
    out = []
    target = origin + timedelta(hours=1)
    for step in range(1, HORIZON + 1):
        feats = build_feature_vector(hourly, target, predicted_counts=predicted)
        X = pl.DataFrame([dict(zip(FEATURES, feats, strict=True))]).to_numpy()
        p = float(model.predict(X)[0])
        out.append((target, step, p))
        predicted[target] = p
        target += timedelta(hours=1)
    return out


def main() -> None:
    # Use the service's own configured tracking URI (env override or default)
    # rather than requiring another var in .env.
    mlflow.set_tracking_uri(settings.forecast.mlflow_tracking_uri)
    v4 = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/Production")
    print(f"loaded Production ({MODEL_NAME}) from MLflow\n")

    data = load_days(CONTIGUOUS)
    print(f"features: {len(data)} rows, {data['hour_start'].min()} -> {data['hour_start'].max()}\n")

    # ---- one-step-ahead backtest (walk-forward) --------------------------
    results: dict[str, list[tuple[np.ndarray, np.ndarray]]] = {}
    for train_end, test_day in FOLDS:
        train = data.filter(pl.col("hour_start").dt.date() <= train_end)
        test = data.filter(pl.col("hour_start").dt.date() == test_day)
        if train.is_empty() or test.is_empty():
            print(f"  skip fold test={test_day} (empty)")
            continue

        models = {
            "v4 (Production, Sep3-9)": v4,
            "retrain · base params": fit(train, PARAMS_BASE),
            "retrain · tuned params": fit(train, PARAMS_TUNED),
            "same-window · tuned": fit(data.filter(pl.col("hour_start").dt.date() <= V4_TRAINED_THROUGH), PARAMS_TUNED),
        }
        Xte = test.select(FEATURES).to_numpy()
        yte = test.select(TARGET).to_numpy().flatten()
        print(f"fold: train ->{train_end} ({len(train)} rows)  test {test_day} ({len(test)} rows)")
        for name, m in models.items():
            results.setdefault(name, []).append((yte, m.predict(Xte)))

    print("\n=== ONE-STEP-AHEAD BACKTEST (h=1, pooled over folds) ===")
    print(f"  {'model':26} {'n':>4} {'MAPE':>8} {'MAE':>7} {'RMSE':>7} {'bias':>8} {'R2':>7}")
    rows = []
    for name, parts in results.items():
        yt = np.concatenate([p[0] for p in parts])
        yp = np.concatenate([p[1] for p in parts])
        s = stats(yt, yp)
        rows.append((name, s))
        print(
            f"  {name:26} {s['n']:>4} {s['mape']:>7.1f}% {s['mae']:>7.2f} "
            f"{s['rmse']:>7.2f} {s['bias']:>+7.1f}% {s['r2']:>7.3f}"
        )

    # ---- recursive h=1..6 backtest --------------------------------------
    print(f"\n=== RECURSIVE BACKTEST (h=1..{HORIZON}, production path) ===")
    hourly = data.select(["hour_start", "flight_count"])
    counts = {r["hour_start"]: float(r["flight_count"]) for r in hourly.iter_rows(named=True)}

    candidates = {
        "v4 (Production)": v4,
        "retrain · tuned": fit(data.filter(pl.col("hour_start").dt.date() <= date(2026, 9, 10)), PARAMS_TUNED),
    }
    per_h: dict[str, dict[int, list[float]]] = {n: {h: [] for h in range(1, HORIZON + 1)} for n in candidates}
    maes: dict[str, dict[int, list[float]]] = {n: {h: [] for h in range(1, HORIZON + 1)} for n in candidates}

    origins = [
        h
        for h in sorted(counts)
        if date(2026, 9, 10) <= h.date() <= date(2026, 9, 11) and h.hour < 18  # need 6 future hours present
    ]
    for origin in origins:
        for name, m in candidates.items():
            for target, step, pred in recursive_forecast(m, hourly, origin):
                actual = counts.get(target)
                if actual is None:
                    continue
                per_h[name][step].append(abs(pred - actual) / actual * 100)
                maes[name][step].append(abs(pred - actual))

    print(f"  origins: {len(origins)} hours across Sep 10-11")
    for name in candidates:
        print(f"\n  {name}")
        for h in range(1, HORIZON + 1):
            v, a = per_h[name][h], maes[name][h]
            if v:
                print(f"    h={h}: MAPE {np.mean(v):6.1f}%   MAE {np.mean(a):5.2f}   (n={len(v)})")


if __name__ == "__main__":
    main()

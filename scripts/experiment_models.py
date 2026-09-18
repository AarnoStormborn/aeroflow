"""Model bake-off on the full hourly horizon (h=1..6, recursive).

Compares model families and hyperparameters the way production actually
forecasts: recursively, feeding each prediction back in as lag input, using the
same feature builder as the forecasting service (build_feature_vector). So the
numbers here are directly comparable to production behaviour.

Methodology notes that matter:
  - Time-ordered split only. No shuffling anywhere; a shuffled split would leak
    future hours into training.
  - Scoring excludes hours whose actuals are unreliable. `flight_count` is the
    union of unique aircraft across polls, so an hour with only 1-2 polls
    undercounts it (this is what made 2026-09-14 look like a bad model day).
    Poll counts come from the raw listing, same check the health job uses.
  - Hyperparameters are chosen on a validation window scored on the SAME
    recursive metric that is reported, not on a one-step proxy.

Usage (repo root):
    set -a && source .env && set +a
    PYTHONPATH=forecasting:forecasting/src uv run --project forecasting \
        --with lightgbm --with catboost python scripts/experiment_models.py
"""

from __future__ import annotations

import io
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import boto3
import numpy as np
import polars as pl
from catboost import CatBoostRegressor
from lightgbm import LGBMRegressor
from sklearn.ensemble import ExtraTreesRegressor, HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from src.forecasting.config import settings
from src.forecasting.data.loader import build_feature_vector
from src.forecasting.models.health import _polls_per_hour
from xgboost import XGBRegressor

BUCKET = "flights-forecasting"
FEATURES = ["hour_of_day", "day_of_week", "is_weekend", "lag_1h", "lag_24h", "rolling_mean_6h"]
TARGET = "flight_count"
HORIZON = 6
CONTIGUOUS = [date(2026, 9, d) for d in range(3, 18)]  # Sep 3..Sep 17
TEST_DAYS = {date(2026, 9, 15), date(2026, 9, 16), date(2026, 9, 17)}
VAL_DAYS = {date(2026, 9, 14)}  # tuning window


# ---------------------------------------------------------------- data


def load_days(days: list[date]) -> pl.DataFrame:
    s3 = boto3.client("s3", region_name=settings.s3.region)
    frames = []
    for d in days:
        key = f"{settings.s3.features_prefix}/year={d.year}/month={d.month:02d}/features_{d.isoformat()}.parquet"
        try:
            body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            frames.append(pl.read_parquet(io.BytesIO(body)))
        except Exception:
            continue
    return (
        pl.concat(frames, how="diagonal_relaxed")
        .sort("hour_start")
        .with_columns(pl.col("hour_start").dt.replace_time_zone("UTC"))
    )


def unreliable_hours(days: list[date]) -> set[datetime]:
    """Hours whose actual count is undercounted (>2 polls missing).

    flight_count is a union of unique aircraft over an hour's polls, so fewer
    polls means a smaller union that is not comparable to a full hour.
    """
    bad: set[datetime] = set()
    for d in days:
        polls = _polls_per_hour(d)
        for hour, n in polls.items():
            if n <= 2:
                bad.add(datetime(d.year, d.month, d.day, hour, tzinfo=timezone.utc))
        # also any hour with no file at all inside a covered day
        if polls:
            for hour in range(24):
                if hour not in polls and hour <= max(polls):
                    bad.add(datetime(d.year, d.month, d.day, hour, tzinfo=timezone.utc))
    return bad


# ---------------------------------------------------------------- models


def model_zoo() -> dict:
    """name -> (factory, param_grid | None). Grids are deliberately small; the
    dataset is ~300 rows so a huge search would just overfit the validation."""
    return {
        "ridge": (lambda **p: Ridge(**p), {"alpha": [0.1, 1.0, 10.0, 100.0]}),
        "random_forest": (
            lambda **p: RandomForestRegressor(random_state=42, n_jobs=-1, **p),
            {"n_estimators": [200, 400], "max_depth": [None, 6, 10], "min_samples_leaf": [1, 2, 4]},
        ),
        "extra_trees": (
            lambda **p: ExtraTreesRegressor(random_state=42, n_jobs=-1, **p),
            {"n_estimators": [200, 400], "max_depth": [None, 6, 10], "min_samples_leaf": [1, 2, 4]},
        ),
        "hist_gbm": (
            lambda **p: HistGradientBoostingRegressor(random_state=42, **p),
            {"max_iter": [100, 300], "learning_rate": [0.05, 0.1], "max_depth": [3, None], "min_samples_leaf": [5, 20]},
        ),
        "xgboost": (
            lambda **p: XGBRegressor(random_state=42, objective="reg:squarederror", **p),
            {
                "n_estimators": [100, 300, 600],
                "max_depth": [3, 4, 6],
                "learning_rate": [0.03, 0.1],
                "subsample": [0.8, 1.0],
                "colsample_bytree": [0.8, 1.0],
                "min_child_weight": [1, 3],
            },
        ),
        "lightgbm": (
            lambda **p: LGBMRegressor(random_state=42, verbose=-1, **p),
            {
                "n_estimators": [200, 500],
                "num_leaves": [15, 31, 63],
                "learning_rate": [0.03, 0.1],
                "min_child_samples": [5, 20],
                "subsample": [0.8, 1.0],
                "subsample_freq": [1],
            },
        ),
        "catboost": (
            lambda **p: CatBoostRegressor(random_state=42, verbose=0, **p),
            {"iterations": [300, 800], "depth": [4, 6, 8], "learning_rate": [0.03, 0.1]},
        ),
    }


# ---------------------------------------------------------------- evaluation


def recursive_predict(model, hourly: pl.DataFrame, origin: datetime) -> list[tuple[datetime, int, float]]:
    """Same recursion the production forecaster uses: h=1..6, each prediction
    fed back as lag input for the next step."""
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


class Persistence:
    """Baseline: repeat the last actual hour (this is what lag_1h encodes)."""

    def __init__(self, hourly: pl.DataFrame):
        self.counts = {r["hour_start"]: float(r["flight_count"]) for r in hourly.iter_rows(named=True)}

    def predict(self, X):
        lag1 = np.asarray(X)[:, FEATURES.index("lag_1h")]
        return lag1


def score_window(
    model, hourly: pl.DataFrame, counts: dict, days: set[date], bad: set[datetime]
) -> dict[int, list[tuple[float, float]]]:
    """Recursive predictions over `days`, bucketed by horizon as (pred, actual)."""
    out: dict[int, list[tuple[float, float]]] = defaultdict(list)
    origins = [h for h in sorted(counts) if h.date() in days and h.hour < 19]
    for origin in origins:
        for target, step, pred in recursive_predict(model, hourly, origin):
            if target in bad:
                continue  # undercounted hour; not comparable
            actual = counts.get(target)
            if actual is None:
                continue
            out[step].append((pred, actual))
    return out


def summarize(buckets: dict[int, list[tuple[float, float]]]) -> dict:
    per, allp = {}, []
    for step in range(1, HORIZON + 1):
        pairs = buckets.get(step, [])
        if not pairs:
            continue
        mapes = [abs(p - a) / a * 100 for p, a in pairs if a]
        maes = [abs(p - a) for p, a in pairs]
        per[step] = (float(np.mean(mapes)), float(np.mean(maes)), len(pairs))
        allp.extend(mapes)
    return {"per_horizon": per, "overall_mape": float(np.mean(allp)) if allp else float("nan")}


def main() -> None:
    data = load_days(CONTIGUOUS)
    bad = unreliable_hours(CONTIGUOUS)
    counts = {r["hour_start"]: float(r["flight_count"]) for r in data.iter_rows(named=True)}
    hourly = data.select(["hour_start", "flight_count"])

    print(f"data: {len(data)} rows, {data['hour_start'].min()} -> {data['hour_start'].max()}")
    print(f"unreliable (sparse-poll) hours excluded from scoring: {len(bad)}")
    train = data.filter(~pl.col("hour_start").dt.date().is_in(list(TEST_DAYS) + list(VAL_DAYS)))
    print(f"train {len(train)} rows | validation {sorted(VAL_DAYS)} | test {sorted(TEST_DAYS)}")

    Xtr = train.select(FEATURES).to_numpy()
    ytr = train.select(TARGET).to_numpy().flatten()

    results: dict[str, dict] = {}

    # ---- baselines (no tuning) ----
    print("\n=== baselines ===")
    baselines = {
        "persistence (lag_1h)": Persistence(hourly),
        "seasonal naive (lag_24h)": None,
        "train mean": None,
    }
    pers = baselines["persistence (lag_1h)"]
    results["persistence (lag_1h)"] = summarize(score_window(pers, hourly, counts, TEST_DAYS, bad))

    class Const:
        def __init__(self, v):
            self.v = v

        def predict(self, X):
            return np.full(len(X), self.v)

    class Seasonal:
        def predict(self, X):
            return np.asarray(X)[:, FEATURES.index("lag_24h")]

    results["seasonal naive (lag_24h)"] = summarize(score_window(Seasonal(), hourly, counts, TEST_DAYS, bad))
    results["train mean"] = summarize(score_window(Const(float(ytr.mean())), hourly, counts, TEST_DAYS, bad))

    # ---- hyperparameter search on the recursive validation metric ----
    rng = np.random.default_rng(42)
    for name, (factory, grid) in model_zoo().items():
        best = (None, float("inf"))
        trials = 0
        if grid:
            keys = list(grid)
            seen = set()
            for _ in range(24):
                params = {k: grid[k][int(rng.integers(len(grid[k])))] for k in keys}
                key = tuple(sorted(params.items()))
                if key in seen:
                    continue
                seen.add(key)
                trials += 1
                try:
                    m = factory(**params)
                    m.fit(Xtr, ytr)
                    sc = summarize(score_window(m, hourly, counts, VAL_DAYS, bad))["overall_mape"]
                except Exception:
                    continue
                if sc < best[1]:
                    best = (params, sc)
        else:
            best = ({}, 0.0)
        params, val_score = best
        print(f"  {name}: {trials} trials | best val MAPE(h1..6) {val_score:.1f}% | {params}")
        m = factory(**params)
        m.fit(Xtr, ytr)
        res = summarize(score_window(m, hourly, counts, TEST_DAYS, bad))
        res["params"] = params
        res["val_mape"] = val_score
        results[name] = res

    # ---- report ----
    print(f"\n=== TEST RESULTS (recursive h=1..{HORIZON}, {sorted(TEST_DAYS)}) ===")
    ordered = sorted(results.items(), key=lambda kv: kv[1]["overall_mape"])
    hdr = "  " + f"{'model':26}" + "".join(f"{'h=' + str(h):>8}" for h in range(1, HORIZON + 1)) + f"{'overall':>10}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for name, r in ordered:
        row = f"  {name:26}"
        for h in range(1, HORIZON + 1):
            v = r["per_horizon"].get(h)
            row += f"{v[0]:>7.1f}%" if v else f"{'-':>8}"
        row += f"{r['overall_mape']:>9.1f}%"
        print(row)

    print(f"\n  {'model':26}{'MAE h=1':>9}{'MAE h=6':>9}")
    for name, r in ordered:
        a1 = r["per_horizon"].get(1)
        a6 = r["per_horizon"].get(6)
        print(f"  {name:26}{(a1[1] if a1 else float('nan')):>9.2f}{(a6[1] if a6 else float('nan')):>9.2f}")


if __name__ == "__main__":
    main()

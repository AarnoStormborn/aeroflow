"""Walk-forward confirmation of the model bake-off.

The single-window bake-off (experiment_models.py) tuned on one validation day
and scored on three test days. That is enough to be suggestive and not enough to
act on: on 2026-09-14 the tuning day ranked lightgbm/hist_gbm best, yet on the
test window catboost won and hist_gbm placed fifth. So this re-runs the
comparison as a walk-forward over several folds, which is the honest way to rank
on a ~300-row dataset.

Protocol:
  - for each fold day d: train on every day before d, predict d recursively
    (h=1..6) and score, excluding sparse-poll hours whose actuals undercount
  - hyperparameters are FIXED per family (nested re-tuning per fold would be
    ideal but the per-fold training set is ~100-200 rows, where a search mostly
    fits noise); the point here is a stable ranking with a variance estimate
"""

from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl
from catboost import CatBoostRegressor
from experiment_models import (
    CONTIGUOUS,
    FEATURES,
    HORIZON,
    Persistence,
    load_days,
    recursive_predict,
    summarize,
    unreliable_hours,
)
from lightgbm import LGBMRegressor
from sklearn.ensemble import ExtraTreesRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from xgboost import XGBRegressor

FOLD_DAYS = [date(2026, 9, d) for d in range(12, 18)]  # Sep 12..17

FIXED = {
    # the config production actually uses today
    "xgboost (prod params)": lambda: XGBRegressor(
        random_state=42,
        objective="reg:squarederror",
        n_estimators=60,
        max_depth=3,
        learning_rate=0.1,
        subsample=0.9,
        colsample_bytree=0.9,
        min_child_weight=1,
    ),
    "xgboost (bake-off best)": lambda: XGBRegressor(
        random_state=42,
        objective="reg:squarederror",
        n_estimators=300,
        max_depth=4,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=1,
    ),
    "lightgbm": lambda: LGBMRegressor(
        random_state=42, verbose=-1, n_estimators=500, num_leaves=31, learning_rate=0.03, min_child_samples=20
    ),
    "catboost": lambda: CatBoostRegressor(random_state=42, verbose=0, iterations=800, depth=4, learning_rate=0.03),
    "extra_trees": lambda: ExtraTreesRegressor(random_state=42, n_jobs=-1, n_estimators=200, min_samples_leaf=1),
    "random_forest": lambda: RandomForestRegressor(
        random_state=42, n_jobs=-1, n_estimators=400, max_depth=10, min_samples_leaf=4
    ),
    "ridge": lambda: Ridge(alpha=0.1),
}


class Seasonal24:
    def predict(self, X):
        return np.asarray(X)[:, FEATURES.index("lag_24h")]


class AverageEnsemble:
    """Mean of several models, averaged at every recursion step.

    On a ~300-row dataset the top families land within noise of each other, so
    averaging is the natural way to buy robustness rather than picking a winner
    off a single window.
    """

    def __init__(self, factories):
        self.models = [f() for f in factories]

    def fit(self, X, y):
        for m in self.models:
            m.fit(X, y)
        return self

    def predict(self, X):
        return np.mean([np.asarray(m.predict(X), dtype=float) for m in self.models], axis=0)


def main() -> None:
    data = load_days(CONTIGUOUS)
    bad = unreliable_hours(CONTIGUOUS)
    counts = {r["hour_start"]: float(r["flight_count"]) for r in data.iter_rows(named=True)}
    hourly = data.select(["hour_start", "flight_count"])

    print(f"data {len(data)} rows | sparse hours excluded: {len(bad)}")
    print(f"walk-forward folds: {[d.isoformat() for d in FOLD_DAYS]}\n")

    per_fold: dict[str, dict[date, float]] = {}

    for fold in FOLD_DAYS:
        train = data.filter(pl.col("hour_start").dt.date() < fold)
        if len(train) < 60:
            continue
        Xtr = train.select(FEATURES).to_numpy()
        ytr = train.select("flight_count").to_numpy().flatten()

        models = {name: f() for name, f in FIXED.items()}
        models["ensemble (avg of 4)"] = AverageEnsemble(
            [
                FIXED["catboost"],
                FIXED["xgboost (prod params)"],
                FIXED["lightgbm"],
                FIXED["random_forest"],
            ]
        )
        models["persistence (lag_1h)"] = Persistence(hourly)
        models["seasonal naive (lag_24h)"] = Seasonal24()

        line = f"  {fold.isoformat()} (train {len(train):3} rows)"
        for name, m in models.items():
            try:
                m.fit(Xtr, ytr) if hasattr(m, "fit") else None
                origins = [h for h in sorted(counts) if h.date() == fold and h.hour < 19]
                pairs: dict[int, list[tuple[float, float]]] = {}
                for origin in origins:
                    for target, step, pred in recursive_predict(m, hourly, origin):
                        if target in bad:
                            continue
                        a = counts.get(target)
                        if a is None:
                            continue
                        pairs.setdefault(step, []).append((pred, a))
                mape = summarize(pairs)["overall_mape"]
                per_fold.setdefault(name, {})[fold] = mape
                line += f"  {name.split(' ')[0][:4]}={mape:5.1f}"
            except Exception as e:
                line += f"  {name[:4]}=ERR({type(e).__name__})"
        print(line)

    print(f"\n=== WALK-FORWARD AGGREGATE (recursive h=1..{HORIZON}, mean over folds) ===")
    rows = []
    for name, folds in per_fold.items():
        vals = list(folds.values())
        rows.append((name, float(np.mean(vals)), float(np.std(vals)), len(vals)))
    print(f"  {'model':26}{'mean MAPE':>11}{'std':>7}{'folds':>7}   per-fold")
    for name, mean, sd, n in sorted(rows, key=lambda r: r[1]):
        folds = per_fold[name]
        detail = " ".join(f"{folds[d]:.0f}" for d in sorted(folds))
        print(f"  {name:26}{mean:>10.1f}%{sd:>7.1f}{n:>7}   {detail}")


if __name__ == "__main__":
    main()

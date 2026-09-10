"""Head-to-head comparison of the deployed forecasting models.

Runs the production evaluator over the live forecast archive, then produces:
  - per-model archive coverage
  - per-horizon mean MAPE (unpaired)
  - a PAIRED comparison restricted to target-hours BOTH models produced
    (fair: the two models have different lifetimes in the archive)
  - bias / level analysis (predicted vs actual)
  - the champion decision exactly as champion.py would make it

Usage (from repo root):
    set -a && source .env && set +a
    PYTHONPATH=forecasting:forecasting/src .venv/bin/python scripts/compare_deployed_models.py
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone

from src.forecasting.models.champion import select_champion
from src.forecasting.models.evaluator import ForecastEvaluator

SHORT = "flight-traffic-"


def short(name: str) -> str:
    return name.replace(SHORT, "")


def main() -> None:
    ev = ForecastEvaluator()
    evals = ev.evaluate_all()

    # ---------------- archive coverage + unpaired per-horizon ----------------
    coverage: dict[str, int] = defaultdict(int)
    unpaired: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for e in evals:
        for name, m in e["models"].items():
            coverage[name] += 1
            for h_str, mape in m["per_horizon_mean_mape"].items():
                unpaired[name][int(h_str)].append(mape)

    models = sorted(coverage)
    print(f"\n=== Deployed model comparison — {len(evals)} evaluated forecasts ===\n")
    print("Archive coverage:")
    for n in models:
        print(f"  {short(n):22} {coverage[n]} forecasts")
    print()

    horizons = sorted({h for n in models for h in unpaired[n]})
    print("Per-horizon mean MAPE (unpaired — modeled over different lifetimes):")
    for h in horizons:
        cells = []
        for n in models:
            v = unpaired[n].get(h, [])
            cells.append(f"{short(n)} {statistics.mean(v):6.1f}%" if v else f"{short(n)}    n/a")
        print(f"  h={h:<2}  " + " | ".join(cells))

    # ---------------- paired, like-for-like ----------------
    s3 = ev._s3
    now = datetime.now(timezone.utc)
    eligible = []
    for key in ev._list_forecasts():
        try:
            fc = json.loads(s3.get_object(Bucket=ev.bucket, Key=key)["Body"].read())
            gen = datetime.fromisoformat(fc["generated_at"])
            if (now - gen).total_seconds() >= (ev.quarter_horizon + 1) * 3600:
                eligible.append((gen, fc))
        except Exception:
            continue

    if not eligible:
        print("\nNo forecasts old enough for a paired comparison yet.")
        return

    actuals = ev._load_actuals(min(g for g, _ in eligible), now)

    rows: dict[tuple[str, str, int], tuple[float, float]] = {}
    for _gen, fc in eligible:
        for name, steps in ev._model_predictions(fc).items():
            for step in steps:
                target = datetime.fromisoformat(step["hour_start"])
                actual = actuals.get(target)
                if actual is not None:
                    rows[(name, step["hour_start"], step["horizon_hours"])] = (
                        float(step["predicted_flight_count"]),
                        float(actual),
                    )

    per_model_targets: dict[str, set[tuple[str, int]]] = defaultdict(set)
    for name, tgt, h in rows:
        per_model_targets[name].add((tgt, h))
    common = set.intersection(*per_model_targets.values()) if per_model_targets else set()

    print(f"\nPaired comparison — {len(common)} target-hours produced by BOTH models")
    print(
        "  (unpaired target counts: "
        + ", ".join(f"{short(n)}={len(v)}" for n, v in sorted(per_model_targets.items()))
        + ")"
    )

    if not common:
        print("  No overlapping target-hours; cannot pair.")
        return

    print("\n  h  | " + " | ".join(f"{short(n) + ' MAPE/MAE/bias':>30}" for n in models))
    for h in sorted({h for _, h in common}):
        cells = []
        for n in models:
            subset = [rows[(n, t, hh)] for (t, hh) in common if hh == h and (n, t, hh) in rows]
            if not subset:
                cells.append(f"{'n/a':>30}")
                continue
            mapes = [abs(p - a) / a * 100 for p, a in subset if a]
            maes = [abs(p - a) for p, a in subset]
            biases = [(p - a) / a * 100 for p, a in subset if a]
            cells.append(
                f"{statistics.mean(mapes):>8.1f}% /{statistics.mean(maes):>6.1f} /{statistics.mean(biases):>+8.1f}%"
            )
        print(f"  {h:<2} | " + " | ".join(cells))

    # ---------------- paired h=1 head-to-head ----------------
    h1 = sorted({(t, hh) for (t, hh) in common if hh == 1})
    print(f"\nPaired h=1 head-to-head ({len(h1)} shared target-hours):")
    wins = dict.fromkeys(models, 0)
    for t, hh in h1:
        errs = {}
        for n in models:
            if (n, t, hh) in rows:
                p, a = rows[(n, t, hh)]
                errs[n] = abs(p - a) / a * 100 if a else float("inf")
        if errs:
            wins[min(errs, key=lambda k: errs[k])] += 1

    print(f"  {'model':22} {'mean h=1 MAPE':>14} {'MAE':>7} {'bias':>9} {'wins':>8}  pred/actual")
    for n in models:
        subset = [rows[(n, t, hh)] for (t, hh) in h1 if (n, t, hh) in rows]
        if not subset:
            continue
        mapes = [abs(p - a) / a * 100 for p, a in subset if a]
        maes = [abs(p - a) for p, a in subset]
        biases = [(p - a) / a * 100 for p, a in subset if a]
        preds = [p for p, _ in subset]
        acts = [a for _, a in subset]
        ratio = statistics.mean(preds) / statistics.mean(acts) if statistics.mean(acts) else float("nan")
        print(
            f"  {short(n):22} {statistics.mean(mapes):>13.1f}% {statistics.mean(maes):>7.1f} "
            f"{statistics.mean(biases):>+8.1f}% {wins[n]:>4}/{len(h1):<3} "
            f"{statistics.mean(preds):>5.1f}/{statistics.mean(acts):<5.1f} ({ratio:.2f}x)"
        )

    # ---------------- champion decision ----------------
    decision = select_champion(evals)
    print("\n=== champion.py auto-promotion decision ===")
    if decision is None:
        print("  not enough comparable data (needs 2+ models with >=20 comparisons each)")
        return
    print(f"  winner: {short(decision['champion'])}  (h=1 MAPE {decision['champion_h1_mape']:.2f}%)")
    for n, a in sorted(decision["comparisons"].items(), key=lambda kv: kv[1]["h1_mape"]):
        print(f"    {short(n):22} h=1 MAPE {a['h1_mape']:7.2f}%  over {a['n']} forecasts")


if __name__ == "__main__":
    main()

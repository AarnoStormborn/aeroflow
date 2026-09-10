"""Diagnose the systematic bias in deployed forecasts, and test recalibration.

Finding to test: for both models, mean bias% ~= mean MAPE%, which implies the
error is dominated by a constant multiplicative level offset rather than
timing/shape error. If true, a simple scale correction recovers most of the
accuracy -- and the honest way to measure it is to fit the scale on an early
window and evaluate on a later window (out-of-sample).

Usage (from repo root):
    set -a && source .env && set +a
    PYTHONPATH=forecasting:forecasting/src .venv/bin/python scripts/diagnose_forecast_bias.py
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone

from src.forecasting.models.evaluator import ForecastEvaluator

SHORT = "flight-traffic-"


def short(n: str) -> str:
    return n.replace(SHORT, "")


def mape(pairs: list[tuple[float, float]]) -> float:
    vals = [abs(p - a) / a * 100 for p, a in pairs if a]
    return statistics.mean(vals) if vals else float("nan")


def main() -> None:
    ev = ForecastEvaluator()
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
        print("no eligible forecasts")
        return

    actuals = ev._load_actuals(min(g for g, _ in eligible), now)

    # model -> list of (target_dt, horizon, pred, actual)
    data: dict[str, list[tuple[datetime, int, float, float]]] = defaultdict(list)
    for _gen, fc in eligible:
        for name, steps in ev._model_predictions(fc).items():
            for step in steps:
                target = datetime.fromisoformat(step["hour_start"])
                actual = actuals.get(target)
                if actual is not None:
                    data[name].append(
                        (target, step["horizon_hours"], float(step["predicted_flight_count"]), float(actual))
                    )

    models = sorted(data)

    # ---------- bias by day (h=1) ----------
    print("=== h=1 bias by target date (is the over-prediction drifting?) ===")
    print(f"  {'date':12}" + "".join(f"{short(m):>24}" for m in models))
    by_day: dict[str, dict[str, list[tuple[float, float]]]] = defaultdict(lambda: defaultdict(list))
    for m in models:
        for target, h, p, a in data[m]:
            if h == 1:
                by_day[target.date().isoformat()][m].append((p, a))
    for day in sorted(by_day):
        cells = []
        for m in models:
            pairs = by_day[day].get(m, [])
            if not pairs:
                cells.append(f"{'-':>24}")
                continue
            mp = statistics.mean(p for p, _ in pairs)
            ma = statistics.mean(a for _, a in pairs)
            cells.append(f"{mp:>8.1f}/{ma:<6.1f}({mp / ma:4.2f}x)")
        print(f"  {day:12}" + "".join(cells))

    # ---------- out-of-sample multiplicative recalibration (h=1) ----------
    print("\n=== Does a constant scale fix it? (fit on first 50% of days, eval on rest) ===")
    print(f"  {'model':22} {'raw MAPE':>10} {'cal MAPE':>10} {'scale':>7}  verdict")
    for m in models:
        h1 = sorted([(t, p, a) for t, h, p, a in data[m] if h == 1], key=lambda x: x[0])
        if len(h1) < 10:
            continue
        days = sorted({t.date() for t, _, _ in h1})
        if len(days) < 2:
            print(f"  {short(m):22} too few distinct days to split")
            continue
        cut = days[len(days) // 2]
        train = [(p, a) for t, p, a in h1 if t.date() < cut]
        test = [(p, a) for t, p, a in h1 if t.date() >= cut]
        if not train or not test:
            continue
        # NOTE: train entries are (predicted, actual) pairs. The second
        # unpacking below must therefore be `for p, _`, not `for _, p`.
        scale = sum(a for _, a in train) / sum(p for p, _ in train)
        cal = [(p * scale, a) for p, a in test]
        raw_m, cal_m = mape(test), mape(cal)
        verdict = "LEVEL OFFSET dominates" if cal_m < raw_m * 0.5 else "shape/other error dominates"
        print(f"  {short(m):22} {raw_m:>9.1f}% {cal_m:>9.1f}% {scale:>7.3f}  {verdict}")

    # ---------- training vs current level ----------
    print("\n=== Level context ===")
    for m in models:
        h1 = [(p, a) for t, h, p, a in data[m] if h == 1]
        if not h1:
            continue
        preds = [p for p, _ in h1]
        acts = [a for _, a in h1]
        print(
            f"  {short(m):22} mean pred {statistics.mean(preds):6.1f} | "
            f"mean actual {statistics.mean(acts):6.1f} | "
            f"ratio {statistics.mean(preds) / statistics.mean(acts):.2f}x"
        )
    print("\n  (Training-era traffic was ~57-81 aircraft/hr; current actuals are far lower,")
    print("   so a model trained on the earlier regime predicts a level that no longer holds.)")


if __name__ == "__main__":
    main()

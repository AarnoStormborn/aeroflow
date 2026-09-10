"""Accuracy report for the single deployed forecasting model.

Evaluates the live forecast archive against actuals and reports:
  - which model version produced each forecast (provenance)
  - per-horizon MAPE / MAE / bias
  - h=1 accuracy by target date, so drift is visible
  - an out-of-sample level-offset test: does a constant rescale explain the
    error? (tells level/regime error apart from shape/timing error)
  - predicted vs actual level context

Usage (from repo root):
    set -a && source .env && set +a
    PYTHONPATH=forecasting:forecasting/src .venv/bin/python scripts/forecast_accuracy.py
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone

from src.forecasting.config import settings
from src.forecasting.models.evaluator import ForecastEvaluator

SHORT = "flight-traffic-"


def short(name: str) -> str:
    return name.replace(SHORT, "")


def _mape(pairs: list[tuple[float, float]]) -> float:
    vals = [abs(p - a) / a * 100 for p, a in pairs if a]
    return statistics.mean(vals) if vals else float("nan")


def main() -> None:
    ev = ForecastEvaluator()
    s3 = ev._s3
    now = datetime.now(timezone.utc)

    # (generated_at, forecast_doc) for everything with elapsed actuals
    eligible: list[tuple[datetime, dict]] = []
    for key in ev._list_forecasts():
        try:
            fc = json.loads(s3.get_object(Bucket=ev.bucket, Key=key)["Body"].read())
            gen = datetime.fromisoformat(fc["generated_at"])
            if (now - gen).total_seconds() >= (ev.quarter_horizon + 1) * 3600:
                eligible.append((gen, fc))
        except Exception:
            continue

    if not eligible:
        print("No forecasts old enough to evaluate yet.")
        return

    actuals = ev._load_actuals(min(g for g, _ in eligible), now)

    # model -> [(target, horizon, predicted, actual)]
    data: dict[str, list[tuple[datetime, int, float, float]]] = defaultdict(list)
    versions: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for _gen, fc in eligible:
        for name, m in fc.get("models", {}).items():
            versions[name][str(m.get("model_version"))] += 1
            for step in m.get("quarter_daily", []):
                target = datetime.fromisoformat(step["hour_start"])
                actual = actuals.get(target)
                if actual is not None:
                    data[name].append(
                        (
                            target,
                            step["horizon_hours"],
                            float(step["predicted_flight_count"]),
                            float(actual),
                        )
                    )

    models = sorted(data)
    served = [name for name, _stage in settings.forecast.models]

    print(f"\n=== Forecast accuracy — {len(eligible)} evaluated forecasts ===\n")
    print("Served model(s): " + ", ".join(f"{short(n)}/Production" for n in served))
    print("\nProvenance (registered version that produced each forecast):")
    for n in models:
        vs = ", ".join(f"v{v}: {c}" for v, c in sorted(versions[n].items()))
        print(f"  {short(n):22} {vs}")

    # ---------------- per-horizon ----------------
    print("\nPer-horizon error:")
    print(f"  {'model':22} {'h':>2} {'MAPE':>8} {'MAE':>7} {'bias':>9} {'n':>6}")
    for n in models:
        for h in sorted({h for _, h, _, _ in data[n]}):
            rows = [(p, a) for _, hh, p, a in data[n] if hh == h]
            bias = statistics.mean((p - a) / a * 100 for p, a in rows if a)
            print(
                f"  {short(n):22} {h:>2} {_mape(rows):>7.1f}% "
                f"{statistics.mean(abs(p - a) for p, a in rows):>7.1f} "
                f"{bias:>+8.1f}% {len(rows):>6}"
            )

    # ---------------- h=1 by date ----------------
    print("\nh=1 by target date (spot drift):")
    for n in models:
        print(f"\n  {short(n)}")
        by_day: dict[str, list[tuple[float, float]]] = defaultdict(list)
        for target, h, p, a in data[n]:
            if h == 1:
                by_day[target.date().isoformat()].append((p, a))
        for day in sorted(by_day):
            rows = by_day[day]
            bias = statistics.mean((p - a) / a * 100 for p, a in rows if a)
            print(
                f"    {day}  n={len(rows):>3}  MAPE {_mape(rows):>6.1f}%  "
                f"MAE {statistics.mean(abs(p - a) for p, a in rows):>5.1f}  bias {bias:>+6.1f}%"
            )

    # ---------------- level-offset test ----------------
    print("\nLevel-offset test (fit scale on first half of days, score the rest):")
    print(f"  {'model':22} {'raw MAPE':>10} {'rescaled':>10} {'scale':>7}  verdict")
    for n in models:
        h1 = sorted([(t, p, a) for t, h, p, a in data[n] if h == 1], key=lambda x: x[0])
        days = sorted({t.date() for t, _, _ in h1})
        if len(h1) < 10 or len(days) < 2:
            print(f"  {short(n):22} too few samples/days to split")
            continue
        cut = days[len(days) // 2]
        train = [(p, a) for t, p, a in h1 if t.date() < cut]
        test = [(p, a) for t, p, a in h1 if t.date() >= cut]
        if not train or not test:
            continue
        # train entries are (predicted, actual) -> unpack as such
        scale = sum(a for _, a in train) / sum(p for p, _ in train)
        raw_m, cal_m = _mape(test), _mape([(p * scale, a) for p, a in test])
        verdict = "level/regime offset dominates" if cal_m < raw_m * 0.5 else "shape/timing error dominates"
        print(f"  {short(n):22} {raw_m:>9.1f}% {cal_m:>9.1f}% {scale:>7.3f}  {verdict}")

    # ---------------- level context ----------------
    print("\nLevel context (h=1):")
    for n in models:
        rows = [(p, a) for _, h, p, a in data[n] if h == 1]
        if not rows:
            continue
        mp, ma = statistics.mean(p for p, _ in rows), statistics.mean(a for _, a in rows)
        print(f"  {short(n):22} mean predicted {mp:6.1f} | mean actual {ma:6.1f} | ratio {mp / ma:5.2f}x")


if __name__ == "__main__":
    main()

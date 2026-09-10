"""
Forecast quality report → Discord (multi-model comparison).

Runs the forecast evaluator, aggregates per-horizon MAPE across all
evaluable forecasts FOR EACH MODEL, renders a grouped bar chart
(MAPE by horizon, one bar series per model), and posts it to Discord.

Run daily (Modal run_eval) once enough forecasts have elapsed.
"""

import io
import json
import os
from datetime import datetime, timezone

import httpx
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
import matplotlib.pyplot as plt
import numpy as np
from src.forecasting.models.evaluator import ForecastEvaluator

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
DISCORD_ENABLED = os.environ.get("DISCORD_ENABLED", "true").lower() == "true"


def aggregate_by_model_horizon(evals: list[dict]) -> dict[str, dict[str, dict]]:
    """Aggregate per-model per-horizon MAPE across forecasts.

    evals: [{models: {name: {per_horizon_mean_mape: {h: mape}}}}]
    Returns {model_name: {horizon: {"mean_mape": ..., "n": ...}}}.
    """
    out: dict[str, dict[str, dict]] = {}
    for e in evals:
        for name, m in e.get("models", {}).items():
            model_agg = out.setdefault(name, {})
            for h, mape in m.get("per_horizon_mean_mape", {}).items():
                if mape is None:
                    continue
                bucket = model_agg.setdefault(h, {"mape": [], "n": 0})
                bucket["mape"].append(mape)
    # Collapse
    result: dict[str, dict[str, dict]] = {}
    for name, horizons in out.items():
        result[name] = {
            h: {"mean_mape": sum(b["mape"]) / len(b["mape"]), "n": len(b["mape"])}
            for h, b in sorted(horizons.items(), key=lambda kv: int(kv[0]))
        }
    return result


def render_graph(agg: dict[str, dict[str, dict]]) -> bytes:
    """Grouped bar chart: MAPE by horizon, one series per model."""
    # Collect all horizons across models
    all_h = sorted({h for m in agg.values() for h in m.keys()}, key=int)
    model_names = list(agg.keys())
    colors = ["#5865F2", "#57F287", "#FEE75C", "#EB459E"]  # discord palette
    short = {n: n.replace("flight-traffic-", "") for n in model_names}

    fig, ax = plt.subplots(figsize=(9, 5))
    width = 0.35 / max(len(model_names), 1)
    for i, name in enumerate(model_names):
        m = agg[name]
        mapes = [m.get(h, {}).get("mean_mape", 0) for h in all_h]
        x = np.arange(len(all_h)) + i * width
        bars = ax.bar(x, mapes, width, label=short[name], color=colors[i % len(colors)], alpha=0.85)
        for bar, v in zip(bars, mapes, strict=True):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.3,
                f"{v:.1f}",
                ha="center",
                va="bottom",
                fontsize=8,
            )

    ax.set_xticks(np.arange(len(all_h)) + width * (len(model_names) - 1) / 2)
    ax.set_xticklabels([f"{h}h" for h in all_h])
    ax.set_xlabel("Forecast Horizon")
    ax.set_ylabel("Mean MAPE (%)")
    ax.set_title("Forecast Accuracy by Horizon — Model Comparison")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def send_discord_report(agg: dict[str, dict[str, dict]], png_bytes: bytes, n_forecasts: int) -> bool:
    if not DISCORD_ENABLED or not DISCORD_WEBHOOK_URL:
        print("Discord disabled or no webhook — skipping")
        return False

    short = {n: n.replace("flight-traffic-", "") for n in agg}
    lines = []
    for name, horizons in agg.items():
        h1 = horizons.get("1", {})
        h1v = h1.get("mean_mape")
        line = f"**{short[name]}:** "
        if h1v is not None:
            line += f"h=1 MAPE {h1v:.1f}% ({h1.get('n', 0)} fcst)"
            # worst horizon
            worst = max(horizons.values(), key=lambda b: b["mean_mape"])
            line += f" | worst {worst['mean_mape']:.1f}%"
        else:
            line += "no data"
        lines.append(line)
    summary_text = "\n".join(lines) if lines else "No evaluable forecasts yet."

    color = 0x00FF00 if all(h.get("mean_mape", 99) < 25 for m in agg.values() for h in m.values()) else 0xFFA500

    embed = {
        "title": "📈 Forecast Quality — Model Comparison",
        "color": color,
        "description": f"Per-model recursive accuracy by horizon.\n\n{summary_text}",
        "fields": [
            {"name": "Forecasts evaluated", "value": str(n_forecasts), "inline": True},
            {"name": "Models", "value": ", ".join(short.values()), "inline": True},
        ],
        "footer": {
            "text": f"as of {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        },
        "image": {"url": "attachment://forecast_accuracy.png"},
    }

    payload = {"content": None, "embeds": [embed]}
    files = {"forecast_accuracy.png": ("forecast_accuracy.png", png_bytes, "image/png")}
    try:
        with httpx.Client(timeout=20) as client:
            resp = client.post(DISCORD_WEBHOOK_URL, data={"payload_json": json.dumps(payload)}, files=files)
            if resp.status_code in (200, 204):
                print("Forecast quality report sent to Discord")
                return True
            print(f"Discord webhook failed: {resp.status_code} {resp.text[:300]}")
            return False
    except Exception as e:
        print(f"Failed to send report: {e}")
        return False


def main():
    evaluator = ForecastEvaluator()
    evals = evaluator.evaluate_all()
    if not evals:
        print("No forecasts old enough to evaluate yet — skipping report.")
        return

    agg = aggregate_by_model_horizon(evals)
    print("=== Forecast Quality (per model) ===")
    for name, horizons in agg.items():
        for h, b in horizons.items():
            print(f"  {name} h={h}: {b['mean_mape']:.2f}% ({b['n']} fcst)")
    png = render_graph(agg)
    send_discord_report(agg, png, len(evals))


if __name__ == "__main__":
    main()

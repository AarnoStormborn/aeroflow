"""
Forecast quality report → Discord.

Runs the forecast evaluator, aggregates per-horizon MAPE across all
evaluable forecasts, renders a bar chart (MAPE by horizon h=1..6), and
posts it to Discord as an embed with the graph attached.

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
from src.forecasting.config import settings
from src.forecasting.models.evaluator import ForecastEvaluator

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
DISCORD_ENABLED = os.environ.get("DISCORD_ENABLED", "true").lower() == "true"


def aggregate_by_horizon(evals: list[dict]) -> dict[str, dict]:
    """Aggregate per-horizon MAPE across forecasts.

    Each eval has per_horizon_mean_mape = {"1": x, ..., "6": y}.
    Returns {horizon: {"mean_mape": ..., "n": ...}}.
    """
    horizon_mape: dict[str, list[float]] = {}
    for e in evals:
        for h, m in e.get("per_horizon_mean_mape", {}).items():
            if m is not None:
                horizon_mape.setdefault(h, []).append(m)
    out = {}
    for h in sorted(horizon_mape, key=int):
        vals = horizon_mape[h]
        out[h] = {"mean_mape": sum(vals) / len(vals), "n": len(vals)}
    return out


def render_graph(agg: dict[str, dict]) -> bytes:
    """Bar chart of mean MAPE by forecast horizon. Returns PNG bytes."""
    horizons = sorted(agg.keys(), key=int)
    mapes = [agg[h]["mean_mape"] for h in horizons]
    ns = [agg[h]["n"] for h in horizons]

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(horizons, mapes, color="#5865F2", alpha=0.85)
    ax.set_xlabel("Forecast Horizon (hours)")
    ax.set_ylabel("Mean MAPE (%)")
    ax.set_title("Forecast Accuracy by Horizon (recursive)")
    ax.set_ylim(0, max(mapes) * 1.25 if mapes else 1)

    for bar, m, n in zip(bars, mapes, ns, strict=True):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{m:.1f}%", ha="center", va="bottom", fontsize=9)
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() / 2,
                f"n={n}", ha="center", va="center", fontsize=8, color="white")

    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def send_discord_report(agg: dict[str, dict], png_bytes: bytes, n_forecasts: int) -> bool:
    """Post the report embed + graph to Discord via webhook."""
    if not DISCORD_ENABLED or not DISCORD_WEBHOOK_URL:
        print("Discord disabled or no webhook — skipping")
        return False

    # Text summary for the embed
    lines = []
    for h in sorted(agg.keys(), key=int):
        a = agg[h]
        lines.append(f"**h={h}h:** {a['mean_mape']:.1f}% MAPE ({a['n']} forecasts)")
    summary_text = "\n".join(lines) if lines else "No evaluable forecasts yet."

    color = 0x00FF00 if all(a["mean_mape"] < 15 for a in agg.values()) else 0xFFA500

    embed = {
        "title": "📈 Forecast Quality Report",
        "color": color,
        "description": f"Recursive prediction accuracy by horizon.\n\n{summary_text}",
        "fields": [
            {"name": "Forecasts evaluated", "value": str(n_forecasts), "inline": True},
            {"name": "Model", "value": settings.forecast.registered_model, "inline": True},
        ],
        "footer": {
            "text": f"as of {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        },
        "image": {"url": "attachment://forecast_accuracy.png"},
    }

    payload = {
        "content": None,
        "embeds": [embed],
    }
    files = {
        "forecast_accuracy.png": ("forecast_accuracy.png", png_bytes,
                                  "image/png"),
    }
    # Webhook with file: multipart form
    try:
        with httpx.Client(timeout=20) as client:
            # Discord supports files via multipart with payload_json
            data = {"payload_json": json.dumps(payload)}
            resp = client.post(DISCORD_WEBHOOK_URL, data=data, files=files)
            if resp.status_code == 204 or resp.status_code == 200:
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

    agg = aggregate_by_horizon(evals)
    png = render_graph(agg)
    # Print the table too
    print("=== Forecast Quality ===")
    for h in sorted(agg.keys(), key=int):
        a = agg[h]
        print(f"  h={h}: mean MAPE {a['mean_mape']:.2f}% ({a['n']} forecasts)")
    send_discord_report(agg, png, len(evals))


if __name__ == "__main__":
    main()

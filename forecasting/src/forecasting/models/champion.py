"""
Champion selection: promote the best-performing model to Production.

After enough forecast-vs-actual samples accumulate, compares the models
that are forecast in parallel (per h=1 MAPE) and promotes the winner to
Production in the MLflow registry. The loser is left registered but
demoted/archived so the forecast pipeline always uses the champion.

Promotion happens on the MLflow server (Modal) via the tracking API.
"""


import mlflow
from loguru import logger
from src.forecasting.config import settings

# Minimum number of forecast comparisons required before we auto-promote.
# Avoids flipping the champion on tiny sample counts / noise.
MIN_COMPARISONS = 20


def _aggregate(evals: list[dict]) -> dict[str, dict]:
    """{model_name: {h1_mape: mean, n: count}} from eval results."""
    model_h1: dict[str, dict] = {}
    for e in evals:
        for name, m in e.get("models", {}).items():
            if "1" in m.get("per_horizon_mean_mape", {}):
                bucket = model_h1.setdefault(name, {"sum": 0.0, "n": 0})
                bucket["sum"] += m["per_horizon_mean_mape"]["1"]
                bucket["n"] += 1
    return {
        name: {"h1_mape": b["sum"] / b["n"], "n": b["n"]}
        for name, b in model_h1.items()
        if b["n"] > 0
    }


def select_champion(evals: list[dict],
                    min_comparisons: int = MIN_COMPARISONS) -> dict | None:
    """Compare models and return the champion decision dict, or None if
    there isn't enough data yet."""
    agg = _aggregate(evals)
    eligible = {n: a for n, a in agg.items() if a["n"] >= min_comparisons}
    if len(eligible) < 2:
        return None  # need at least 2 models with enough samples

    champion = min(eligible, key=lambda n: eligible[n]["h1_mape"])
    return {
        "champion": champion,
        "champion_h1_mape": eligible[champion]["h1_mape"],
        "comparisons": eligible,
    }


def promote_champion(decision: dict) -> dict:
    """Promote the champion model version to Production in MLflow."""
    champion = decision["champion"]
    mlflow.set_tracking_uri(settings.forecast.mlflow_tracking_uri)
    client = mlflow.tracking.MlflowClient()

    # Promote the latest READY version of the champion to Production
    versions = client.search_model_versions(f"name='{champion}'")
    ready = [v for v in versions if v.status == "READY"]
    if not ready:
        logger.warning(f"No READY versions for {champion}")
        return {"promoted": False, "reason": "no ready versions"}
    target = max(ready, key=lambda v: int(v.version))

    if target.current_stage == "Production":
        logger.info(f"{champion} v{target.version} already Production")
        return {"promoted": False, "reason": "already production"}

    client.transition_model_version_stage(champion, target.version, "Production")
    logger.info(f"Promoted {champion} v{target.version} to Production "
                f"(h=1 MAPE {decision['champion_h1_mape']:.2f}%)")

    # Demote other models from Production so only the champion is active
    for name in decision["comparisons"]:
        if name == champion:
            continue
        for v in client.search_model_versions(f"name='{name}'"):
            if v.current_stage == "Production":
                client.transition_model_version_stage(name, v.version, "Archived")
                logger.info(f"Archived {name} v{v.version} (was Production)")

    return {"promoted": True, "champion": champion,
            "version": target.version,
            "h1_mape": decision["champion_h1_mape"]}


def run_autopromote(evals: list[dict]) -> dict:
    """Evaluate + optionally promote the champion. Returns decision/result."""
    decision = select_champion(evals)
    if decision is None:
        logger.info("Not enough comparisons to auto-promote yet")
        return {"autopromoted": False, "reason": "insufficient data"}
    print(f"Champion candidate: {decision['champion']} "
          f"(h=1 MAPE {decision['champion_h1_mape']:.2f}%)")
    result = promote_champion(decision)
    return {"autopromoted": result.get("promoted", False), **result}


if __name__ == "__main__":
    # Quick self-test with fake data
    fake = [
        {"models": {
            "model-a": {"per_horizon_mean_mape": {"1": 10.0}},
            "model-b": {"per_horizon_mean_mape": {"1": 20.0}},
        }} for _ in range(25)
    ]
    print("Decision:", select_champion(fake))

"""
On-demand forecast API (FastAPI).

Serves a fresh forecast over HTTP so it can be queried on demand (vs. the
hourly scheduled run). Exposed on Modal as a web endpoint.

Endpoints:
    GET /forecast          -> run a fresh forecast now (all configured models)
    GET /health            -> liveness
    GET /models            -> which models are configured
"""

from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel
from src.forecasting.config import settings
from src.forecasting.models.forecaster import ForecastEngine

app = FastAPI(title="Aeroflow Forecasting API")


class ModelForecast(BaseModel):
    model_stage: str
    # Registered MLflow version that produced these predictions (provenance).
    # Optional: the lookup is best-effort and must not break serving.
    model_version: str | None = None
    hourly: dict
    quarter_daily: list[dict]


class ForecastResponse(BaseModel):
    schema_version: int = 2
    generated_at: str
    last_actual_hour: str | None
    models: dict[str, ModelForecast]


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.get("/models")
def models() -> dict:
    return {"models": [{"name": n, "stage": s} for n, s in settings.forecast.models]}


@app.get("/forecast", response_model=ForecastResponse)
def forecast() -> ForecastResponse:
    """Run a fresh forecast and return it."""
    engine = ForecastEngine()
    result = engine.forecast()
    return ForecastResponse(
        schema_version=result.get("schema_version", 2),
        generated_at=result["generated_at"],
        last_actual_hour=result["last_actual_hour"],
        models={
            name: ModelForecast(
                model_stage=m["model_stage"],
                model_version=m.get("model_version"),
                hourly=m["hourly"],
                quarter_daily=m["quarter_daily"],
            )
            for name, m in result["models"].items()
        },
    )

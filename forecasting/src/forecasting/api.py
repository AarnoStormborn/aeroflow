"""
On-demand forecast API (FastAPI).

Serves the latest multi-model forecast over HTTP so it can be queried on
demand (vs. the hourly scheduled run). Exposed on Modal as a web endpoint.

Endpoints:
    GET /forecast          -> run a fresh forecast now (all models)
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
    hourly: dict
    quarter_daily: list[dict]


class ForecastResponse(BaseModel):
    generated_at: str
    last_actual_hour: str | None
    models: dict[str, ModelForecast]


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.get("/models")
def models() -> dict:
    return {"models": [
        {"name": n, "stage": s} for n, s in settings.forecast.models
    ]}


@app.get("/forecast", response_model=ForecastResponse)
def forecast() -> ForecastResponse:
    """Run a fresh multi-model forecast and return it."""
    engine = ForecastEngine()
    result = engine.forecast()
    return ForecastResponse(
        generated_at=result["generated_at"],
        last_actual_hour=result["last_actual_hour"],
        models={
            name: ModelForecast(
                model_stage=m["model_stage"],
                hourly=m["hourly"],
                quarter_daily=m["quarter_daily"],
            )
            for name, m in result["models"].items()
        },
    )

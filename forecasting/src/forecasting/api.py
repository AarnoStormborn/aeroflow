"""
On-demand forecast API (FastAPI).

Serves a fresh forecast over HTTP so it can be queried on demand (vs. the
hourly scheduled run). Exposed on Modal as a web endpoint.

Every route requires `Authorization: Bearer <AEROFLOW_API_KEY>`. This endpoint
is publicly reachable and `GET /forecast` performs real work, so without a key
anyone who has the URL can trigger unlimited forecast runs (a cost/DoS vector).
It also exposes the registered model names and versions.

Endpoints:
    GET /forecast          -> run a fresh forecast now (all configured models)
    GET /health            -> liveness
    GET /models            -> which models are configured
"""

import secrets
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel
from src.forecasting.config import settings
from src.forecasting.models.forecaster import ForecastEngine


def require_api_key(authorization: str | None = Header(default=None)) -> None:
    """Validate `Authorization: Bearer <AEROFLOW_API_KEY>`.

    Fails closed: if the key is not configured we reject rather than allow, so a
    missing secret can never silently turn the endpoint back into an open one.
    """
    expected = settings.api_key
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Server misconfigured: AEROFLOW_API_KEY is not set",
        )

    scheme, _, token = (authorization or "").partition(" ")
    # compare_digest to avoid leaking the key through response timing
    if scheme.lower() != "bearer" or not token or not secrets.compare_digest(token, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )


app = FastAPI(title="Aeroflow Forecasting API", dependencies=[Depends(require_api_key)])


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

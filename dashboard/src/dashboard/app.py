"""
Dashboard FastAPI app.

Serves the dashboard HTML + JSON API endpoints. All data reads go through
the S3Store aggregation layer (cached). The MLflow tracking URI is used
server-side only and is never exposed to the page.
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from src.dashboard import data as data_layer

app = FastAPI(title="Aeroflow Dashboard")

_STATIC = Path(__file__).parent / "static"


@app.get("/")
def index():
    return FileResponse(_STATIC / "index.html")


@app.get("/api/live")
def api_live():
    return data_layer.live_snapshot()


@app.get("/api/patterns")
def api_patterns():
    return data_layer.patterns_snapshot()


@app.get("/api/forecasts")
def api_forecasts():
    return data_layer.forecasts_snapshot()


@app.get("/api/health")
def api_health():
    return data_layer.health_snapshot()


app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")

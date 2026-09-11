"""
Dashboard FastAPI app.

Serves the dashboard HTML + JSON API endpoints. All data reads go through
the S3Store aggregation layer (cached). The MLflow tracking URI is used
server-side only and is never exposed to the page.
"""

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from src.dashboard import data as data_layer

app = FastAPI(title="Aeroflow Dashboard")

_STATIC = Path(__file__).parent / "static"


@app.middleware("http")
async def api_cache_headers(request: Request, call_next):
    """Let the browser reuse API responses briefly.

    Underlying data only changes when ingestion runs (every 15 min), so serving
    a response from the browser cache for a minute is safe and saves a Modal
    invocation per repeat view.
    """
    response = await call_next(request)
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "public, max-age=60"
    return response


@app.get("/")
def index():
    resp = FileResponse(_STATIC / "index.html")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return resp


# Serve static assets without long-lived caching so redeploys are picked up
# quickly (cache-bust via ?v= handles the actual freshness).
app.mount(
    "/static",
    StaticFiles(directory=str(_STATIC)),
    name="static",
)


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

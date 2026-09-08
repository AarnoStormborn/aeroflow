"""
Dashboard FastAPI app.

Serves the dashboard HTML + JSON API endpoints. All data reads go through
the S3Store aggregation layer (cached). The MLflow tracking URI is used
server-side only and is never exposed to the page.
"""

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from src.dashboard import data as data_layer
from src.dashboard.config import settings

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


@app.get("/api/reports")
def api_reports():
    store = data_layer.S3Store()
    files = store.report_files()
    out = []
    for f in files[:40]:
        # derive date from key: reports/daily/YYYY/MM/flight_report_YYYY-MM-DD.pdf
        parts = f["key"].split("/")
        try:
            dstr = parts[-1].split("flight_report_")[1].split(".pdf")[0]
        except IndexError:
            dstr = ""
        out.append({"date": dstr, "key": f["key"], "size": f["size"]})
    return {"reports": out}


@app.get("/api/reports/file")
def report_file(key: str = ""):
    """Stream a report PDF from S3 (server-side; no public bucket)."""
    store = data_layer.S3Store()
    try:
        resp = store._client.get_object(Bucket=settings.s3.bucket_name, Key=key)
        body = resp["Body"].read()
    except Exception:
        raise HTTPException(status_code=404, detail="report not found") from None
    return Response(content=body, media_type="application/pdf")


app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")

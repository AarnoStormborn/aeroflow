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


# Content-Security-Policy. The page is a self-contained dashboard: Chart.js is
# self-hosted, the only external origin is Google Fonts, and there is no user
# input. So scripts can be locked to 'self' with no 'unsafe-inline' (the theme
# bootstrap lives in /static/theme.js precisely to make that possible) and the
# only permitted style origin is the fonts stylesheet.
_CSP = "; ".join([
    "default-src 'self'",
    "base-uri 'none'",
    "object-src 'none'",
    # clickjacking protection (X-Frame-Options below covers older browsers)
    "frame-ancestors 'none'",
    "form-action 'none'",
    "script-src 'self'",
    "style-src 'self' https://fonts.googleapis.com",
    "font-src https://fonts.gstatic.com",
    "img-src 'self' data:",
    "connect-src 'self'",
])

_SECURITY_HEADERS = {
    "Content-Security-Policy": _CSP,
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Permissions-Policy": (
        "geolocation=(), microphone=(), camera=(), payment=(), usb=(), "
        "magnetometer=(), gyroscope=(), accelerometer=()"
    ),
    # No includeSubDomains: this is a Modal hostname, and we should not pin
    # sibling *.modal.run hosts to HTTPS on a visitor's browser.
    "Strict-Transport-Security": "max-age=31536000",
    "Cross-Origin-Opener-Policy": "same-origin",
}


@app.middleware("http")
async def api_cache_headers(request: Request, call_next):
    """Add security headers, and let the browser reuse API responses briefly.

    Underlying data only changes when ingestion runs (every 15 min), so serving
    a response from the browser cache for a minute is safe and saves a Modal
    invocation per repeat view.
    """
    response = await call_next(request)
    for header, value in _SECURITY_HEADERS.items():
        response.headers[header] = value
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

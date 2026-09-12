"""
Aeroflow — single Modal app with multiple components.

Components:
  - run_feature      : scheduled feature engineering (daily)
  - run_report       : scheduled daily report (daily)
  - run_training     : scheduled model training (every 3 days)
  - run_forecast     : hourly forecast (1h + 6h recursive) → S3
  - run_eval         : daily forecast-vs-actual evaluation
  - mlflow_ui        : MLflow tracking server as a web_server

Note: Ingestion (OpenSky → S3) runs on a Raspberry Pi via systemd, not
on Modal — Modal's deployed-function egress is blocked by OpenSky.

Deploy:
    modal deploy modal_app.py

Secrets (Modal Secret named "aeroflow-env"):
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET_NAME
    OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET (only if re-adding ingestion)
    DISCORD_WEBHOOK_URL, DISCORD_ENABLED

Volume:
    aeroflow-data  (mounted at /data — holds mlflow.db)
"""

from __future__ import annotations

import os

import modal

# ---------------------------------------------------------------------------
# App / image / volume / secrets
# ---------------------------------------------------------------------------

app = modal.App("aeroflow")

# ---------------------------------------------------------------------------
# Images — one per service to keep builds lean
# ---------------------------------------------------------------------------

def _ignore(p) -> bool:
    return (
        ".venv" in str(p)
        or "__pycache__" in str(p)
        or ".git" in str(p)
        or ".venv-pi" in str(p)
    )

# Feature-engineering image (polars + plotting for reports)
feature_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "polars>=1.0.0",
        "boto3>=1.34.0",
        "python-dotenv",
        "loguru>=0.7.0",
        "httpx>=0.27.0",
        "pydantic>=2.0.0",
        "pydantic-settings>=2.0.0",
        "matplotlib>=3.8.0",
        "seaborn>=0.13.0",
        "reportlab>=4.0.0",
    )
    .add_local_dir("./feature-engineering", "/root/feature", copy=True, ignore=_ignore)
)

# Model-training image (sklearn + xgboost + mlflow)
# NOTE: keep this list in sync with model-training/pyproject.toml.
# A missing `xgboost` here broke the scheduled retrain with
# ModuleNotFoundError; tests/unit/test_modal_images.py guards the drift.
training_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "polars>=1.0.0",
        "boto3>=1.34.0",
        "python-dotenv",
        "loguru>=0.7.0",
        "pydantic>=2.0.0",
        "pydantic-settings>=2.0.0",
        "matplotlib>=3.8.0",
        "seaborn>=0.13.0",
        "scikit-learn>=1.4.0",
        "xgboost>=2.0.0",
        "mlflow==3.15.2",
        "anyio>=4.0.0",
    )
    .add_local_dir("./model-training", "/root/training", copy=True, ignore=_ignore)
)

# MLflow server image
mlflow_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "mlflow==3.15.2",
        # Required by `--app-name basic-auth`; without it the server refuses to
        # start (mlflow's auth app imports Flask-WTF for CSRF validation).
        "Flask-WTF<2",
        "boto3>=1.34.0",
        "psycopg2-binary>=2.9.0",
        "anyio>=4.0.0",
        "fastapi",
        "uvicorn",
    )
)

# Forecasting image (loads model from MLflow, predicts, writes S3)
forecast_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "polars>=1.0.0",
        "boto3>=1.34.0",
        "python-dotenv",
        "loguru>=0.7.0",
        "pydantic>=2.0.0",
        "pydantic-settings>=2.0.0",
        "scikit-learn>=1.4.0",
        "xgboost>=2.0.0",
        "mlflow==3.15.2",
        "anyio>=4.0.0",
        "matplotlib>=3.8.0",
        "httpx>=0.27.0",
        "fastapi",
        "uvicorn",
    )
    .add_local_dir("./forecasting", "/root/forecast", copy=True, ignore=_ignore)
)

# Dashboard image (FastAPI + static assets)
dashboard_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "polars>=1.0.0",
        "boto3>=1.34.0",
        "python-dotenv",
        "loguru>=0.7.0",
        "pydantic>=2.0.0",
        "pydantic-settings>=2.0.0",
        "fastapi",
        "uvicorn",
    )
    .add_local_dir("./dashboard", "/root/dashboard", copy=True, ignore=_ignore)
)

secrets = modal.Secret.from_name("aeroflow-env", required_keys=[
    "OPENSKY_CLIENT_ID",
    "OPENSKY_CLIENT_SECRET",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_S3_BUCKET_NAME",
])

# MLflow basic-auth credentials. Kept in their own secret so rotating the MLflow
# password can never disturb the main env secret (and vice versa). Needed by the
# server itself, and by any client that talks to it over HTTP (the forecasting
# service loads models from the registry; training writes to the SQLite store
# directly and so does not need these).
mlflow_auth = modal.Secret.from_name("mlflow-auth", required_keys=[
    "MLFLOW_TRACKING_USERNAME",
    "MLFLOW_TRACKING_PASSWORD",
    # MLflow's auth app refuses to start without a static Flask secret key for
    # CSRF protection. It must be STABLE across restarts, otherwise sessions and
    # CSRF tokens are invalidated on every cold start.
    "MLFLOW_FLASK_SERVER_SECRET_KEY",
])

# Shared secret for the public forecasting HTTP API. Its own secret so the key is
# only handed to the one function that checks it (least privilege) and can be
# rotated without touching anything else.
api_auth = modal.Secret.from_name("aeroflow-api-auth", required_keys=[
    "AEROFLOW_API_KEY",
])

volume = modal.Volume.from_name("aeroflow-data", create_if_missing=True)
VOLUME_MOUNT = "/data"

# Where the sources were baked into images (see add_local_dir above)
FEATURE_ROOT = "/root/feature"
TRAINING_ROOT = "/root/training"
FORECAST_ROOT = "/root/forecast"
DASHBOARD_ROOT = "/root/dashboard"


def _set_env_defaults() -> None:
    """Ensure env vars the code expects are set, with sensible defaults."""
    os.environ.setdefault("AWS_S3_BUCKET_NAME", "flights-forecasting")
    os.environ.setdefault("AWS_REGION", "ap-south-1")
    os.environ.setdefault("DISCORD_ENABLED", "true")
    os.environ.setdefault("FE_S3_PREFIX", "raw/flights/states")
    os.environ.setdefault("MLFLOW_TRACKING_URI", f"sqlite:///{VOLUME_MOUNT}/mlflow.db")
    # Forecast loads the registered model from the Modal MLflow server (reachable
    # from both local dev and Modal functions); model artifacts live in S3.
    os.environ.setdefault(
        "FORECAST_MLFLOW_TRACKING_URI",
        "https://harshsingh90220--aeroflow-mlflow-ui.modal.run",
    )
    os.environ.setdefault(
        "MLFLOW_ARTIFACT_ROOT",
        f"s3://{os.environ.get('AWS_S3_BUCKET_NAME', 'flights-forecasting')}/mlflow",
    )


def _syspath(*roots: str) -> None:
    """Add source roots to sys.path so `from src...` imports resolve."""
    import sys

    for r in roots:
        sys.path.insert(0, r)
        sys.path.insert(0, os.path.join(r, "src"))


# ---------------------------------------------------------------------------
# 1. Feature engineering (daily) — reads raw S3, writes features S3
# ---------------------------------------------------------------------------


@app.function(
    image=feature_image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("0 2 * * *"),
    timeout=1800,
)
def run_feature(target_date: str | None = None) -> dict:
    """Feature engineering (daily 2 AM UTC). Processes yesterday by default.

    Args:
        target_date: YYYY-MM-DD to process (defaults to yesterday).
    """
    _set_env_defaults()
    _syspath(FEATURE_ROOT)

    from datetime import date, timedelta

    from src.pipeline.run import run_feature_pipeline

    if target_date:
        d = date.fromisoformat(target_date)
    else:
        d = date.today() - timedelta(days=1)

    s3_url = run_feature_pipeline(d)
    # Self-healing: also fill any raw days that lack features (missed runs)
    backfilled = 0
    try:
        from src.pipeline.backfill import backfill

        results = backfill()
        backfilled = len(results)
    except Exception as e:
        print(f"Backfill failed: {e}")
    return {"component": "feature", "status": "ok", "date": str(d),
            "s3_url": s3_url, "backfilled": backfilled}


# ---------------------------------------------------------------------------
# 2. Daily report (daily) — builds PDF from features, uploads to S3
# ---------------------------------------------------------------------------


@app.function(
    image=feature_image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("0 3 * * *"),
    timeout=1800,
)
def run_report(target_date: str | None = None) -> dict:
    """Daily report (daily 3 AM UTC). Generates PDF for yesterday by default.

    Args:
        target_date: YYYY-MM-DD to process (defaults to yesterday).
    """
    _set_env_defaults()
    _syspath(FEATURE_ROOT)

    from datetime import date, timedelta

    from src.features.daily_report import generate_daily_report

    if target_date:
        d = date.fromisoformat(target_date)
    else:
        d = date.today() - timedelta(days=1)

    generate_daily_report(d)
    return {"component": "report", "status": "ok", "date": str(d)}


# ---------------------------------------------------------------------------
# 3. Model training (every 3 days) — trains on features, logs to MLflow
# ---------------------------------------------------------------------------


@app.function(
    image=training_image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("0 2 */3 * *"),
    timeout=3600,
)
def run_training(end_date: str | None = None) -> dict:
    """Retrain production hourly model (every 3 days at 2 AM UTC).

    Trains the tuned XGBoost on the freshest feature block and registers
    a new Production version of flight-traffic-hourly. Skips (gracefully)
    if insufficient fresh data is available.

    Args:
        end_date: YYYY-MM-DD last day of the training window (default yesterday).
    """
    _set_env_defaults()
    _syspath(TRAINING_ROOT)

    from datetime import date

    from src.training.train_production import train_production

    d = date.fromisoformat(end_date) if end_date else None
    result = train_production(d)
    return {"component": "training", "status": result.get("status", "?")}


# ---------------------------------------------------------------------------
# 4. Forecasting (hourly: 1h + 6h recursive forecasts; eval daily)
# ---------------------------------------------------------------------------


@app.function(
    image=forecast_image,
    secrets=[secrets, mlflow_auth],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("15 * * * *"),  # every hour at :15
    timeout=600,
)
def run_forecast() -> dict:
    """Run hourly forecast: next-hour + next-6h (recursive), store to S3."""
    _set_env_defaults()
    _syspath(FORECAST_ROOT)

    from src.forecasting.models.forecaster import run_forecast as _rf

    result = _rf()
    return {"component": "forecast", "status": "ok", "generated_at": result["generated_at"]}


@app.function(
    image=dashboard_image,
    secrets=[secrets],
    # Scale to zero promptly. Requests are cheap now that aggregates are served
    # from a published payload, so an idle container would bill for pure waiting.
    scaledown_window=30,
)
# The page loads 4 endpoints in parallel. Modal defaults to one input per
# container, so a single page view would boot 4 containers, each paying the full
# polars/boto3 import cost. These handlers are I/O-bound, so let one warm
# container serve a whole page load instead.
@modal.concurrent(max_inputs=8)
@modal.asgi_app()
def dashboard_app():
    """Live traffic + forecasting dashboard (web).

    NOTE: public by design. Does NOT expose the MLflow tracking UI URL —
    the MLflow URI is used server-side only.
    """
    _set_env_defaults()
    _syspath(DASHBOARD_ROOT)

    from src.dashboard.app import app as dash_app

    return dash_app


@app.function(
    image=forecast_image,
    secrets=[secrets, mlflow_auth, api_auth],
    volumes={VOLUME_MOUNT: volume},
    scaledown_window=30,
)
@modal.concurrent(max_inputs=4)
@modal.asgi_app()
def forecast_api():
    """On-demand forecasting API (GET /forecast, /health, /models)."""
    _set_env_defaults()
    _syspath(FORECAST_ROOT)

    from src.forecasting.api import app as fastapi_app

    return fastapi_app


@app.function(
    image=forecast_image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("30 1 * * *"),  # daily 01:30 UTC
    timeout=600,
)
def run_eval() -> dict:
    """Daily eval + Discord quality report (MAPE by horizon with graph)."""
    _set_env_defaults()
    _syspath(FORECAST_ROOT)

    # Evaluates forecasts vs actuals and posts the report+graph to Discord.
    from src.forecasting.models.quality_report import main as _report

    _report()
    return {"component": "eval", "status": "ok"}


# ---------------------------------------------------------------------------
# 5. MLflow tracking server
# ---------------------------------------------------------------------------


@app.function(
    image=mlflow_image,
    secrets=[secrets, mlflow_auth],
    volumes={VOLUME_MOUNT: volume},
)
@modal.concurrent(max_inputs=8)
@modal.web_server(port=5000, startup_timeout=120)
def mlflow_ui():
    """Expose MLflow tracking server on Modal, requiring basic auth.

    This endpoint is publicly reachable, so it MUST be authenticated: without
    auth anyone with the URL can read every run, metric and registered model —
    and WRITE to the registry (verified: anonymous callers could create and
    delete experiments and registered models). Since the forecasting service
    loads `models:/<name>/Production`, anonymous registry writes are a model-
    poisoning / RCE vector, not just an information leak.
    """
    import subprocess

    _set_env_defaults()

    db_path = os.environ.get("MLFLOW_BACKEND_STORE_URI") or os.path.join(VOLUME_MOUNT, "mlflow.db")
    artifact_root = os.environ.get("MLFLOW_ARTIFACT_ROOT") or (
        f"s3://{os.environ.get('AWS_S3_BUCKET_NAME', 'flights-forecasting')}/mlflow"
    )

    # MLflow falls back to its OWN bundled config when MLFLOW_AUTH_CONFIG_PATH
    # is unset, and that ships a well-known default admin password. So always
    # point it at a config we generate from the secret.
    auth_ini = "/tmp/basic_auth.ini"
    with open(auth_ini, "w") as f:
        f.write(
            "[mlflow]\n"
            "default_permission = READ\n"
            # Auth tables live in ephemeral storage on purpose: the admin user is
            # recreated from this config on every start, so the password always
            # matches the secret instead of going stale in a persisted auth DB.
            "database_uri = sqlite:////tmp/mlflow_auth.db\n"
            f"admin_username = {os.environ['MLFLOW_TRACKING_USERNAME']}\n"
            f"admin_password = {os.environ['MLFLOW_TRACKING_PASSWORD']}\n"
        )
    os.environ["MLFLOW_AUTH_CONFIG_PATH"] = auth_ini

    # Run mlflow server on 0.0.0.0:5000 with the Volume-backed SQLite store.
    # MLflow 3.5+ validates Host headers; Modal proxies requests with its own
    # hostname, so allow it (restrict to the Modal subdomain for production).
    cmd = [
        "mlflow", "server",
        "--backend-store-uri", f"sqlite:///{db_path}",
        "--default-artifact-root", artifact_root,
        "--host", "0.0.0.0",
        "--port", "5000",
        "--allowed-hosts", "*",
        "--app-name", "basic-auth",
    ]
    proc = subprocess.Popen(cmd)
    return proc


# ---------------------------------------------------------------------------
# Local entry (used by `modal serve` / direct runs)
# ---------------------------------------------------------------------------


@app.local_entrypoint()
def main():
    """Local entrypoint for `modal run` — run feature for yesterday as a smoke test."""
    print(run_feature.local())

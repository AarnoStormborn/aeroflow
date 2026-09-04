"""
Aeroflow — single Modal app with multiple components.

Components:
  - ingest_once      : scheduled ingestion (OpenSky → S3 raw + SQLite record)
  - run_feature      : scheduled feature engineering (stub)
  - run_report       : scheduled daily report (stub)
  - run_training     : scheduled model training (stub)
  - mlflow_ui        : MLflow tracking server as a web_server

Deploy:
    modal deploy modal_app.py

Secrets (Modal Secret named "aeroflow-env"):
    OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET_NAME
    DISCORD_WEBHOOK_URL, DISCORD_ENABLED
    DB_PATH=/data/ingestion.db

Volume:
    aeroflow-data  (mounted at /data — holds ingestion.db, mlflow.db)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

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

# Ingestion image (light — fetches OpenSky, writes S3)
ingestion_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "polars",
        "boto3",
        "httpx",
        "python-dotenv",
        "loguru>=0.7.3",
        "pydantic>=2.12.5",
        "pydantic-settings>=2.12.0",
        "apscheduler>=3.11.1",
    )
    .add_local_dir("./ingestion", "/root/ingestion", copy=True, ignore=_ignore)
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

# Model-training image (sklearn + mlflow)
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
        "mlflow==3.8.1",
    )
    .add_local_dir("./model-training", "/root/training", copy=True, ignore=_ignore)
)

# MLflow server image
mlflow_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("mlflow==3.8.1", "boto3>=1.34.0", "psycopg2-binary>=2.9.0")
)

secrets = modal.Secret.from_name("aeroflow-env", required_keys=[
    "OPENSKY_CLIENT_ID",
    "OPENSKY_CLIENT_SECRET",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_S3_BUCKET_NAME",
])

volume = modal.Volume.from_name("aeroflow-data", create_if_missing=True)
VOLUME_MOUNT = "/data"

# Where the sources were baked into images (see add_local_dir above)
INGESTION_ROOT = "/root/ingestion"
FEATURE_ROOT = "/root/feature"
TRAINING_ROOT = "/root/training"


def _set_env_defaults() -> None:
    """Ensure env vars the code expects are set, with sensible defaults."""
    os.environ.setdefault("AWS_S3_BUCKET_NAME", "flights-forecasting")
    os.environ.setdefault("AWS_REGION", "us-east-1")
    os.environ.setdefault("DB_PATH", os.path.join(VOLUME_MOUNT, "ingestion.db"))
    os.environ.setdefault("SCHEDULER_INTERVAL_SECONDS", "900")
    os.environ.setdefault("DISCORD_ENABLED", "true")
    os.environ.setdefault("FE_S3_PREFIX", "raw/flights/states")
    os.environ.setdefault("MLFLOW_TRACKING_URI", f"sqlite:///{VOLUME_MOUNT}/mlflow.db")
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
# 1. Scheduled ingestion
# ---------------------------------------------------------------------------


@app.function(
    image=ingestion_image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("*/15 * * * *"),
    timeout=300,
)
def ingest_once() -> dict:
    """Run a single ingestion cycle (OpenSky → S3 + SQLite)."""
    _set_env_defaults()
    _syspath(INGESTION_ROOT)

    from src.ingestion import run_ingestion

    result = run_ingestion()

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": result.status.value,
        "record_count": result.record_count,
        "s3_path": result.s3_path,
        "error_message": result.error_message,
    }


# ---------------------------------------------------------------------------
# 2. Feature engineering (daily) — reads raw S3, writes features S3
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
    return {"component": "feature", "status": "ok", "date": str(d), "s3_url": s3_url}


# ---------------------------------------------------------------------------
# 3. Daily report (daily) — builds PDF from features, uploads to S3
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
# 4. Model training (every 3 days) — trains on features, logs to MLflow
# ---------------------------------------------------------------------------


@app.function(
    image=training_image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("0 2 */3 * *"),
    timeout=3600,
)
def run_training(end_date: str | None = None) -> dict:
    """Model training (every 3 days at 2 AM UTC). Trains on rolling window
    ending yesterday by default. Logs to MLflow (web_server on Modal).

    Args:
        end_date: YYYY-MM-DD last day of the training window (default yesterday).
    """
    _set_env_defaults()
    _syspath(TRAINING_ROOT)

    from datetime import date, timedelta

    from src.training.train import train_model

    if end_date:
        d = date.fromisoformat(end_date)
    else:
        d = date.today() - timedelta(days=1)

    result = train_model(d)
    return {"component": "training", "status": "ok", "end_date": str(d), **result}


# ---------------------------------------------------------------------------
# 5. MLflow tracking server
# ---------------------------------------------------------------------------


@app.function(
    image=mlflow_image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
)
@modal.concurrent(max_inputs=1)
@modal.web_server(port=5000, startup_timeout=120)
def mlflow_ui():
    """Expose MLflow tracking server on Modal."""
    import subprocess

    _set_env_defaults()

    db_path = os.environ.get("MLFLOW_BACKEND_STORE_URI") or os.path.join(VOLUME_MOUNT, "mlflow.db")
    artifact_root = os.environ.get("MLFLOW_ARTIFACT_ROOT") or (
        f"s3://{os.environ.get('AWS_S3_BUCKET_NAME', 'flights-forecasting')}/mlflow"
    )

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
    ]
    proc = subprocess.Popen(cmd)
    return proc


# ---------------------------------------------------------------------------
# Local entry (used by `modal serve` / direct runs)
# ---------------------------------------------------------------------------


@app.local_entrypoint()
def main():
    """Run one ingestion locally, then exit (useful for testing)."""
    result = ingest_once.local()
    print(result)

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

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "polars",
        "boto3",
        "pyarrow",
        "httpx",
        "python-dotenv",
        "loguru>=0.7.3",
        "pydantic>=2.12.5",
        "pydantic-settings>=2.12.0",
        "pyyaml>=6.0.3",
        "apscheduler>=3.11.1",
        "mlflow==3.8.1",
        "psycopg2-binary>=2.9.0",
        "fastapi",
        "uvicorn",
    )
    # Bake the ingestion source into the image so containers can import it.
    .add_local_dir(
        "./ingestion",
        "/root/ingestion",
        copy=True,
        ignore=lambda p: ".venv" in str(p) or "__pycache__" in str(p) or ".git" in str(p),
    )
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

# Where the ingestion source was baked into the image (see add_local_dir above)
INGESTION_ROOT = "/root/ingestion"


def _set_env_defaults() -> None:
    """Ensure env vars the code expects are set, with sensible defaults."""
    os.environ.setdefault("AWS_S3_BUCKET_NAME", "flights-forecasting")
    os.environ.setdefault("AWS_REGION", "us-east-1")
    os.environ.setdefault("DB_PATH", os.path.join(VOLUME_MOUNT, "ingestion.db"))
    os.environ.setdefault("SCHEDULER_INTERVAL_SECONDS", "900")
    os.environ.setdefault("DISCORD_ENABLED", "true")


# ---------------------------------------------------------------------------
# 1. Scheduled ingestion
# ---------------------------------------------------------------------------


@app.function(
    image=image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("*/15 * * * *"),
    timeout=300,
)
def ingest_once() -> dict:
    """Run a single ingestion cycle (OpenSky → S3 + SQLite)."""
    _set_env_defaults()

    import sys

    sys.path.insert(0, INGESTION_ROOT)
    sys.path.insert(0, os.path.join(INGESTION_ROOT, "src"))

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
# 2/3/4. Feature / report / training (stubs — wired in later iterations)
# ---------------------------------------------------------------------------


@app.function(
    image=image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("0 2 * * *"),
    timeout=1800,
)
def run_feature() -> dict:
    """Feature engineering (daily 2 AM). TODO: wire src.pipeline.run."""
    return {"component": "feature", "status": "stub", "at": datetime.now(timezone.utc).isoformat()}


@app.function(
    image=image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("0 3 * * *"),
    timeout=1800,
)
def run_report() -> dict:
    """Daily report (daily 3 AM). TODO: wire src.features.daily_report."""
    return {"component": "report", "status": "stub", "at": datetime.now(timezone.utc).isoformat()}


@app.function(
    image=image,
    secrets=[secrets],
    volumes={VOLUME_MOUNT: volume},
    schedule=modal.Cron("0 2 */3 * *"),
    timeout=3600,
)
def run_training() -> dict:
    """Model training (every 3 days at 2 AM). TODO: wire src.training.train."""
    return {"component": "training", "status": "stub", "at": datetime.now(timezone.utc).isoformat()}


# ---------------------------------------------------------------------------
# 5. MLflow tracking server
# ---------------------------------------------------------------------------


@app.function(
    image=image,
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
    artifact_root = os.environ.get("MLFLOW_ARTIFACT_ROOT") or f"s3://{os.environ.get('AWS_S3_BUCKET_NAME', 'flights-forecasting')}/mlflow"

    # Run mlflow server on 0.0.0.0:5000 with the Volume-backed SQLite store
    cmd = [
        "mlflow", "server",
        "--backend-store-uri", f"sqlite:///{db_path}",
        "--default-artifact-root", artifact_root,
        "--host", "0.0.0.0",
        "--port", "5000",
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

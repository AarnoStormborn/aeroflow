# Aeroflow on Modal

Single Modal app hosting all pipeline components serverlessly.

> The repo is a **uv workspace**: `pyproject.toml` at root ties together
> `ingestion/`, `feature-engineering/`, `model-training/`. One lockfile
> (`uv.lock`) at root. Run `uv sync` at root to install everything, or
> `uv run --package <service> --directory <service> pytest` per service.

## Components

| Function | Schedule | Purpose |
|---|---|---|
| `ingest_once` | `*/15 * * * *` | OpenSky → S3 raw + SQLite record (Volume) |
| `run_feature` | `0 2 * * *` | Feature engineering (stub) |
| `run_report` | `0 3 * * *` | Daily PDF report (stub) |
| `run_training` | `0 2 */3 * *` | Model training (stub) |
| `mlflow_ui` | — (web server) | MLflow tracking UI at `:5000` |

## Setup

1. **Install Modal CLI** and log in:
   ```bash
   pip install modal
   modal token new   # browser auth
   ```

2. **Create the secrets** (Modal dashboard → Secrets → Create → or CLI):
   ```bash
   modal secret create aeroflow-env \
     OPENSKY_CLIENT_ID=... OPENSKY_CLIENT_SECRET=... \
     AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
     AWS_S3_BUCKET_NAME=flights-forecasting \
     DISCORD_WEBHOOK_URL=... DISCORD_ENABLED=true

   # Required: the MLflow endpoint is public, so it must be authenticated.
   # Generate strong values and keep them out of the repo:
   #   python -c "import secrets; print(secrets.token_urlsafe(32))"
   modal secret create mlflow-auth \
     MLFLOW_TRACKING_USERNAME=admin \
     MLFLOW_TRACKING_PASSWORD=<strong-password> \
     MLFLOW_FLASK_SERVER_SECRET_KEY=<random-64-hex>
   ```

   `mlflow-auth` is a separate secret so rotating the MLflow password cannot
   disturb `aeroflow-env`. `MLFLOW_FLASK_SERVER_SECRET_KEY` must be **stable**:
   MLflow's auth app refuses to start without it, and changing it invalidates
   sessions and CSRF tokens.

3. **Create the volume** (auto-created on first deploy, or):
   ```bash
   modal volume create aeroflow-data
   ```

4. **Deploy**:
   ```bash
   modal deploy modal_app.py
   ```

## Local testing

```bash
modal run modal_app.py        # runs ingest_once once, prints result
modal serve modal_app.py      # live-reload dev server (web endpoints get temp URLs)
```

## Notes

- **DB_PATH** defaults to `/data/ingestion.db` (Volume). Set explicitly in the secret if needed.
- **MLflow** uses a Volume-backed SQLite store + S3 artifact root (`s3://flights-forecasting/mlflow`).
- **MLflow requires basic auth.** `mlflow_ui` is a public URL, so anonymous
  access exposes every run and model — and, verified in an audit, allowed
  anonymous *writes* to the model registry. Because the forecaster loads
  `models:/<name>/Production`, that is a model-poisoning path, not just a leak.
  The server runs with `--app-name basic-auth` and reads credentials from a
  config generated out of `mlflow-auth`. MLflow falls back to its own bundled
  config (with a well-known default password) if `MLFLOW_AUTH_CONFIG_PATH` is
  unset, so it is always set explicitly.
- **HTTP clients need the same credentials.** The forecasting service loads
  models over HTTP, so `run_forecast` and `forecast_api` include `mlflow-auth`.
  Training writes to the SQLite store directly and needs none.
- The auth database lives in `/tmp` deliberately: the admin user is recreated
  from the secret on each start, so the password cannot go stale in a persisted
  auth DB.
- The ingestion source (`ingestion/`) is baked into the image via `add_local_dir`, so redeploy picks up code changes.
- Schedules are UTC (Modal cron is UTC). Adjust if you want local-time runs.

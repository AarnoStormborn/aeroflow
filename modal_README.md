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

   # Required: the forecasting HTTP API is public and /forecast does real work.
   modal secret create aeroflow-api-auth \
     AEROFLOW_API_KEY=<strong-random-key>
   ```

   Each is a separate secret so a compromise or rotation of one cannot affect
   the others, and so no function receives credentials it does not use.

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
- **The forecasting HTTP API requires an API key.** `forecast_api` is a public
  URL and `GET /forecast` performs real work, so unauthenticated callers could
  trigger unlimited forecast runs and read the model names/versions. Every route
  requires `Authorization: Bearer <AEROFLOW_API_KEY>` (from
  `aeroflow-api-auth`).
  - It **fails closed**: if `AEROFLOW_API_KEY` is unset the API returns 503
    rather than allowing anonymous access, so a missing secret cannot silently
    reopen it. Covered by tests in
    `forecasting/tests/unit/test_api_auth.py`.
  - Comparison uses `secrets.compare_digest` to avoid leaking the key via
    response timing.
  - The hourly **scheduled** forecast (`run_forecast`) does not go through HTTP
    and needs no key.
- **The scheduled retrain does not auto-promote.** `train_production` scores the
  candidate and the incumbent Production model on the **same** validation split
  and promotes only if the candidate wins by `MIN_IMPROVEMENT` (default 5%
  relative). It previously promoted unconditionally, which is unsafe here: a
  backtest on held-out days scored retrains 3-5 points worse than v4
  (MAPE 20.0-21.9% vs 16.9%), so an unguarded retrain would have degraded
  production. The margin also prevents churn, since on ~150 samples a 0.5pp
  "win" is noise. `should_promote()` is pure and unit-tested.
  - A declined retrain is still registered (stage `None`) so it can be
    inspected, and the Modal run reports `decision: kept_incumbent` with the
    reason.
  - If a Production version exists but cannot be scored, the guard keeps it
    rather than promoting blind.
  - Promotion passes `archive_existing_versions=True` so exactly one version
    stays in Production. Without it, MLflow leaves every previously-promoted
    version in the stage and `models:/<name>/Production` would silently fall
    back to an older model if the newest were archived.
- The **dashboard stays public by design** and cannot use header auth: a browser
  cannot attach a custom `Authorization` header to a page navigation, and any
  key shipped to the client would be readable by everyone. Its `/api/*` routes
  are read-only and served from the published S3 payload cache.
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

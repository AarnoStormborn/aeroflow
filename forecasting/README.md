# Forecasting Service

Flight traffic forecasting using the registered MLflow production model.

## What it does

Produces two forecast horizons via **recursive prediction**:
- **Hourly (1h)** — next hour's flight count (the model's natural 1-step prediction)
- **Quarter-daily (6h)** — the next 6 hours, computed by feeding each
  prediction back as `lag_1h` for the following hour

Forecasts are written to S3 (`forecasts/hourly/...`) so they can be
compared against actuals by the evaluation job.

## Components

- `src/forecasting/data/loader.py` — loads recent raw flight data from S3,
  aggregates to hourly counts, builds model feature vectors
- `src/forecasting/models/forecaster.py` — recursive forecast engine that
  loads the production model from MLflow and runs + persists forecasts

## Usage

```bash
uv sync                          # from forecasting/
uv run python -m src.forecasting.models.forecaster
```

Environment (via repo root `.env`):
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_S3_BUCKET_NAME`
- `FORECAST_MLFLOW_TRACKING_URI` (defaults to the Modal MLflow server)
- Model: `flight-traffic-hourly` @ `Production` (retrained every 3 days)

The service serves a **single** model. An A/B period ran `flight-traffic-forecaster`
(Dec–Jan) alongside `flight-traffic-hourly` (current regime); the former was
retired after it lost decisively (paired h=1 MAPE 204% vs 69%, losing 69/69
shared target-hours). Every forecast records the producing `model_version`.

## Authentication

`GET /forecast`, `/models` and `/health` all require a bearer token:

```bash
curl -H "Authorization: Bearer $AEROFLOW_API_KEY" https://<forecast-api>/forecast
```

The key comes from the `AEROFLOW_API_KEY` env var (Modal secret
`aeroflow-api-auth`). The endpoint is publicly reachable and `/forecast` does
real work (model load + S3 reads), so the key is what stops anonymous callers
triggering unlimited runs.

Auth **fails closed**: with `AEROFLOW_API_KEY` unset every route returns 503
instead of falling open. Comparison is constant-time.

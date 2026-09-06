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
- Model: `flight-traffic-forecaster` @ `Production` stage

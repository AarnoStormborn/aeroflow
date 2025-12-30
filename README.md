# Flights Forecasting Services

Monorepo containing microservices for flight traffic forecasting pipeline.

## Architecture

```
┌─────────────────┐     ┌─────────────────────┐     ┌─────────────────┐
│   Ingestion     │────▶│ Feature Engineering │────▶│ Model Training  │
│   (10 min)      │     │      (Daily)        │     │   (3 Days)      │
└────────┬────────┘     └──────────┬──────────┘     └────────┬────────┘
         │                         │                         │
         ▼                         ▼                         ▼
    S3: raw/               S3: features/              MLflow + S3
```

## Projects

| Project | Description | Schedule |
|---------|-------------|----------|
| [`ingestion/`](./ingestion/) | Fetch flight data from OpenSky API → S3 | Every 10 min |
| [`feature-engineering/`](./feature-engineering/) | Feature pipeline + daily reports | Daily |
| [`model-training/`](./model-training/) | Train models with MLflow | Every 3 days |

---

## Ingestion Service

Fetches real-time flight state vectors from OpenSky Network API and stores as Parquet in S3.

```bash
cd ingestion
uv sync
./ingestion_script.sh  # Runs scheduled fetch
```

---

## Feature Engineering Service

### Daily Feature Pipeline

Creates hourly aggregated features for model training:

| Feature | Description |
|---------|-------------|
| `hour_of_day` | Hour (0-23) |
| `day_of_week` | Weekday (0=Mon, 6=Sun) |
| `is_weekend` | Boolean flag |
| `lag_1h` | Flight count 1 hour ago |
| `lag_24h` | Flight count 24 hours ago |
| `rolling_mean_6h` | 6-hour rolling average |

```bash
cd feature-engineering
uv sync

# Generate features for a date
uv run python -m src.pipeline.run --date 2025-12-28

# Run full daily pipeline (features + report)
./daily_report_script.sh --date 2025-12-28
```

### Output
- **Features**: `s3://flights-forecasting/features/hourly/year=YYYY/month=MM/features_YYYY-MM-DD.parquet`
- **Reports**: `s3://flights-forecasting/reports/daily/`

---

## Model Training Service

Trains Linear Regression models on 14-day rolling window of features, logging everything to MLflow.

### Features Used
```python
["hour_of_day", "day_of_week", "is_weekend", "lag_1h", "lag_24h", "rolling_mean_6h"]
```

### Usage

```bash
cd model-training
uv sync

# Train model
uv run python -m src.training.train --end-date 2025-12-28

# Or use script
./train_script.sh --end-date 2025-12-28
```

### MLflow Server (Docker)

```bash
cd model-training

# Start MLflow UI
docker-compose up -d --build

# View logs
docker-compose logs -f mlflow
```

Access MLflow UI at **http://localhost:5000**

### MLflow Artifacts
- **Metrics**: MAE, MAPE, R² (train/test)
- **Plots**: Forecast, residuals, feature importance
- **Model**: Registered as `flight-traffic-forecaster`

---

## Cron Schedule

```bash
# Ingestion - Every 10 minutes
*/10 * * * * cd /path/to/ingestion && ./ingestion_script.sh

# Feature Engineering - Daily at 2 AM
0 2 * * * cd /path/to/feature-engineering && ./daily_report_script.sh

# Model Training - Every 3 days at 3 AM
0 3 */3 * * cd /path/to/model-training && ./train_script.sh
```

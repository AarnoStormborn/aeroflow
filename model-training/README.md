# Model Training Pipeline

Model training service for flight traffic forecasting with MLflow integration.

## Setup

```bash
cd model-training
cp .env.example .env
# Edit .env with your credentials
uv sync
```

## Usage

```bash
# Train with default settings (yesterday as end date, 14-day window)
uv run python -m src.training.train

# Train with specific end date
uv run python -m src.training.train --end-date 2025-12-28

# Or use the shell script
./train_script.sh --end-date 2025-12-28
```

## Cron Setup (Every 3 Days)

```bash
crontab -e
# Add (runs at 3 AM every 3rd day):
0 3 */3 * * cd /path/to/services/model-training && ./train_script.sh >> logs/train.log 2>&1
```

## MLflow

View experiments:
```bash
mlflow ui --backend-store-uri sqlite:///mlflow.db
# Open http://localhost:5000
```

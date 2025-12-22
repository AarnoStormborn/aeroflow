# Flights Forecasting Services

Monorepo containing microservices for flight traffic forecasting.

## Projects

| Project | Description |
|---------|-------------|
| [`ingestion/`](./ingestion/) | Data ingestion from OpenSky API → S3 |
| [`feature-engineering/`](./feature-engineering/) | Feature engineering pipeline |

## Setup

Each project is managed independently with its own `pyproject.toml` and virtual environment.

```bash
# Ingestion service
cd ingestion
uv sync
uv run python main.py --run-once

# Feature engineering
cd feature-engineering
uv sync
uv run python main.py
```

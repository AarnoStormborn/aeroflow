# Dashboard

Live traffic + forecasting dashboard for Aeroflow.

Reads data from S3 (raw flight states, hourly features, forecasts) and
serves a public web dashboard via FastAPI (deployed on Modal).

## Endpoints

- `GET /` — dashboard page
- `GET /api/live` — current aircraft, today vs yesterday curves
- `GET /api/patterns` — hour-of-day profile, weekday pattern, anomalies
- `GET /api/forecasts` — model comparison, actual-vs-predicted, MAPE
- `GET /api/health` — data freshness, coverage gaps
- `GET /api/reports` — list of daily PDF reports

## Local dev

```bash
uv sync && uv run uvicorn src.dashboard.app:app --reload --port 8000
```

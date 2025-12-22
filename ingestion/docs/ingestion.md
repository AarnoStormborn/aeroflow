# Ingestion Pipeline

## Overview

The ingestion pipeline fetches real-time flight state vectors from the OpenSky Network API, transforms them into Parquet format, and stores them in AWS S3 for downstream ML processing.

---

## First Principles: Why This Architecture?

### The Core Problem

We need flight traffic data for forecasting. This requires:
1. **Continuous data collection** - Air traffic is time-series data; we need regular snapshots
2. **Efficient storage** - Raw JSON is inefficient for ML workloads
3. **Reliability tracking** - Know when collection succeeds or fails
4. **Scalability** - Handle growing data volumes over time

### Design Decisions

#### 1. Polling vs Streaming

**Question**: Should we poll the API or set up real-time streaming?

**Answer**: Polling at fixed intervals.

*Reasoning*:
- OpenSky API is pull-based (no webhooks/streaming)
- 10-minute intervals capture sufficient temporal resolution
- Simpler to implement, monitor, and debug
- No need for complex event-driven infrastructure

#### 2. Parquet vs CSV/JSON

**Question**: What storage format should we use?

**Answer**: Apache Parquet.

*Reasoning*:
- **Columnar storage** - Analytics/ML often access specific columns, not full rows
- **Compression** - Snappy compression reduces storage costs by 60-80%
- **Schema enforcement** - Catches data quality issues early
- **Native Polars/Pandas support** - Zero-friction integration with ML tools

#### 3. S3 vs Database for Raw Data

**Question**: Store in a database or object storage?

**Answer**: S3 for data, SQLite for metadata.

*Reasoning*:
- S3 is optimized for large files and time-partitioned data
- SQLite tracks ingestion status (lightweight, no server needed)
- Separation of concerns: data plane (S3) vs control plane (SQLite)

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   OpenSky API   │────▶│  Ingestion Job  │────▶│    AWS S3       │
│  /states/all    │     │                 │     │  (Parquet)      │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │    SQLite DB    │
                        │ (Run Tracking)  │
                        └─────────────────┘
```

---

## Components

### 1. OpenSky Client (`src/ingestion/components/client.py`)

Handles API communication with:
- **Bounding box filtering** - Fetch only Mumbai airspace (configurable)
- **Error classification** - Distinguishes rate limits, timeouts, connection errors
- **Timeout handling** - 30-second timeout to prevent hanging

```python
client = OpenSkyClient()
response = client.get_states(bounding_box=(18.0, 71.5, 20.0, 74.0))
```

### 2. S3 Uploader (`src/ingestion/components/s3_uploader.py`)

Converts API response to Parquet and uploads:
- **Schema enforcement** - 19 typed columns for state vectors
- **Time partitioning** - `year=YYYY/month=MM/day=DD/` prefix structure
- **Snappy compression** - Efficient storage

```python
uploader = S3Uploader()
s3_path, count = uploader.upload_states(response, timestamp)
# s3://bucket/raw/flights/states/year=2025/month=12/day=18/20251218_010000.parquet
```

### 3. Ingestion Repository (`src/ingestion/db/models.py`)

SQLite-based tracking of each ingestion run:
- **Status tracking** - PENDING → SUCCESS or FAILED
- **Error messages** - Categorized with `[CATEGORY] message` format
- **Metrics** - Record count, S3 path, timestamps

### 4. Ingestion Job (`src/ingestion/jobs/ingestion_job.py`)

Orchestrates the pipeline:

```
1. Create PENDING record in SQLite
2. Fetch states from OpenSky API
3. Convert to Parquet + Upload to S3
4. Update record to SUCCESS (or FAILED with error)
5. Send notifications (CloudWatch metrics + SNS alerts)
```

### 5. Scheduler (`src/ingestion/jobs/scheduler.py`)

APScheduler-based periodic execution:
- **Configurable interval** - `SCHEDULER_INTERVAL_SECONDS` (default: 60s)
- **Graceful shutdown** - Handles SIGINT/SIGTERM
- **Run-on-start** - Immediate first run, then scheduled

---

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `SCHEDULER_INTERVAL_SECONDS` | `60` | Polling interval |
| `OPENSKY_BBOX_LAMIN` | `18.0` | Bounding box min latitude |
| `OPENSKY_BBOX_LAMAX` | `20.0` | Bounding box max latitude |
| `OPENSKY_BBOX_LOMIN` | `71.5` | Bounding box min longitude |
| `OPENSKY_BBOX_LOMAX` | `74.0` | Bounding box max longitude |
| `AWS_S3_BUCKET_NAME` | - | S3 bucket for storage |
| `AWS_ACCESS_KEY_ID` | - | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | - | AWS credentials |

---

## Error Handling

All errors are categorized and captured:

| Category | Cause | Retry Strategy |
|----------|-------|----------------|
| `RATE_LIMIT` | API quota exceeded | Wait for `retry_after` |
| `API_TIMEOUT` | Request took too long | Automatic retry next interval |
| `API_CONNECTION` | Network unreachable | Check connectivity |
| `S3_UPLOAD` | Upload failed | Check AWS credentials |
| `PARQUET` | Data conversion error | Check data quality |
| `UNEXPECTED` | Unknown error | Review logs |

---

## Usage

```bash
# Run scheduler (continuous)
python main.py

# Single run (testing)
python main.py --run-once

# Custom interval
python main.py --interval 300  # 5 minutes
```

---

## Data Schema

Each Parquet file contains these columns:

| Column | Type | Description |
|--------|------|-------------|
| `icao24` | String | Aircraft transponder ID |
| `callsign` | String | Flight callsign |
| `origin_country` | String | Aircraft registration country |
| `longitude` | Float64 | Current longitude |
| `latitude` | Float64 | Current latitude |
| `baro_altitude` | Float64 | Barometric altitude (meters) |
| `velocity` | Float64 | Ground speed (m/s) |
| `true_track` | Float64 | Heading (degrees) |
| `vertical_rate` | Float64 | Climb/descent rate (m/s) |
| `on_ground` | Boolean | Aircraft on ground |
| `capture_time` | Int32 | Unix timestamp of capture |

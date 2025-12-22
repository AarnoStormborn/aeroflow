# Notifications System

## Overview

The notifications system provides observability into the ingestion pipeline through CloudWatch metrics (monitoring) and SNS alerts (alerting).

---

## First Principles: Why This Architecture?

### The Core Problem

Failures happen. We need to:
1. **Know when things break** - Immediate awareness of failures
2. **Understand patterns** - Historical view of success/failure rates
3. **Minimize noise** - Alert on actionable issues, not every hiccup
4. **Enable automation** - Allow downstream actions on alerts

### Design Decisions

#### 1. Push vs Pull Notifications

**Question**: Should we poll for failures or push notifications?

**Answer**: Push-based via SNS.

*Reasoning*:
- Immediate awareness (no polling delay)
- SNS handles delivery to multiple channels (email, SMS, Lambda, etc.)
- Built-in retry logic for delivery failures
- Decouples notification logic from ingestion logic

#### 2. CloudWatch vs Custom Metrics Store

**Question**: Where should we store metrics?

**Answer**: AWS CloudWatch.

*Reasoning*:
- Native integration with AWS ecosystem
- Built-in dashboarding and alarming
- No infrastructure to manage
- Standard for AWS-hosted applications
- Can trigger alarms automatically

#### 3. Per-Failure Alerts vs Aggregated

**Question**: Alert on every failure or aggregate?

**Answer**: Both - immediate email + aggregated metrics.

*Reasoning*:
- **Email per failure**: For immediate visibility during development
- **CloudWatch metrics**: For trend analysis ("3 failures in 10 minutes")
- Can configure CloudWatch Alarms for sophisticated alert logic later

---

## Architecture

```
┌─────────────────┐
│  Ingestion Job  │
│   (on success)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│   CloudWatch    │     │   (No Alert)    │
│ IngestionSuccess│     │                 │
│ RecordCount     │     │                 │
│ Duration        │     │                 │
└─────────────────┘     └─────────────────┘

┌─────────────────┐
│  Ingestion Job  │
│   (on failure)  │
└────────┬────────┘
         │
         ├──────────────────┐
         ▼                  ▼
┌─────────────────┐  ┌─────────────────┐
│   CloudWatch    │  │      SNS        │
│ IngestionFailure│  │   Topic         │
│ FailureByCategory│  │                │
│ Duration        │  │    ┌───────┐    │
└─────────────────┘  │    │ Email │    │
                     │    └───────┘    │
                     └─────────────────┘
```

---

## Components

### 1. Configuration (`src/notifications/config.py`)

Centralized settings for notifications:

```python
# Environment variables
CLOUDWATCH_ENABLED=true
CLOUDWATCH_NAMESPACE=FlightsForecasting/Ingestion

SNS_ENABLED=true
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:ACCOUNT:flights-ingestion-alerts

NOTIFY_ENVIRONMENT=production
NOTIFY_SERVICE_NAME=ingestion-service
```

### 2. CloudWatch Publisher (`src/notifications/cloudwatch.py`)

Publishes custom metrics:

| Metric | Dimensions | Unit |
|--------|------------|------|
| `IngestionSuccess` | Environment, Service | Count |
| `IngestionFailure` | Environment, Service | Count |
| `IngestionFailureByCategory` | Environment, Service, ErrorCategory | Count |
| `IngestionRecordCount` | Environment, Service | Count |
| `IngestionDuration` | Environment, Service | Seconds |

### 3. SNS Notifier (`src/notifications/sns.py`)

Sends email alerts with:
- **Subject**: `[ENVIRONMENT] Ingestion Failed: CATEGORY`
- **Body**: Timestamp, error details, record ID
- **Attributes**: For message filtering

Example email:
```
🚨 INGESTION FAILURE ALERT

Environment: production
Service: ingestion-service
Timestamp: 2025-12-18T01:02:00+00:00

Error Category: API_CONNECTION
Error Message: Failed to connect to OpenSky API

Record ID: 42
```

### 4. Unified Notifier (`src/notifications/notifier.py`)

Single interface used by the ingestion job:

```python
from src.notifications import get_notifier

notifier = get_notifier()

# On success (metrics only)
notifier.on_success(record_id=1, record_count=26, s3_path="s3://...", duration_seconds=3.5)

# On failure (metrics + email)
notifier.on_failure(record_id=2, error_category="API_TIMEOUT", error_message="...")
```

---

## AWS Setup

### Required Services

1. **SNS Topic** - For email delivery
2. **Email Subscription** - Confirm to receive alerts
3. **IAM Permissions** - `sns:Publish` + `cloudwatch:PutMetricData`

### IAM Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["sns:Publish"],
            "Resource": "arn:aws:sns:us-east-1:*:flights-ingestion-alerts"
        },
        {
            "Effect": "Allow",
            "Action": ["cloudwatch:PutMetricData"],
            "Resource": "*"
        }
    ]
}
```

### Creating SNS Topic

```python
import boto3
sns = boto3.client('sns', region_name='us-east-1')

# Create topic
response = sns.create_topic(Name='flights-ingestion-alerts')
topic_arn = response['TopicArn']

# Subscribe email
sns.subscribe(
    TopicArn=topic_arn,
    Protocol='email',
    Endpoint='your@email.com'
)
# Check email and confirm subscription!
```

---

## Configuration Reference

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `CLOUDWATCH_ENABLED` | `true` | Enable metrics publishing |
| `CLOUDWATCH_NAMESPACE` | `FlightsForecasting/Ingestion` | Metric namespace |
| `CLOUDWATCH_REGION` | `us-east-1` | AWS region |
| `SNS_ENABLED` | `true` | Enable email alerts |
| `SNS_TOPIC_ARN` | - | SNS topic ARN (required) |
| `SNS_REGION` | `us-east-1` | AWS region |
| `NOTIFY_ENVIRONMENT` | `development` | Included in alerts |
| `NOTIFY_SERVICE_NAME` | `ingestion-service` | Included in alerts |

---

## Extending Notifications

### Adding Slack

```python
# In src/notifications/slack.py
class SlackNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
    
    def notify_failure(self, category: str, message: str):
        requests.post(self.webhook_url, json={
            "text": f"🚨 *Ingestion Failed*\n*Category*: {category}\n{message}"
        })
```

### CloudWatch Alarms

Create an alarm for consecutive failures:

```python
cloudwatch = boto3.client('cloudwatch')
cloudwatch.put_metric_alarm(
    AlarmName='IngestionFailureAlarm',
    MetricName='IngestionFailure',
    Namespace='FlightsForecasting/Ingestion',
    Statistic='Sum',
    Period=300,  # 5 minutes
    EvaluationPeriods=1,
    Threshold=3,  # 3 failures in 5 minutes
    ComparisonOperator='GreaterThanOrEqualToThreshold',
    AlarmActions=[SNS_TOPIC_ARN],
)
```

---

## Failure Categories

| Category | Description | Typical Action |
|----------|-------------|----------------|
| `RATE_LIMIT` | API quota exceeded | Wait or upgrade API tier |
| `API_TIMEOUT` | Request too slow | Check network/API status |
| `API_CONNECTION` | Can't reach API | Check DNS/firewall |
| `S3_UPLOAD` | S3 write failed | Check credentials/permissions |
| `S3_CONFIG` | S3 misconfigured | Verify bucket/region |
| `PARQUET` | Data format error | Check API response schema |
| `UNEXPECTED` | Unknown error | Review logs |

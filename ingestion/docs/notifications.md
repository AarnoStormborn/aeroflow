# Notifications System

## Overview

The notifications system provides observability into the ingestion pipeline by sending **Discord alerts** (via webhook) when ingestion fails.

---

## First Principles: Why This Architecture?

### The Core Problem

Failures happen. We need to:
1. **Know when things break** - Immediate awareness of failures
2. **Understand patterns** - Historical view of success/failure rates (via ingestion DB records)
3. **Minimize noise** - Alert on actionable issues, not every hiccup
4. **Enable automation** - Allow downstream actions on alerts

### Design Decisions

#### 1. Push vs Pull Notifications

**Question**: Should we poll for failures or push notifications?

**Answer**: Push-based via Discord webhook.

*Reasoning*:
- Immediate awareness (no polling delay)
- Webhooks are simple HTTP POSTs — no SDK/infra needed
- Discord is free and works on desktop + mobile
- Decouples notification logic from ingestion logic

#### 2. Failure Alerts Only vs Success Too

**Question**: Alert on failures, successes, or both?

**Answer**: **Failures only.**

*Reasoning*:
- Success is the expected steady state — alerting on it creates noise
- Ingestion records are persisted in SQLite with status, so success/failure history is queryable
- Failures are the actionable signal ("something is broken")

---

## Architecture

```
┌─────────────────┐
│  Ingestion Job  │
│   (on failure)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     Discord     │
│    Webhook      │
└─────────────────┘
```

---

## Components

### 1. Configuration (`src/notifications/config.py`)

Centralized settings for notifications:

```python
# Environment variables
DISCORD_ENABLED=true
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

NOTIFY_ENVIRONMENT=production
NOTIFY_SERVICE_NAME=ingestion-service
```

### 2. Discord Notifier (`src/notifications/discord.py`)

Sends failure alerts as Discord **embeds** with:
- **Title**: `🚨 Ingestion Failed`
- **Color**: Red (`0xFF0000`)
- **Fields**: Environment, Service, Error Category, Record ID, Error Message
- **Footer**: UTC timestamp

Example message:

```
🚨 Ingestion Failed

Environment: production
Service: ingestion-service
Error Category: API_CONNECTION
Record ID: 42

Error Message:
Failed to connect to OpenSky API

⏰ 2025-12-18 01:02:00 UTC
```

### 3. Unified Notifier (`src/notifications/notifier.py`)

Single interface used by the ingestion job:

```python
from src.notifications import get_notifier

notifier = get_notifier()

# On failure (sends Discord alert)
notifier.on_failure(record_id=2, error_category="API_TIMEOUT", error_message="...")
```

On success, `notify_from_record` simply logs (no alert) — success is the expected state.

---

## Discord Setup

1. Open your Discord server → **Server Settings** → **Integrations** → **Webhooks**
2. Click **New Webhook**, name it (e.g. `ingestion-alerts`), pick a channel
3. Copy the **Webhook URL**
4. Set it as `DISCORD_WEBHOOK_URL` in your environment

---

## Configuration Reference

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DISCORD_ENABLED` | `true` | Enable Discord notifications |
| `DISCORD_WEBHOOK_URL` | - | Discord webhook URL (required to send) |
| `NOTIFY_ENVIRONMENT` | `development` | Included in alerts |
| `NOTIFY_SERVICE_NAME` | `ingestion-service` | Included in alerts |

---

## Extending Notifications

### Adding another channel (e.g. Telegram)

The notifier is structured so each channel is a self-contained class implementing `notify_failure(...)`:

```python
# In src/notifications/telegram.py
class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id

    def notify_failure(self, error_category: str, error_message: str, record_id: int | None = None):
        ...
```

Then wire it into `IngestionNotifier` alongside (or instead of) `DiscordNotifier`.

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

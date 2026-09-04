"""
Daily ingestion summary — sends a Discord message summarizing ingestion
activity since the last cleanup (the DB is reset daily by daily_cleanup).

Runs on the Raspberry Pi daily (via systemd timer / cron). Queries the
local SQLite DB, then posts a summary embed to Discord.

Usage:
    python daily_summary.py
"""

import argparse
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import httpx

# Load .env (repo root has the env file; systemd also injects via EnvironmentFile)
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(_env_path)
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = Path(os.environ.get("DB_PATH", "data/ingestion.db"))
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
DISCORD_ENABLED = os.environ.get("DISCORD_ENABLED", "true").lower() == "true"
ENVIRONMENT = os.environ.get("NOTIFY_ENVIRONMENT", "development")
SERVICE_NAME = os.environ.get("NOTIFY_SERVICE_NAME", "ingestion-service")
BUCKET = os.environ.get("AWS_S3_BUCKET_NAME", "flights-forecasting")


def get_stats() -> dict:
    """Query ingestion stats from the current DB contents.

    The DB is reset by daily_cleanup (daily), so its rows represent the
    data collected since the last cleanup. No calendar-day math needed —
    this avoids timezone issues between the Pi's local time and UTC.
    """
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    def _q(sql, params=()):
        return cur.execute(sql, params).fetchone()[0]

    total_runs = _q("SELECT COUNT(*) FROM ingestion_records")
    success = _q("SELECT COUNT(*) FROM ingestion_records WHERE status='success'")
    failed = _q("SELECT COUNT(*) FROM ingestion_records WHERE status='failed'")
    records = _q(
        "SELECT COALESCE(SUM(record_count), 0) FROM ingestion_records WHERE status='success'"
    )

    # When did the earliest record in this DB window start?
    earliest = _q("SELECT MIN(created_at) FROM ingestion_records")
    if earliest:
        window_start = datetime.fromisoformat(earliest).strftime("%Y-%m-%d %H:%M UTC")
    else:
        window_start = None

    conn.close()

    return {
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "window_start": window_start,
        "total_runs": total_runs,
        "success": success,
        "failed": failed,
        "records": records,
    }


def send_summary(stats: dict) -> bool:
    """Send the summary embed to Discord."""
    if not DISCORD_ENABLED or not DISCORD_WEBHOOK_URL:
        print("Discord disabled or no webhook URL — skipping")
        return False

    window_label = stats["window_start"] or "no data yet"
    # Color: green if no failures, amber if some, red if all failed, gray if no runs
    if stats["total_runs"] == 0:
        color = 0x808080  # gray — no runs
    elif stats["failed"] == 0:
        color = 0x00FF00  # green
    elif stats["success"] == 0:
        color = 0xFF0000  # red
    else:
        color = 0xFFA500  # amber

    success_rate = (
        (stats["success"] / stats["total_runs"] * 100)
        if stats["total_runs"] else 0
    )

    payload = {
        "content": None,
        "embeds": [
            {
                "title": "📊 Ingestion Summary",
                "color": color,
                "fields": [
                    {"name": "Runs", "value": str(stats["total_runs"]), "inline": True},
                    {"name": "Successful", "value": str(stats["success"]), "inline": True},
                    {"name": "Failed", "value": str(stats["failed"]), "inline": True},
                    {"name": "Success Rate", "value": f"{success_rate:.1f}%", "inline": True},
                    {"name": "Records Collected", "value": f"{stats['records']:,}", "inline": True},
                    {"name": "Environment", "value": ENVIRONMENT, "inline": True},
                    {"name": "Since", "value": window_label, "inline": False},
                ],
                "footer": {
                    "text": f"{SERVICE_NAME} • as of {stats['as_of']}",
                },
            }
        ],
    }

    try:
        with httpx.Client(timeout=15) as client:
            resp = client.post(DISCORD_WEBHOOK_URL, json=payload)
            if resp.status_code == 204:
                print(f"Summary sent to Discord (window since {window_label})")
                return True
            print(f"Discord webhook failed: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"Failed to send summary: {e}")
        return False


def main():
    argparse.ArgumentParser(description="Daily ingestion summary → Discord").parse_args()
    stats = get_stats()
    print(f"Summary: {stats}")
    send_summary(stats)


if __name__ == "__main__":
    main()

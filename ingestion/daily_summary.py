"""
Daily ingestion summary — sends a Discord message summarizing the day's
ingestion activity and lifetime totals.

Runs on the Raspberry Pi daily (via systemd timer / cron). Queries the
local SQLite DB, then posts a summary embed to Discord.

Usage:
    python daily_summary.py [--date YYYY-MM-DD]   (default: today)
"""

import argparse
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
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


def get_stats(target_day: date) -> dict:
    """Query ingestion stats for a given day + lifetime totals."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    day_start = datetime.combine(target_day, datetime.min.time(), tzinfo=timezone.utc)
    day_end = datetime.combine(target_day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)

    def _q(sql, params=()):
        return cur.execute(sql, params).fetchone()[0]

    # Day stats
    day_total_runs = _q(
        "SELECT COUNT(*) FROM ingestion_records WHERE created_at >= ? AND created_at < ?",
        (day_start.isoformat(), day_end.isoformat()),
    )
    day_success = _q(
        "SELECT COUNT(*) FROM ingestion_records WHERE created_at >= ? AND created_at < ? AND status='success'",
        (day_start.isoformat(), day_end.isoformat()),
    )
    day_failed = _q(
        "SELECT COUNT(*) FROM ingestion_records WHERE created_at >= ? AND created_at < ? AND status='failed'",
        (day_start.isoformat(), day_end.isoformat()),
    )
    day_records = _q(
        "SELECT COALESCE(SUM(record_count), 0) FROM ingestion_records WHERE created_at >= ? AND created_at < ? AND status='success'",
        (day_start.isoformat(), day_end.isoformat()),
    )
    # Lifetime totals
    life_total_runs = _q("SELECT COUNT(*) FROM ingestion_records")
    life_records = _q("SELECT COALESCE(SUM(record_count), 0) FROM ingestion_records WHERE status='success'")
    life_failed = _q("SELECT COUNT(*) FROM ingestion_records WHERE status='failed'")

    conn.close()

    return {
        "day": target_day.isoformat(),
        "day_total_runs": day_total_runs,
        "day_success": day_success,
        "day_failed": day_failed,
        "day_records": day_records,
        "life_total_runs": life_total_runs,
        "life_records": life_records,
        "life_failed": life_failed,
    }


def send_summary(stats: dict) -> bool:
    """Send the summary embed to Discord."""
    if not DISCORD_ENABLED or not DISCORD_WEBHOOK_URL:
        print("Discord disabled or no webhook URL — skipping")
        return False

    day = stats["day"]
    # Color: green if no failures, amber if some, red if all failed
    if stats["day_total_runs"] == 0:
        color = 0x808080  # gray — no runs
    elif stats["day_failed"] == 0:
        color = 0x00FF00  # green
    elif stats["day_success"] == 0:
        color = 0xFF0000  # red
    else:
        color = 0xFFA500  # amber

    success_rate = (
        (stats["day_success"] / stats["day_total_runs"] * 100)
        if stats["day_total_runs"] else 0
    )

    payload = {
        "content": None,
        "embeds": [
            {
                "title": f"📊 Daily Ingestion Summary — {day}",
                "color": color,
                "fields": [
                    {"name": "Today's Runs", "value": str(stats["day_total_runs"]), "inline": True},
                    {"name": "Successful", "value": str(stats["day_success"]), "inline": True},
                    {"name": "Failed", "value": str(stats["day_failed"]), "inline": True},
                    {"name": "Success Rate", "value": f"{success_rate:.1f}%", "inline": True},
                    {"name": "Records Collected Today", "value": f"{stats['day_records']:,}", "inline": True},
                    {"name": "Environment", "value": ENVIRONMENT, "inline": True},
                    {"name": "Lifetime Runs", "value": str(stats["life_total_runs"]), "inline": True},
                    {"name": "Lifetime Records", "value": f"{stats['life_records']:,}", "inline": True},
                    {"name": "Lifetime Failures", "value": str(stats["life_failed"]), "inline": True},
                ],
                "footer": {
                    "text": f"{SERVICE_NAME} • {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
                },
            }
        ],
    }

    try:
        with httpx.Client(timeout=15) as client:
            resp = client.post(DISCORD_WEBHOOK_URL, json=payload)
            if resp.status_code == 204:
                print(f"Summary sent to Discord for {day}")
                return True
            print(f"Discord webhook failed: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"Failed to send summary: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Daily ingestion summary → Discord")
    parser.add_argument("--date", type=str, default=None, help="Date to summarize (YYYY-MM-DD), default today")
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else date.today()
    stats = get_stats(target)
    print(f"Summary for {target}: {stats}")
    send_summary(stats)


if __name__ == "__main__":
    main()

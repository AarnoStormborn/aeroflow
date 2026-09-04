"""
Daily cleanup — backs up the ingestion SQLite DB + logs to S3, then resets
the DB for a fresh day.

Runs on the Raspberry Pi daily (via systemd timer / cron).

Approach:
- Uses SQLite's online backup API (sqlite3.Connection.backup) so the
  snapshot is consistent even while the ingestion service writes.
- Uploads DB + logs to S3 under backups/ingestion/YYYY-MM-DD/
- Resets the DB in place (DELETE all rows, keep schema) — no downtime,
  the running service is unaffected.

Usage:
    python daily_cleanup.py
"""

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import boto3

# ---------------------------------------------------------------------------
# Config (loaded from .env by the systemd EnvironmentFile)
# ---------------------------------------------------------------------------

DB_PATH = Path(os.environ.get("DB_PATH", "data/ingestion.db"))
LOG_PATH = Path(os.environ.get("LOG_DIR", "logs")) / (
    os.environ.get("LOG_FILE", "services.log")
)
BUCKET = os.environ.get("AWS_S3_BUCKET_NAME", "flights-forecasting")
BACKUP_PREFIX = os.environ.get("BACKUP_PREFIX", "backups/ingestion")


def s3_client():
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def upload_to_s3(key: str, data: bytes) -> str:
    """Upload bytes to S3, return s3:// URL."""
    client = s3_client()
    client.put_object(Bucket=BUCKET, Key=key, Body=data)
    url = f"s3://{BUCKET}/{key}"
    print(f"Uploaded: {url} ({len(data)} bytes)")
    return url


def main():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_dir = f"{BACKUP_PREFIX}/{today}"
    print(f"=== Daily cleanup for {today} ===")

    # 1. Consistent DB snapshot using SQLite online backup to a temp file
    backup_file = DB_PATH.with_name(f"ingestion_backup_{today}.db")
    src = sqlite3.connect(str(DB_PATH))
    dst = sqlite3.connect(str(backup_file))
    try:
        src.backup(dst)
        print(f"DB snapshot created: {backup_file}")
    finally:
        src.close()
        dst.close()

    # 2. Upload DB + logs to S3
    if backup_file.exists():
        upload_to_s3(f"{date_dir}/ingestion.db", backup_file.read_bytes())
        backup_file.unlink()  # remove local temp

    if LOG_PATH.exists():
        upload_to_s3(f"{date_dir}/services.log", LOG_PATH.read_bytes())
    else:
        print(f"Log not found at {LOG_PATH}, skipping")

    # 3. Reset the DB in place (fresh day) — keep schema, clear rows
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.execute("DELETE FROM ingestion_records")
        conn.commit()
        print("DB reset for fresh day (rows cleared, schema kept)")
    finally:
        conn.close()

    print("=== Cleanup complete ===")


if __name__ == "__main__":
    main()

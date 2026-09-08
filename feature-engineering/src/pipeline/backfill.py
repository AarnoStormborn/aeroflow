"""
Self-healing feature backfill.

Scans S3 for raw flight-data days that do NOT yet have a corresponding
feature file, and runs the feature pipeline for those days (most recent
first). This ensures gaps (a missed run_feature, a Modal outage, etc.)
get filled automatically instead of leaving permanent holes in the
feature history that downstream training relies on.

Usage:
    uv run python -m src.pipeline.backfill            # backfill all gaps
    uv run python -m src.pipeline.backfill --limit 3  # only N most recent
"""

import argparse
import re
from datetime import date, datetime

import boto3
from loguru import logger
from src.features.config import settings
from src.pipeline.run import run_feature_pipeline


def _s3():
    return boto3.client(
        "s3",
        region_name=settings.s3.region,
        aws_access_key_id=settings.s3.access_key_id,
        aws_secret_access_key=settings.s3.secret_access_key,
    )


def _raw_days() -> set[date]:
    """All days (UTC) that have raw parquet files."""
    client = _s3()
    days: set[date] = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.s3.bucket_name,
                                   Prefix=settings.s3.prefix):
        for obj in page.get("Contents", []):
            m = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})", obj["Key"])
            if m:
                days.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
    return days


def _feature_days() -> set[date]:
    """All days that already have a feature file."""
    client = _s3()
    days: set[date] = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.s3.bucket_name,
                                   Prefix="features/hourly/"):
        for obj in page.get("Contents", []):
            m = re.search(r"features_(\d{4})-(\d{2})-(\d{2})", obj["Key"])
            if m:
                days.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
    return days


def find_gaps() -> list[date]:
    """Raw days lacking feature files, sorted newest first."""
    raw = _raw_days()
    have = _feature_days()
    gaps = sorted(raw - have, reverse=True)
    logger.info(f"Raw days: {len(raw)} | feature days: {len(have)} | gaps: {len(gaps)}")
    return gaps


def backfill(limit: int | None = None) -> list[dict]:
    """Run the feature pipeline for gap days. Returns results per day."""
    gaps = find_gaps()
    if not gaps:
        print("No feature gaps — everything is current.")
        return []

    # Don't backfill today (its raw data is still accumulating)
    today = datetime.now().date()
    gaps = [d for d in gaps if d < today]

    if limit:
        gaps = gaps[:limit]

    results = []
    for day in gaps:
        print(f"\n=== Backfilling features for {day} ===")
        try:
            s3_url = run_feature_pipeline(day)
            results.append({"date": str(day), "status": "ok", "s3_url": s3_url})
        except Exception as e:
            logger.error(f"Backfill failed for {day}: {e}")
            results.append({"date": str(day), "status": "failed", "error": str(e)})
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the N most recent gap days")
    args = parser.parse_args()
    results = backfill(args.limit)
    print(f"\n=== Backfill complete: {len(results)} days ===")
    for r in results:
        print(f"  {r['date']}: {r['status']}")


if __name__ == "__main__":
    main()

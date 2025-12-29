"""
Daily feature pipeline runner.

Loads raw data, creates features, uploads to S3.
Runs daily alongside the report generation pipeline.

Usage:
    uv run python -m src.pipeline.run --date 2025-12-28
"""

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
import io

import polars as pl
from loguru import logger

from src.features.data import create_loader
from src.features.config import settings
from src.pipeline.features import prepare_daily_features

import boto3


class FeatureUploader:
    """Uploads feature parquet files to S3."""
    
    def __init__(self):
        self.bucket = settings.s3.bucket_name
        self.client = boto3.client(
            "s3",
            region_name=settings.s3.region,
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
        )
        logger.info(f"FeatureUploader initialized (bucket: {self.bucket})")
    
    def upload_features(self, df: pl.DataFrame, feature_date: date) -> str:
        """
        Upload feature parquet to S3.
        
        Args:
            df: Feature DataFrame
            feature_date: Date of the features
            
        Returns:
            S3 URL of uploaded file
        """
        key = f"features/hourly/year={feature_date.year}/month={feature_date.month:02d}/features_{feature_date.isoformat()}.parquet"
        
        # Write to bytes buffer
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        
        self.client.upload_fileobj(
            buffer,
            self.bucket,
            key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )
        
        s3_url = f"s3://{self.bucket}/{key}"
        logger.info(f"Uploaded features: {s3_url}")
        return s3_url


def run_feature_pipeline(target_date: date) -> str:
    """
    Run the daily feature engineering pipeline.
    
    Args:
        target_date: Date to process
        
    Returns:
        S3 URL of uploaded features
    """
    print("=" * 60)
    print("DAILY FEATURE ENGINEERING PIPELINE")
    print(f"Date: {target_date}")
    print("=" * 60)
    
    # 1. Load raw data (target date + prior day for lag features)
    logger.info("Step 1: Loading raw data...")
    loader = create_loader()
    
    # Need 2 days to compute lag_24h features for target date
    prior_date = target_date - timedelta(days=1)
    
    prior_df = loader.load_day(prior_date)
    target_df = loader.load_day(target_date)
    
    if target_df.is_empty():
        raise ValueError(f"No raw data available for {target_date}")
    
    # Combine both days
    if not prior_df.is_empty():
        raw_df = pl.concat([prior_df, target_df], how="diagonal_relaxed")
        logger.info(f"Loaded {len(raw_df):,} raw records (2 days)")
    else:
        raw_df = target_df
        logger.warning(f"No prior day data, using only {len(raw_df):,} records")
    
    # 2. Create features
    logger.info("Step 2: Creating features...")
    features_df = prepare_daily_features(raw_df)
    
    # Filter to only hours from target date
    features_df = features_df.filter(
        pl.col("hour_start").dt.date() == target_date
    )
    
    print(f"\nFeature DataFrame:")
    print(f"  Rows: {len(features_df)}")
    print(f"  Columns: {features_df.columns}")
    print(features_df.head(5))
    
    # 3. Save locally (for debugging)
    local_path = Path("data/features") / f"features_{target_date.isoformat()}.parquet"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    features_df.write_parquet(local_path)
    logger.info(f"Saved locally: {local_path}")
    
    # 4. Upload to S3
    logger.info("Step 3: Uploading to S3...")
    uploader = FeatureUploader()
    s3_url = uploader.upload_features(features_df, target_date)
    
    print("=" * 60)
    print("PIPELINE COMPLETE")
    print(f"Features: {s3_url}")
    print("=" * 60)
    
    return s3_url


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run daily feature engineering pipeline")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to process (YYYY-MM-DD). Defaults to yesterday.",
    )
    
    args = parser.parse_args()
    
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)
    
    run_feature_pipeline(target_date)


if __name__ == "__main__":
    main()

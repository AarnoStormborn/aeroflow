"""
Data loader for feature files from S3.

Loads the rolling window of feature parquet files.
"""

from datetime import date, timedelta
from pathlib import Path
import io

import boto3
import polars as pl
from loguru import logger

from src.training.config import settings


class FeatureLoader:
    """Loads feature data from S3."""
    
    def __init__(self):
        self.bucket = settings.s3.bucket_name
        self.client = boto3.client(
            "s3",
            region_name=settings.s3.region,
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
        )
        logger.info(f"FeatureLoader initialized (bucket: {self.bucket})")
    
    def _get_feature_key(self, dt: date) -> str:
        """Get S3 key for feature file."""
        return f"features/hourly/year={dt.year}/month={dt.month:02d}/features_{dt.isoformat()}.parquet"
    
    def load_day(self, dt: date) -> pl.DataFrame | None:
        """
        Load feature file for a specific day.
        
        Args:
            dt: Date to load
            
        Returns:
            DataFrame or None if not found
        """
        key = self._get_feature_key(dt)
        
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            data = response["Body"].read()
            df = pl.read_parquet(io.BytesIO(data))
            logger.debug(f"Loaded {len(df)} rows from {key}")
            return df
        except self.client.exceptions.NoSuchKey:
            logger.warning(f"No feature file for {dt}")
            return None
        except Exception as e:
            logger.error(f"Failed to load {key}: {e}")
            return None
    
    def load_rolling_window(self, end_date: date, window_days: int = 14) -> pl.DataFrame:
        """
        Load rolling window of feature data.
        
        Args:
            end_date: Last day of the window (inclusive)
            window_days: Number of days to load
            
        Returns:
            Combined DataFrame
        """
        start_date = end_date - timedelta(days=window_days - 1)
        logger.info(f"Loading features from {start_date} to {end_date} ({window_days} days)")
        
        dfs = []
        current = start_date
        loaded_days = 0
        
        while current <= end_date:
            df = self.load_day(current)
            if df is not None and not df.is_empty():
                dfs.append(df)
                loaded_days += 1
            current += timedelta(days=1)
        
        if not dfs:
            raise ValueError(f"No feature data available for {start_date} to {end_date}")
        
        combined = pl.concat(dfs, how="diagonal_relaxed").sort("hour_start")
        logger.info(f"Loaded {len(combined)} samples from {loaded_days}/{window_days} days")
        
        return combined


def create_loader() -> FeatureLoader:
    """Create a new feature loader."""
    return FeatureLoader()


__all__ = ["FeatureLoader", "create_loader"]

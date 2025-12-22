"""
S3 data loader for reading flight state vectors.

Loads Parquet files from S3 for a given date range.
"""

from datetime import date, datetime, timedelta
from pathlib import Path
import io

import boto3
import polars as pl
from loguru import logger

from src.features.config import settings


class S3DataLoader:
    """
    Loads flight data from S3.
    
    Data is stored in partitioned format:
    s3://bucket/raw/flights/states/year=YYYY/month=MM/day=DD/*.parquet
    """
    
    def __init__(self):
        """Initialize S3 client."""
        self.bucket = settings.s3.bucket_name
        self.prefix = settings.s3.prefix
        
        self.client = boto3.client(
            "s3",
            region_name=settings.s3.region,
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
        )
        
        # Ensure cache directory exists
        self.cache_dir = Path(settings.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"S3DataLoader initialized (bucket: {self.bucket})")
    
    def _get_prefix_for_date(self, dt: date) -> str:
        """Get S3 prefix for a specific date."""
        return f"{self.prefix}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    
    def list_files_for_date(self, dt: date) -> list[str]:
        """
        List all Parquet files for a specific date.
        
        Args:
            dt: Date to list files for
            
        Returns:
            List of S3 object keys
        """
        prefix = self._get_prefix_for_date(dt)
        
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
            )
            
            files = [
                obj["Key"]
                for obj in response.get("Contents", [])
                if obj["Key"].endswith(".parquet")
            ]
            
            logger.info(f"Found {len(files)} files for {dt}")
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files for {dt}: {e}")
            return []
    
    def load_file(self, key: str) -> pl.DataFrame:
        """
        Load a single Parquet file from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            Polars DataFrame
        """
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            data = response["Body"].read()
            
            df = pl.read_parquet(io.BytesIO(data))
            logger.debug(f"Loaded {len(df)} rows from {key}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load {key}: {e}")
            return pl.DataFrame()
    
    def load_day(self, dt: date, use_cache: bool = True) -> pl.DataFrame:
        """
        Load all data for a specific day.
        
        Args:
            dt: Date to load
            use_cache: Whether to use local cache
            
        Returns:
            Combined Polars DataFrame
        """
        cache_file = self.cache_dir / f"{dt.isoformat()}.parquet"
        
        # Try cache first
        if use_cache and cache_file.exists():
            logger.info(f"Loading from cache: {cache_file}")
            return pl.read_parquet(cache_file)
        
        # Load from S3
        files = self.list_files_for_date(dt)
        
        if not files:
            logger.warning(f"No data found for {dt}")
            return pl.DataFrame()
        
        # Load and concatenate all files
        dfs = []
        for key in files:
            df = self.load_file(key)
            if len(df) > 0:
                dfs.append(df)
        
        if not dfs:
            return pl.DataFrame()
        
        combined = pl.concat(dfs, how="diagonal_relaxed")
        logger.info(f"Loaded {len(combined)} total rows for {dt}")
        
        # Cache the result
        if use_cache:
            combined.write_parquet(cache_file)
            logger.info(f"Cached to {cache_file}")
        
        return combined
    
    def load_date_range(
        self,
        start_date: date,
        end_date: date,
        use_cache: bool = True,
    ) -> pl.DataFrame:
        """
        Load data for a date range.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            use_cache: Whether to use local cache
            
        Returns:
            Combined Polars DataFrame
        """
        dfs = []
        current = start_date
        
        while current <= end_date:
            df = self.load_day(current, use_cache=use_cache)
            if len(df) > 0:
                dfs.append(df)
            current += timedelta(days=1)
        
        if not dfs:
            return pl.DataFrame()
        
        return pl.concat(dfs, how="diagonal_relaxed")


def create_loader() -> S3DataLoader:
    """Create a new S3 data loader."""
    return S3DataLoader()


__all__ = ["S3DataLoader", "create_loader"]

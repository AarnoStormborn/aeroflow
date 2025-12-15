"""
S3 uploader for storing flight data as Parquet files.

Uses Polars for DataFrame operations and Parquet generation,
and boto3 for S3 uploads.
"""

import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import polars as pl

from src.utils import logger
from src.utils.exceptions import (
    S3UploadError,
    S3ConfigurationError,
    ParquetError,
)
from src.ingestion.config import settings


class S3Uploader:
    """
    Upload Parquet files to AWS S3.
    
    Handles:
    - Converting flight data to Polars DataFrame
    - Generating Parquet files with proper schema
    - Uploading to S3 with timestamp-based paths
    """
    
    # Schema for flight data from OpenSky API
    FLIGHT_SCHEMA = {
        "icao24": pl.Utf8,
        "firstSeen": pl.Int64,
        "estDepartureAirport": pl.Utf8,
        "lastSeen": pl.Int64,
        "estArrivalAirport": pl.Utf8,
        "callsign": pl.Utf8,
        "estDepartureAirportHorizDistance": pl.Int64,
        "estDepartureAirportVertDistance": pl.Int64,
        "estArrivalAirportHorizDistance": pl.Int64,
        "estArrivalAirportVertDistance": pl.Int64,
        "departureAirportCandidatesCount": pl.Int64,
        "arrivalAirportCandidatesCount": pl.Int64,
    }
    
    # Schema for state vectors from OpenSky API
    STATE_SCHEMA = {
        "icao24": pl.Utf8,
        "callsign": pl.Utf8,
        "origin_country": pl.Utf8,
        "time_position": pl.Int64,
        "last_contact": pl.Int64,
        "longitude": pl.Float64,
        "latitude": pl.Float64,
        "baro_altitude": pl.Float64,
        "on_ground": pl.Boolean,
        "velocity": pl.Float64,
        "true_track": pl.Float64,
        "vertical_rate": pl.Float64,
        "sensors": pl.List(pl.Int64),
        "geo_altitude": pl.Float64,
        "squawk": pl.Utf8,
        "spi": pl.Boolean,
        "position_source": pl.Int64,
        "category": pl.Int64,
    }
    
    def __init__(
        self,
        bucket_name: str | None = None,
        prefix: str | None = None,
        region: str | None = None,
        endpoint_url: str | None = None,
    ):
        """
        Initialize the S3 uploader.
        
        Args:
            bucket_name: S3 bucket name
            prefix: Key prefix for uploaded files
            region: AWS region
            endpoint_url: Custom endpoint URL (for LocalStack/MinIO)
        """
        self.bucket_name = bucket_name or settings.s3.bucket_name
        self.prefix = prefix or settings.s3.prefix
        self.region = region or settings.s3.region
        self.endpoint_url = endpoint_url or settings.s3.endpoint_url
        
        self._client = self._create_client()
        logger.info(f"S3 uploader initialized for bucket: {self.bucket_name}")
    
    def _create_client(self) -> boto3.client:
        """Create boto3 S3 client."""
        try:
            client_kwargs = {
                "service_name": "s3",
                "region_name": self.region,
            }
            
            if self.endpoint_url:
                client_kwargs["endpoint_url"] = self.endpoint_url
                logger.debug(f"Using custom S3 endpoint: {self.endpoint_url}")
            
            # Use explicit credentials if provided
            if settings.s3.access_key_id and settings.s3.secret_access_key:
                client_kwargs["aws_access_key_id"] = settings.s3.access_key_id
                client_kwargs["aws_secret_access_key"] = settings.s3.secret_access_key
            
            return boto3.client(**client_kwargs)
            
        except NoCredentialsError as e:
            raise S3ConfigurationError(f"AWS credentials not found: {e}")
    
    def _generate_s3_key(self, timestamp: datetime, data_type: str = "flights") -> str:
        """
        Generate S3 key with timestamp-based path.
        
        Format: {prefix}/{data_type}/year=YYYY/month=MM/day=DD/YYYYMMDD_HHMMSS.parquet
        
        Args:
            timestamp: Timestamp for the data
            data_type: Type of data (flights, states)
            
        Returns:
            S3 key string
        """
        date_partition = timestamp.strftime("year=%Y/month=%m/day=%d")
        filename = timestamp.strftime("%Y%m%d_%H%M%S.parquet")
        
        return f"{self.prefix}/{data_type}/{date_partition}/{filename}"
    
    def flights_to_dataframe(self, flights: list[dict[str, Any]]) -> pl.DataFrame:
        """
        Convert flight records to a Polars DataFrame.
        
        Args:
            flights: List of flight records from OpenSky API
            
        Returns:
            Polars DataFrame with proper schema
        """
        if not flights:
            logger.warning("Empty flights list, returning empty DataFrame")
            return pl.DataFrame(schema=self.FLIGHT_SCHEMA)
        
        try:
            df = pl.DataFrame(flights)
            
            # Cast columns to proper types
            for col, dtype in self.FLIGHT_SCHEMA.items():
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(dtype, strict=False))
            
            logger.debug(f"Created DataFrame with {len(df)} flight records")
            return df
            
        except Exception as e:
            raise ParquetError(f"Failed to create DataFrame from flights: {e}")
    
    def states_to_dataframe(self, states_response: dict[str, Any]) -> pl.DataFrame:
        """
        Convert state vectors to a Polars DataFrame.
        
        Args:
            states_response: Response from /states/all endpoint
            
        Returns:
            Polars DataFrame with state vector data
        """
        states = states_response.get("states", [])
        
        if not states:
            logger.warning("Empty states list, returning empty DataFrame")
            return pl.DataFrame(schema=self.STATE_SCHEMA)
        
        try:
            # State vectors come as arrays, need to convert to dicts
            columns = [
                "icao24", "callsign", "origin_country", "time_position",
                "last_contact", "longitude", "latitude", "baro_altitude",
                "on_ground", "velocity", "true_track", "vertical_rate",
                "sensors", "geo_altitude", "squawk", "spi", "position_source",
                "category"
            ]
            
            records = []
            for state in states:
                record = {col: state[i] if i < len(state) else None 
                         for i, col in enumerate(columns)}
                records.append(record)
            
            df = pl.DataFrame(records)
            
            # Add capture timestamp
            df = df.with_columns(
                pl.lit(states_response.get("time")).alias("capture_time")
            )
            
            logger.debug(f"Created DataFrame with {len(df)} state vectors")
            return df
            
        except Exception as e:
            raise ParquetError(f"Failed to create DataFrame from states: {e}")
    
    def dataframe_to_parquet_bytes(self, df: pl.DataFrame) -> bytes:
        """
        Convert DataFrame to Parquet bytes.
        
        Args:
            df: Polars DataFrame
            
        Returns:
            Parquet file as bytes
        """
        try:
            buffer = io.BytesIO()
            df.write_parquet(buffer, compression="snappy")
            buffer.seek(0)
            return buffer.read()
        except Exception as e:
            raise ParquetError(f"Failed to convert DataFrame to Parquet: {e}")
    
    def upload_bytes(self, data: bytes, s3_key: str) -> str:
        """
        Upload bytes to S3.
        
        Args:
            data: Bytes to upload
            s3_key: S3 object key
            
        Returns:
            Full S3 path (s3://bucket/key)
        """
        try:
            logger.info(f"Uploading {len(data)} bytes to s3://{self.bucket_name}/{s3_key}")
            
            self._client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=data,
                ContentType="application/octet-stream",
            )
            
            s3_path = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Upload successful: {s3_path}")
            return s3_path
            
        except ClientError as e:
            raise S3UploadError(
                message=f"Failed to upload to S3: {e}",
                bucket=self.bucket_name,
                key=s3_key,
            )
    
    def upload_flights(
        self,
        flights: list[dict[str, Any]],
        timestamp: datetime | None = None,
    ) -> tuple[str, int]:
        """
        Upload flight data to S3 as Parquet.
        
        Args:
            flights: List of flight records
            timestamp: Timestamp for the data (defaults to now)
            
        Returns:
            Tuple of (S3 path, record count)
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        
        df = self.flights_to_dataframe(flights)
        parquet_bytes = self.dataframe_to_parquet_bytes(df)
        s3_key = self._generate_s3_key(timestamp, "flights")
        s3_path = self.upload_bytes(parquet_bytes, s3_key)
        
        return s3_path, len(df)
    
    def upload_states(
        self,
        states_response: dict[str, Any],
        timestamp: datetime | None = None,
    ) -> tuple[str, int]:
        """
        Upload state vectors to S3 as Parquet.
        
        Args:
            states_response: Response from /states/all endpoint
            timestamp: Timestamp for the data (defaults to now)
            
        Returns:
            Tuple of (S3 path, record count)
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        
        df = self.states_to_dataframe(states_response)
        parquet_bytes = self.dataframe_to_parquet_bytes(df)
        s3_key = self._generate_s3_key(timestamp, "states")
        s3_path = self.upload_bytes(parquet_bytes, s3_key)
        
        return s3_path, len(df)


def create_uploader() -> S3Uploader:
    """Create a new S3 uploader with default settings."""
    return S3Uploader()


__all__ = ["S3Uploader", "create_uploader"]

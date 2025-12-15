"""
Local file storage for testing without S3.

Stores Parquet files locally in a specified directory.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from src.utils import logger
from src.utils.exceptions import ParquetError
from src.ingestion.config import settings


class LocalStorage:
    """
    Local file storage for Parquet files.
    
    Used for testing and development without S3.
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
    
    def __init__(self, base_dir: str | Path = "samples"):
        """
        Initialize local storage.
        
        Args:
            base_dir: Base directory for storing files
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"LocalStorage initialized at: {self.base_dir.absolute()}")
    
    def _generate_path(self, timestamp: datetime, data_type: str = "flights") -> Path:
        """
        Generate file path with timestamp-based directory structure.
        
        Format: {base_dir}/{data_type}/year=YYYY/month=MM/day=DD/YYYYMMDD_HHMMSS.parquet
        """
        date_partition = timestamp.strftime("year=%Y/month=%m/day=%d")
        filename = timestamp.strftime("%Y%m%d_%H%M%S.parquet")
        
        full_path = self.base_dir / data_type / date_partition / filename
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        return full_path
    
    def flights_to_dataframe(self, flights: list[dict[str, Any]]) -> pl.DataFrame:
        """Convert flight records to a Polars DataFrame."""
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
        """Convert state vectors to a Polars DataFrame."""
        states = states_response.get("states", [])
        
        if not states:
            logger.warning("Empty states list, returning empty DataFrame")
            return pl.DataFrame(schema=self.STATE_SCHEMA)
        
        try:
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
            df = df.with_columns(
                pl.lit(states_response.get("time")).alias("capture_time")
            )
            
            logger.debug(f"Created DataFrame with {len(df)} state vectors")
            return df
            
        except Exception as e:
            raise ParquetError(f"Failed to create DataFrame from states: {e}")
    
    def save_flights(
        self,
        flights: list[dict[str, Any]],
        timestamp: datetime | None = None,
    ) -> tuple[str, int]:
        """
        Save flight data to local Parquet file.
        
        Args:
            flights: List of flight records
            timestamp: Timestamp for the data (defaults to now)
            
        Returns:
            Tuple of (file path, record count)
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        
        df = self.flights_to_dataframe(flights)
        file_path = self._generate_path(timestamp, "flights")
        
        try:
            df.write_parquet(file_path, compression="snappy")
            logger.info(f"Saved {len(df)} flights to {file_path}")
            return str(file_path), len(df)
        except Exception as e:
            raise ParquetError(f"Failed to save flights: {e}")
    
    def save_states(
        self,
        states_response: dict[str, Any],
        timestamp: datetime | None = None,
    ) -> tuple[str, int]:
        """
        Save state vectors to local Parquet file.
        
        Args:
            states_response: Response from /states/all endpoint
            timestamp: Timestamp for the data (defaults to now)
            
        Returns:
            Tuple of (file path, record count)
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        
        df = self.states_to_dataframe(states_response)
        file_path = self._generate_path(timestamp, "states")
        
        try:
            df.write_parquet(file_path, compression="snappy")
            logger.info(f"Saved {len(df)} states to {file_path}")
            return str(file_path), len(df)
        except Exception as e:
            raise ParquetError(f"Failed to save states: {e}")


def create_local_storage(base_dir: str | Path = "samples") -> LocalStorage:
    """Create a new local storage instance."""
    return LocalStorage(base_dir)


__all__ = ["LocalStorage", "create_local_storage"]

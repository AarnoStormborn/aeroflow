"""
Data cleaning utilities for flight state vectors.

Handles common data quality issues:
- Missing values
- Invalid coordinates
- Duplicate records
- Data type normalization
"""

import polars as pl
from loguru import logger


def clean_flight_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean flight state vector data.
    
    Args:
        df: Raw flight data DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    if df.is_empty():
        return df
    
    initial_count = len(df)
    logger.info(f"Starting cleaning with {initial_count} rows")
    
    # 1. Remove rows with null ICAO24 (aircraft identifier is required)
    df = df.filter(pl.col("icao24").is_not_null())
    
    # 2. Remove rows with null latitude or longitude
    df = df.filter(
        pl.col("latitude").is_not_null() & 
        pl.col("longitude").is_not_null()
    )
    
    # 3. Filter invalid coordinates
    df = df.filter(
        (pl.col("latitude") >= -90) & (pl.col("latitude") <= 90) &
        (pl.col("longitude") >= -180) & (pl.col("longitude") <= 180)
    )
    
    # 4. Filter unrealistic altitudes (below -1000m or above 50000m)
    if "baro_altitude" in df.columns:
        df = df.filter(
            pl.col("baro_altitude").is_null() |
            ((pl.col("baro_altitude") >= -1000) & (pl.col("baro_altitude") <= 50000))
        )
    
    # 5. Filter unrealistic velocities (above 1000 m/s = Mach 3)
    if "velocity" in df.columns:
        df = df.filter(
            pl.col("velocity").is_null() |
            ((pl.col("velocity") >= 0) & (pl.col("velocity") <= 1000))
        )
    
    # 6. Remove exact duplicates
    df = df.unique()
    
    final_count = len(df)
    removed = initial_count - final_count
    logger.info(f"Cleaning complete: {final_count} rows ({removed} removed, {100*removed/initial_count:.1f}%)")
    
    return df


def add_derived_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add derived columns for analysis.
    
    Args:
        df: Cleaned flight data DataFrame
        
    Returns:
        DataFrame with additional columns
    """
    if df.is_empty():
        return df
    
    # Convert capture_time to datetime if it's an integer (Unix timestamp)
    if "capture_time" in df.columns:
        df = df.with_columns([
            pl.from_epoch(pl.col("capture_time")).alias("capture_datetime"),
            pl.from_epoch(pl.col("capture_time")).dt.hour().alias("hour"),
            pl.from_epoch(pl.col("capture_time")).dt.minute().alias("minute"),
        ])
    
    # Add speed in km/h (if velocity is in m/s)
    if "velocity" in df.columns:
        df = df.with_columns([
            (pl.col("velocity") * 3.6).alias("speed_kmh"),
        ])
    
    # Add altitude in feet (if baro_altitude is in meters)
    if "baro_altitude" in df.columns:
        df = df.with_columns([
            (pl.col("baro_altitude") * 3.28084).alias("altitude_ft"),
        ])
    
    # Categorize flight phase based on vertical rate
    if "vertical_rate" in df.columns:
        df = df.with_columns([
            pl.when(pl.col("vertical_rate") > 2)
            .then(pl.lit("climbing"))
            .when(pl.col("vertical_rate") < -2)
            .then(pl.lit("descending"))
            .otherwise(pl.lit("level"))
            .alias("flight_phase"),
        ])
    
    logger.info(f"Added derived columns: {df.columns}")
    return df


def get_data_summary(df: pl.DataFrame) -> dict:
    """
    Get summary statistics of the data.
    
    Args:
        df: Flight data DataFrame
        
    Returns:
        Dictionary with summary statistics
    """
    if df.is_empty():
        return {"error": "Empty DataFrame"}
    
    summary = {
        "total_records": len(df),
        "unique_aircraft": df["icao24"].n_unique() if "icao24" in df.columns else 0,
        "columns": df.columns,
        "null_counts": {col: df[col].null_count() for col in df.columns},
    }
    
    # Numeric column stats
    numeric_cols = ["latitude", "longitude", "baro_altitude", "velocity", "vertical_rate"]
    for col in numeric_cols:
        if col in df.columns:
            summary[f"{col}_min"] = df[col].min()
            summary[f"{col}_max"] = df[col].max()
            summary[f"{col}_mean"] = df[col].mean()
    
    # Time range
    if "capture_time" in df.columns:
        summary["time_min"] = df["capture_time"].min()
        summary["time_max"] = df["capture_time"].max()
    
    return summary


__all__ = ["clean_flight_data", "add_derived_columns", "get_data_summary"]

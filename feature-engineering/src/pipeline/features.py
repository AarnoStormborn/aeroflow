"""
Feature creation for flight traffic forecasting.

Produces hourly aggregates with:
- hour_of_day: Hour (0-23)
- day_of_week: Weekday (0=Mon, 6=Sun)
- is_weekend: Boolean (Sat/Sun)
- lag_1h: Flight count 1 hour ago
- lag_24h: Flight count 24 hours ago
- rolling_mean_6h: 6-hour rolling average
"""

import polars as pl
from loguru import logger


def create_hourly_aggregates(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aggregate raw flight data to hourly flight counts.
    
    Args:
        df: Raw flight data with capture_time
        
    Returns:
        DataFrame with hour_start and flight_count
    """
    # Convert epoch to datetime
    df = df.with_columns([
        pl.from_epoch(pl.col("capture_time")).alias("timestamp"),
    ])
    
    # Truncate to hour
    df = df.with_columns([
        pl.col("timestamp").dt.truncate("1h").alias("hour_start"),
    ])
    
    # Count unique aircraft per hour
    hourly = df.group_by("hour_start").agg([
        pl.col("icao24").n_unique().alias("flight_count"),
    ]).sort("hour_start")
    
    logger.info(f"Created hourly aggregates: {len(hourly)} hours")
    return hourly


def create_features(hourly_df: pl.DataFrame) -> pl.DataFrame:
    """
    Create forecasting features from hourly aggregates.
    
    Args:
        hourly_df: DataFrame with hour_start and flight_count
        
    Returns:
        DataFrame with all features added
    """
    df = hourly_df.sort("hour_start")
    
    df = df.with_columns([
        # Time features
        pl.col("hour_start").dt.hour().alias("hour_of_day"),
        pl.col("hour_start").dt.weekday().alias("day_of_week"),
        (pl.col("hour_start").dt.weekday() >= 5).cast(pl.Int32).alias("is_weekend"),
        
        # Lag features
        pl.col("flight_count").shift(1).alias("lag_1h"),
        pl.col("flight_count").shift(24).alias("lag_24h"),
        
        # Rolling mean (6 hour window for quarter-day patterns)
        pl.col("flight_count").shift(1).rolling_mean(window_size=6).alias("rolling_mean_6h"),
    ])
    
    # Drop rows with null values from lag/rolling
    df = df.drop_nulls(subset=["lag_1h", "lag_24h", "rolling_mean_6h"])
    
    logger.info(f"Created features: {len(df)} samples")
    return df


def prepare_daily_features(raw_df: pl.DataFrame) -> pl.DataFrame:
    """
    Full pipeline: raw data -> daily feature set.
    
    Args:
        raw_df: Raw flight state vectors
        
    Returns:
        DataFrame ready for model training
    """
    hourly = create_hourly_aggregates(raw_df)
    featured = create_features(hourly)
    
    # Select final columns in order
    feature_cols = [
        "hour_start",
        "flight_count",
        "hour_of_day",
        "day_of_week", 
        "is_weekend",
        "lag_1h",
        "lag_24h",
        "rolling_mean_6h",
    ]
    
    return featured.select(feature_cols)


__all__ = [
    "create_hourly_aggregates",
    "create_features",
    "prepare_daily_features",
]

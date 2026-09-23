"""
Feature creation for flight traffic forecasting.

Produces hourly aggregates with:
- hour_of_day: Hour (0-23)
- day_of_week: Weekday (0=Mon, 6=Sun)
- is_weekend: Boolean (Sat/Sun)
- lag_1h: Flight count 1 hour ago
- lag_24h: Flight count 24 hours ago
- rolling_mean_6h: 6-hour rolling average

Missing hours (ingestion gaps) are handled explicitly: the hourly series is
reindexed onto a continuous hourly grid before shifting, so each lag step moves
exactly one hour in time. A lag that lands inside a gap becomes null rather than
silently borrowing an unrelated hour's count.
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


def densify_hourly(hourly_df: pl.DataFrame) -> pl.DataFrame:
    """
    Reindex hourly counts onto a continuous hourly grid.

    shift() works on row positions, not timestamps. If the input only contains
    hours we actually observed, then after an ingestion gap every later lag step
    lands on the wrong hour - a 28-hour-old count would be labelled lag_1h, and
    lag_24h would quietly point somewhere else entirely. Inserting the missing
    hours as null counts makes each shift step exactly one hour, so anything
    reaching into a gap resolves to null: an honest "unknown" rather than a
    wrong number.

    Args:
        hourly_df: DataFrame with hour_start and flight_count

    Returns:
        DataFrame covering every hour from first to last observation
    """
    if hourly_df.is_empty():
        return hourly_df

    ordered = hourly_df.sort("hour_start")
    grid = pl.DataFrame({
        "hour_start": pl.datetime_range(
            ordered["hour_start"].min(),
            ordered["hour_start"].max(),
            interval="1h",
            time_unit="us",
            eager=True,
        ),
    }).with_columns(pl.col("hour_start").cast(ordered.schema["hour_start"]))

    return grid.join(ordered, on="hour_start", how="left")


def create_features(hourly_df: pl.DataFrame) -> pl.DataFrame:
    """
    Create forecasting features from hourly aggregates.

    Args:
        hourly_df: DataFrame with hour_start and flight_count

    Returns:
        DataFrame with all features added
    """
    df = densify_hourly(hourly_df)

    df = df.with_columns([
        # Time features
        pl.col("hour_start").dt.hour().alias("hour_of_day"),
        pl.col("hour_start").dt.weekday().alias("day_of_week"),
        (pl.col("hour_start").dt.weekday() >= 5).cast(pl.Int32).alias("is_weekend"),

        # Lag features - time-true, because the frame is a continuous hourly grid
        pl.col("flight_count").shift(1).alias("lag_1h"),
        pl.col("flight_count").shift(24).alias("lag_24h"),

        # Rolling mean (6 hour window for quarter-day patterns)
        pl.col("flight_count").shift(1).rolling_mean(window_size=6).alias("rolling_mean_6h"),
    ])

    # Keep every hour we actually observed. Requiring *all* lags to be present
    # meant a single 24-hour gap deleted an entire day of real targets, because
    # every lag_24h on the far side of the gap pointed into the hole. Missing
    # features are legitimate unknowns and XGBoost handles them natively, so we
    # only drop rows that have no predictive context at all.
    df = df.filter(
        pl.col("flight_count").is_not_null()
        & (
            pl.col("lag_1h").is_not_null()
            | pl.col("lag_24h").is_not_null()
            | pl.col("rolling_mean_6h").is_not_null()
        )
    )

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
    "create_features",
    "create_hourly_aggregates",
    "densify_hourly",
    "prepare_daily_features",
]

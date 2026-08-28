"""Unit tests for feature engineering pipeline and calculations."""

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from src.features.data.cleaning import (
    add_derived_columns,
    clean_flight_data,
    get_data_summary,
)
from src.pipeline.features import (
    create_features,
    create_hourly_aggregates,
)


def test_create_hourly_aggregates():
    """Test grouping raw flight state timestamps into hourly unique flight counts."""
    # Base timestamp: 2025-12-28 00:00:00 UTC (1766880000)
    base_epoch = int(datetime(2025, 12, 28, 0, 0, 0, tzinfo=timezone.utc).timestamp())

    # Hour 0: 3 events (2 distinct aircraft: plane1, plane2)
    # Hour 1: 2 events (1 distinct aircraft: plane1)
    raw_data = {
        "capture_time": [
            base_epoch + 10,
            base_epoch + 600,
            base_epoch + 1200,
            base_epoch + 3600 + 5,
            base_epoch + 3600 + 100,
        ],
        "icao24": ["plane1", "plane2", "plane1", "plane1", "plane1"],
    }
    df = pl.DataFrame(raw_data)
    hourly = create_hourly_aggregates(df)

    assert len(hourly) == 2
    assert "hour_start" in hourly.columns
    assert "flight_count" in hourly.columns

    counts = hourly["flight_count"].to_list()
    assert counts == [2, 1]


def test_create_features():
    """Test generating time features, lag features, and rolling mean windows."""
    # Generate 35 continuous hours of datetime values
    base_dt = datetime(2025, 12, 28, 0, 0, 0)
    date_list = [base_dt + timedelta(hours=i) for i in range(35)]

    # Simple monotonic flight counts
    counts = list(range(10, 45))

    hourly_df = pl.DataFrame({
        "hour_start": date_list,
        "flight_count": counts,
    })

    featured = create_features(hourly_df)

    # We expect rows with null lags/rolling window to be dropped
    # lag_24h requires 24 previous rows, so 35 - 24 = 11 rows remain
    assert len(featured) == 11

    expected_cols = {
        "hour_start", "flight_count", "hour_of_day", "day_of_week",
        "is_weekend", "lag_1h", "lag_24h", "rolling_mean_6h"
    }
    assert expected_cols.issubset(set(featured.columns))

    # Verify lag values on the first surviving row (index 24 of original, count 34)
    first_row = featured.row(0, named=True)
    assert first_row["flight_count"] == counts[24]
    assert first_row["lag_1h"] == counts[23]
    assert first_row["lag_24h"] == counts[0]

    # rolling_mean_6h of counts[18:24]
    expected_rolling = sum(counts[18:24]) / 6.0
    assert pytest.approx(first_row["rolling_mean_6h"]) == expected_rolling


def test_clean_flight_data():
    """Test filtering invalid coordinates, velocities, and null identifiers."""
    raw = pl.DataFrame({
        "icao24": ["valid1", None, "valid2", "valid3", "valid4", "valid1"],
        "latitude": [19.0, 19.0, None, 95.0, 19.0, 19.0],      # invalid lat: None, 95.0
        "longitude": [72.8, 72.8, 72.8, 72.8, -200.0, 72.8],    # invalid lon: -200.0
        "baro_altitude": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0],
        "velocity": [150.0, 150.0, 150.0, 150.0, 150.0, 150.0],
    })

    cleaned = clean_flight_data(raw)

    # Only valid1 remains (the duplicate valid1 row is removed by unique())
    assert len(cleaned) == 1
    assert cleaned["icao24"][0] == "valid1"


def test_add_derived_columns():
    """Test column enrichment including datetime parsing and unit conversions."""
    base_epoch = int(datetime(2025, 12, 28, 14, 30, 0, tzinfo=timezone.utc).timestamp())
    df = pl.DataFrame({
        "capture_time": [base_epoch],
        "velocity": [100.0],       # 100 m/s = 360 km/h
        "baro_altitude": [1000.0], # 1000 m ≈ 3280.84 ft
        "vertical_rate": [5.0],    # > 2 m/s -> climbing
    })

    enriched = add_derived_columns(df)

    assert "capture_datetime" in enriched.columns
    assert "hour" in enriched.columns
    assert "speed_kmh" in enriched.columns
    assert "altitude_ft" in enriched.columns
    assert "flight_phase" in enriched.columns

    row = enriched.row(0, named=True)
    assert pytest.approx(row["speed_kmh"]) == 360.0
    assert pytest.approx(row["altitude_ft"], rel=1e-3) == 3280.84
    assert row["flight_phase"] == "climbing"


def test_get_data_summary():
    """Test summary statistics extraction from flight dataset."""
    df = pl.DataFrame({
        "icao24": ["a1", "a2", "a1"],
        "baro_altitude": [1000.0, 2000.0, 3000.0],
        "velocity": [100.0, 200.0, 300.0],
    })

    summary = get_data_summary(df)
    assert summary["total_records"] == 3
    assert summary["unique_aircraft"] == 2
    assert summary["baro_altitude_mean"] == 2000.0
    assert summary["velocity_mean"] == 200.0

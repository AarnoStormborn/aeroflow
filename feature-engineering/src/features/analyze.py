"""
Data analysis script for flight traffic.

Usage:
    uv run python -m src.features.analyze --date 2025-12-22
"""

import argparse
from datetime import date, datetime

from loguru import logger

from src.features.data import create_loader, clean_flight_data, add_derived_columns, get_data_summary
from src.features.report import create_analysis_report


def analyze_day(target_date: date) -> None:
    """
    Run analysis for a specific day.
    
    Args:
        target_date: Date to analyze
    """
    logger.info(f"Starting analysis for {target_date}")
    
    # 1. Load data
    loader = create_loader()
    raw_df = loader.load_day(target_date)
    
    if raw_df.is_empty():
        logger.error(f"No data available for {target_date}")
        return
    
    logger.info(f"Loaded {len(raw_df)} raw records")
    
    # 2. Clean data
    clean_df = clean_flight_data(raw_df)
    
    # 3. Add derived columns
    enriched_df = add_derived_columns(clean_df)
    
    # 4. Print summary
    summary = get_data_summary(enriched_df)
    print("\n" + "=" * 60)
    print(f"DATA SUMMARY FOR {target_date}")
    print("=" * 60)
    print(f"Total records: {summary['total_records']:,}")
    print(f"Unique aircraft: {summary['unique_aircraft']:,}")
    print(f"Columns: {len(summary['columns'])}")
    
    if "latitude_min" in summary:
        print(f"\nLatitude range: {summary['latitude_min']:.4f} to {summary['latitude_max']:.4f}")
        print(f"Longitude range: {summary['longitude_min']:.4f} to {summary['longitude_max']:.4f}")
    
    if "baro_altitude_mean" in summary:
        print(f"\nAltitude mean: {summary['baro_altitude_mean']:.0f} m")
    
    if "velocity_mean" in summary:
        print(f"Velocity mean: {summary['velocity_mean']:.1f} m/s ({summary['velocity_mean']*3.6:.1f} km/h)")
    
    print("=" * 60 + "\n")
    
    # 5. Create visualizations
    create_analysis_report(
        enriched_df,
        output_dir="reports",
        date_str=target_date.isoformat(),
    )
    
    logger.info("Analysis complete!")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Analyze flight traffic data")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to analyze (YYYY-MM-DD). Defaults to yesterday.",
    )
    
    args = parser.parse_args()
    
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        # Default to yesterday
        target_date = date.today()
    
    analyze_day(target_date)


if __name__ == "__main__":
    main()

"""
Daily report pipeline.

Generates a complete daily report:
1. Loads and cleans data from S3
2. Generates visualization plots
3. Creates PDF report
4. Uploads cleaned data + PDF to S3
5. Sends Slack notification with download links

Usage:
    uv run python -m src.features.daily_report --date 2025-12-21
"""

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

from loguru import logger

from src.features.data import create_loader, clean_flight_data, add_derived_columns, get_data_summary
from src.features.report import (
    plot_geographic_distribution,
    plot_hourly_traffic,
    plot_altitude_distribution,
    plot_speed_distribution,
    create_report_generator,
    create_uploader,
    create_slack_notifier,
)


def generate_daily_report(target_date: date) -> None:
    """
    Generate and publish a daily report.
    
    Args:
        target_date: Date to generate report for
    """
    logger.info(f"=" * 60)
    logger.info(f"GENERATING DAILY REPORT FOR {target_date}")
    logger.info(f"=" * 60)
    
    slack = create_slack_notifier()
    
    try:
        # 1. Load data from S3
        logger.info("Step 1/5: Loading data from S3...")
        loader = create_loader()
        raw_df = loader.load_day(target_date)
        
        if raw_df.is_empty():
            raise ValueError(f"No data available for {target_date}")
        
        logger.info(f"Loaded {len(raw_df)} raw records")
        
        # 2. Clean and enrich data
        logger.info("Step 2/5: Cleaning and enriching data...")
        clean_df = clean_flight_data(raw_df)
        enriched_df = add_derived_columns(clean_df)
        
        summary = get_data_summary(enriched_df)
        logger.info(f"Cleaned data: {summary['total_records']} records, {summary['unique_aircraft']} aircraft")
        
        # 3. Generate plots
        logger.info("Step 3/5: Generating visualizations...")
        plots_dir = Path("reports") / target_date.isoformat()
        plots_dir.mkdir(parents=True, exist_ok=True)
        
        plot_paths = {}
        
        plot_paths["geographic"] = plots_dir / "geographic.png"
        plot_geographic_distribution(
            enriched_df,
            save_path=str(plot_paths["geographic"]),
            title=f"Flight Distribution - {target_date}",
        )
        
        plot_paths["hourly"] = plots_dir / "hourly.png"
        plot_hourly_traffic(
            enriched_df,
            save_path=str(plot_paths["hourly"]),
            title=f"Hourly Traffic - {target_date}",
        )
        
        plot_paths["altitude"] = plots_dir / "altitude.png"
        plot_altitude_distribution(
            enriched_df,
            save_path=str(plot_paths["altitude"]),
            title=f"Altitude Distribution - {target_date}",
        )
        
        plot_paths["speed"] = plots_dir / "speed.png"
        plot_speed_distribution(
            enriched_df,
            save_path=str(plot_paths["speed"]),
            title=f"Speed Distribution - {target_date}",
        )
        
        # 4. Generate PDF report
        logger.info("Step 4/5: Generating PDF report...")
        report_gen = create_report_generator(output_dir=str(plots_dir))
        pdf_path = report_gen.generate_report(enriched_df, target_date, plot_paths)
        
        # 5. Upload to S3
        logger.info("Step 5/5: Uploading to S3...")
        uploader = create_uploader()
        
        data_s3_url, _ = uploader.upload_cleaned_data(enriched_df, target_date)
        pdf_s3_url, _ = uploader.upload_pdf_report(pdf_path, target_date)
        
        # Generate presigned URLs for sharing
        data_presigned = uploader.generate_presigned_url(data_s3_url)
        pdf_presigned = uploader.generate_presigned_url(pdf_s3_url)
        
        # 6. Send Slack notification
        logger.info("Sending Slack notification...")
        slack.notify_report_ready(
            report_date=target_date,
            record_count=summary["total_records"],
            aircraft_count=summary["unique_aircraft"],
            pdf_url=pdf_presigned,
            data_url=data_presigned,
        )
        
        logger.info("=" * 60)
        logger.info("DAILY REPORT COMPLETE")
        logger.info(f"PDF: {pdf_s3_url}")
        logger.info(f"Data: {data_s3_url}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Failed to generate daily report: {e}")
        slack.notify_report_failed(target_date, str(e))
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate daily flight traffic report")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to generate report for (YYYY-MM-DD). Defaults to yesterday.",
    )
    
    args = parser.parse_args()
    
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        # Default to yesterday
        target_date = date.today() - timedelta(days=1)
    
    generate_daily_report(target_date)


if __name__ == "__main__":
    main()

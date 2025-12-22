"""
Feature engineering module for flight traffic forecasting.

Submodules:
- data: Load and clean flight data from S3
- report: Generate PDF reports, upload to S3, send Slack notifications

Scripts:
- analyze: Run data analysis for a specific day
- daily_report: Generate and publish full daily report
"""

__version__ = "0.1.0"

from src.features.config import settings

# Re-export from submodules for convenience
from src.features.data import (
    S3DataLoader,
    create_loader,
    clean_flight_data,
    add_derived_columns,
    get_data_summary,
)
from src.features.report import (
    plot_geographic_distribution,
    plot_hourly_traffic,
    plot_altitude_distribution,
    plot_speed_distribution,
    create_analysis_report,
    PDFReportGenerator,
    create_report_generator,
    ReportUploader,
    create_uploader,
    SlackNotifier,
    create_slack_notifier,
)

__all__ = [
    # Config
    "settings",
    # Data
    "S3DataLoader",
    "create_loader",
    "clean_flight_data",
    "add_derived_columns",
    "get_data_summary",
    # Report
    "plot_geographic_distribution",
    "plot_hourly_traffic",
    "plot_altitude_distribution",
    "plot_speed_distribution",
    "create_analysis_report",
    "PDFReportGenerator",
    "create_report_generator",
    "ReportUploader",
    "create_uploader",
    "SlackNotifier",
    "create_slack_notifier",
]

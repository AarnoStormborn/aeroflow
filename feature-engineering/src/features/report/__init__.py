"""
Report submodule for generating, uploading, and notifying about reports.
"""

from src.features.report.visualization import (
    plot_geographic_distribution,
    plot_hourly_traffic,
    plot_altitude_distribution,
    plot_speed_distribution,
    create_analysis_report,
)
from src.features.report.report_generator import PDFReportGenerator, create_report_generator
from src.features.report.report_uploader import ReportUploader, create_uploader
from src.features.report.slack_notifier import SlackNotifier, create_slack_notifier

__all__ = [
    # Visualization
    "plot_geographic_distribution",
    "plot_hourly_traffic",
    "plot_altitude_distribution",
    "plot_speed_distribution",
    "create_analysis_report",
    # PDF
    "PDFReportGenerator",
    "create_report_generator",
    # Upload
    "ReportUploader",
    "create_uploader",
    # Slack
    "SlackNotifier",
    "create_slack_notifier",
]

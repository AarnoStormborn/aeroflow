"""
PDF report generator for daily flight analysis.

Creates multi-page PDF reports with:
- Summary statistics
- All visualization plots
- Data quality metrics
"""

from datetime import date
from pathlib import Path
import io
import tempfile

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, 
    Paragraph, 
    Spacer, 
    Image, 
    Table, 
    TableStyle,
    PageBreak,
)
import polars as pl
from loguru import logger

from src.features.data import get_data_summary


class PDFReportGenerator:
    """Generates PDF reports from flight data analysis."""
    
    def __init__(self, output_dir: str = "reports"):
        """Initialize report generator."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.styles = getSampleStyleSheet()
        
        # Custom styles
        self.styles.add(ParagraphStyle(
            'Title_Custom',
            parent=self.styles['Title'],
            fontSize=24,
            spaceAfter=30,
        ))
        self.styles.add(ParagraphStyle(
            'Heading2_Custom',
            parent=self.styles['Heading2'],
            fontSize=16,
            spaceBefore=20,
            spaceAfter=10,
        ))
    
    def generate_report(
        self,
        df: pl.DataFrame,
        report_date: date,
        plot_paths: dict[str, Path],
    ) -> Path:
        """
        Generate a PDF report.
        
        Args:
            df: Cleaned flight data
            report_date: Date of the report
            plot_paths: Dictionary of plot name -> file path
            
        Returns:
            Path to the generated PDF
        """
        pdf_path = self.output_dir / f"flight_report_{report_date.isoformat()}.pdf"
        
        doc = SimpleDocTemplate(
            str(pdf_path),
            pagesize=A4,
            rightMargin=50,
            leftMargin=50,
            topMargin=50,
            bottomMargin=50,
        )
        
        story = []
        
        # Title
        story.append(Paragraph(
            f"Flight Traffic Analysis Report",
            self.styles['Title_Custom']
        ))
        story.append(Paragraph(
            f"Mumbai Airspace - {report_date.strftime('%B %d, %Y')}",
            self.styles['Heading2']
        ))
        story.append(Spacer(1, 20))
        
        # Summary Statistics
        story.append(Paragraph("Executive Summary", self.styles['Heading2_Custom']))
        
        summary = get_data_summary(df)
        summary_data = [
            ["Metric", "Value"],
            ["Total Records", f"{summary['total_records']:,}"],
            ["Unique Aircraft", f"{summary['unique_aircraft']:,}"],
            ["Data Columns", f"{len(summary['columns'])}"],
        ]
        
        if "latitude_min" in summary:
            summary_data.append(["Latitude Range", f"{summary['latitude_min']:.4f}° to {summary['latitude_max']:.4f}°"])
            summary_data.append(["Longitude Range", f"{summary['longitude_min']:.4f}° to {summary['longitude_max']:.4f}°"])
        
        if "baro_altitude_mean" in summary:
            summary_data.append(["Mean Altitude", f"{summary['baro_altitude_mean']:,.0f} m"])
        
        if "velocity_mean" in summary:
            summary_data.append(["Mean Speed", f"{summary['velocity_mean']*3.6:,.1f} km/h"])
        
        summary_table = Table(summary_data, colWidths=[2.5*inch, 3*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E86AB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F0F0F0')),
            ('GRID', (0, 0), (-1, -1), 1, colors.white),
            ('FONTSIZE', (0, 1), (-1, -1), 11),
            ('TOPPADDING', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
        ]))
        story.append(summary_table)
        story.append(PageBreak())
        
        # Add plots
        plot_order = [
            ("geographic", "Geographic Distribution"),
            ("hourly", "Hourly Traffic Pattern"),
            ("altitude", "Altitude Distribution"),
            ("speed", "Speed Distribution"),
        ]
        
        for plot_key, plot_title in plot_order:
            if plot_key in plot_paths and plot_paths[plot_key].exists():
                story.append(Paragraph(plot_title, self.styles['Heading2_Custom']))
                
                # Scale image to fit page width
                img = Image(str(plot_paths[plot_key]), width=6*inch, height=4*inch)
                story.append(img)
                story.append(Spacer(1, 20))
        
        # Build PDF
        doc.build(story)
        logger.info(f"Generated PDF report: {pdf_path}")
        
        return pdf_path


def create_report_generator(output_dir: str = "reports") -> PDFReportGenerator:
    """Create a new PDF report generator."""
    return PDFReportGenerator(output_dir)


__all__ = ["PDFReportGenerator", "create_report_generator"]

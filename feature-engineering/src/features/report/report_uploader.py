"""
S3 uploader for reports and cleaned data.

Uploads:
- Cleaned Parquet data files
- PDF reports
"""

from datetime import date
from pathlib import Path
import io

import boto3
import polars as pl
from loguru import logger

from src.features.config import settings


class ReportUploader:
    """Uploads reports and processed data to S3."""
    
    def __init__(self):
        """Initialize S3 client."""
        self.bucket = settings.s3.bucket_name
        self.client = boto3.client(
            "s3",
            region_name=settings.s3.region,
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
        )
        logger.info(f"ReportUploader initialized (bucket: {self.bucket})")
    
    def _get_s3_url(self, key: str) -> str:
        """Get the S3 URL for a key."""
        return f"s3://{self.bucket}/{key}"
    
    def _get_https_url(self, key: str) -> str:
        """Get the HTTPS URL for a key (for sharing)."""
        return f"https://{self.bucket}.s3.{settings.s3.region}.amazonaws.com/{key}"
    
    def upload_cleaned_data(
        self,
        df: pl.DataFrame,
        report_date: date,
    ) -> tuple[str, str]:
        """
        Upload cleaned data to S3.
        
        Args:
            df: Cleaned DataFrame
            report_date: Date of the data
            
        Returns:
            Tuple of (S3 URL, HTTPS URL)
        """
        key = f"processed/flights/cleaned/year={report_date.year}/month={report_date.month:02d}/{report_date.isoformat()}.parquet"
        
        # Write to bytes buffer
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        
        try:
            self.client.upload_fileobj(
                buffer,
                self.bucket,
                key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )
            
            s3_url = self._get_s3_url(key)
            https_url = self._get_https_url(key)
            
            logger.info(f"Uploaded cleaned data: {s3_url}")
            return s3_url, https_url
            
        except Exception as e:
            logger.error(f"Failed to upload cleaned data: {e}")
            raise
    
    def upload_pdf_report(
        self,
        pdf_path: Path,
        report_date: date,
    ) -> tuple[str, str]:
        """
        Upload PDF report to S3.
        
        Args:
            pdf_path: Path to the PDF file
            report_date: Date of the report
            
        Returns:
            Tuple of (S3 URL, HTTPS URL)
        """
        key = f"reports/daily/{report_date.year}/{report_date.month:02d}/flight_report_{report_date.isoformat()}.pdf"
        
        try:
            self.client.upload_file(
                str(pdf_path),
                self.bucket,
                key,
                ExtraArgs={"ContentType": "application/pdf"},
            )
            
            s3_url = self._get_s3_url(key)
            https_url = self._get_https_url(key)
            
            logger.info(f"Uploaded PDF report: {s3_url}")
            return s3_url, https_url
            
        except Exception as e:
            logger.error(f"Failed to upload PDF report: {e}")
            raise
    
    def generate_presigned_url(self, s3_url: str, expiration: int = 86400) -> str:
        """
        Generate a presigned URL for downloading (valid 24 hours by default).
        
        Args:
            s3_url: S3 URL (s3://bucket/key)
            expiration: URL expiration in seconds
            
        Returns:
            Presigned HTTPS URL
        """
        # Parse s3:// URL
        key = s3_url.replace(f"s3://{self.bucket}/", "")
        
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expiration,
            )
            return url
        except Exception as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return s3_url


def create_uploader() -> ReportUploader:
    """Create a new report uploader."""
    return ReportUploader()


__all__ = ["ReportUploader", "create_uploader"]

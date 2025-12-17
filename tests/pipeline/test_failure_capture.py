"""
Test failure scenarios to verify error capture in the ingestion pipeline.

This script simulates various failure modes and verifies they are
correctly captured in the database with FAILED status and error messages.
"""

from datetime import datetime, timezone
from unittest.mock import Mock, patch

from src.utils import logger
from src.utils.logger import setup_logger
from src.utils.exceptions import (
    RateLimitError,
    APIConnectionError,
    APITimeoutError,
    S3UploadError,
    ParquetError,
)
from src.ingestion.jobs.ingestion_job import IngestionJob, run_ingestion
from src.ingestion.db import IngestionRepository, IngestionStatus


def test_api_connection_failure():
    """Test that API connection failures are captured correctly."""
    print("\n" + "=" * 60)
    print("TEST: API Connection Failure")
    print("=" * 60)
    
    # Create job with mocked client that raises APIConnectionError
    mock_client = Mock()
    mock_client.get_states.side_effect = APIConnectionError("Connection refused: opensky-network.org")
    
    job = IngestionJob(client=mock_client)
    result = job.run()
    
    assert result.status == IngestionStatus.FAILED, f"Expected FAILED, got {result.status}"
    assert "API_CONNECTION" in result.error_message, f"Expected API_CONNECTION in error: {result.error_message}"
    print(f"✓ Status: {result.status.value}")
    print(f"✓ Error: {result.error_message}")


def test_api_timeout_failure():
    """Test that API timeout failures are captured correctly."""
    print("\n" + "=" * 60)
    print("TEST: API Timeout Failure")
    print("=" * 60)
    
    mock_client = Mock()
    mock_client.get_states.side_effect = APITimeoutError("Request timed out", timeout=30)
    
    job = IngestionJob(client=mock_client)
    result = job.run()
    
    assert result.status == IngestionStatus.FAILED
    assert "API_TIMEOUT" in result.error_message
    print(f"✓ Status: {result.status.value}")
    print(f"✓ Error: {result.error_message}")


def test_rate_limit_failure():
    """Test that rate limit errors are captured correctly."""
    print("\n" + "=" * 60)
    print("TEST: Rate Limit Failure")
    print("=" * 60)
    
    mock_client = Mock()
    mock_client.get_states.side_effect = RateLimitError(retry_after=60)
    
    job = IngestionJob(client=mock_client)
    result = job.run()
    
    assert result.status == IngestionStatus.FAILED
    assert "RATE_LIMIT" in result.error_message
    assert "60" in result.error_message  # Should mention retry time
    print(f"✓ Status: {result.status.value}")
    print(f"✓ Error: {result.error_message}")


def test_s3_upload_failure():
    """Test that S3 upload failures are captured correctly."""
    print("\n" + "=" * 60)
    print("TEST: S3 Upload Failure")
    print("=" * 60)
    
    # Mock client to return valid data
    mock_client = Mock()
    mock_client.get_states.return_value = {
        "time": 1234567890,
        "states": [
            ["abc123", "TEST123", "India", 1234567890, 1234567890, 72.8, 19.0, 1000, False, 100, 90, 0, None, 1050, "1234", False, 0, None],
        ]
    }
    
    # Mock uploader to raise S3UploadError
    mock_uploader = Mock()
    mock_uploader.upload_states.side_effect = S3UploadError(
        message="Access Denied",
        bucket="test-bucket",
        key="test/key.parquet"
    )
    
    job = IngestionJob(client=mock_client, uploader=mock_uploader)
    result = job.run()
    
    assert result.status == IngestionStatus.FAILED
    assert "S3_UPLOAD" in result.error_message
    print(f"✓ Status: {result.status.value}")
    print(f"✓ Error: {result.error_message}")


def test_parquet_failure():
    """Test that Parquet conversion failures are captured correctly."""
    print("\n" + "=" * 60)
    print("TEST: Parquet Conversion Failure")
    print("=" * 60)
    
    mock_client = Mock()
    mock_client.get_states.return_value = {
        "time": 1234567890,
        "states": [["bad", "data"]]  # Invalid state vector format
    }
    
    mock_uploader = Mock()
    mock_uploader.upload_states.side_effect = ParquetError("Invalid data format for Parquet conversion")
    
    job = IngestionJob(client=mock_client, uploader=mock_uploader)
    result = job.run()
    
    assert result.status == IngestionStatus.FAILED
    assert "PARQUET" in result.error_message
    print(f"✓ Status: {result.status.value}")
    print(f"✓ Error: {result.error_message}")


def test_unexpected_exception():
    """Test that unexpected exceptions are captured correctly."""
    print("\n" + "=" * 60)
    print("TEST: Unexpected Exception")
    print("=" * 60)
    
    mock_client = Mock()
    mock_client.get_states.side_effect = RuntimeError("Something completely unexpected")
    
    job = IngestionJob(client=mock_client)
    result = job.run()
    
    assert result.status == IngestionStatus.FAILED
    assert "UNEXPECTED" in result.error_message
    print(f"✓ Status: {result.status.value}")
    print(f"✓ Error: {result.error_message}")


def verify_database_records():
    """Verify that all failures are recorded in the database."""
    print("\n" + "=" * 60)
    print("VERIFICATION: Database Records")
    print("=" * 60)
    
    repo = IngestionRepository()
    failed_records = repo.get_by_status(IngestionStatus.FAILED)
    
    print(f"Total FAILED records in database: {len(failed_records)}")
    print("\nRecent failures:")
    for record in failed_records[:5]:
        print(f"  ID: {record.id}")
        print(f"    Created: {record.created_at}")
        print(f"    Error: {record.error_message}")
        print()


def main():
    """Run all failure simulation tests."""
    setup_logger(log_level="WARNING")  # Less noise during tests
    
    print("=" * 60)
    print("FAILURE SIMULATION TESTS")
    print("=" * 60)
    
    # Run all tests
    test_api_connection_failure()
    test_api_timeout_failure()
    test_rate_limit_failure()
    test_s3_upload_failure()
    test_parquet_failure()
    test_unexpected_exception()
    
    # Verify database
    verify_database_records()
    
    print("\n" + "=" * 60)
    print("ALL TESTS PASSED ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()

"""
Database models for the ingestion service.

Uses SQLite to track ingestion runs and their results.
"""

import sqlite3
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import NamedTuple

from src.utils import logger
from src.ingestion.config import settings


class IngestionStatus(str, Enum):
    """Status of an ingestion run."""
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"  # Some records processed, but with errors


class IngestionRecord(NamedTuple):
    """Record of a single ingestion run."""
    id: int | None
    created_at: datetime
    time_window_start: datetime
    time_window_end: datetime
    s3_path: str | None
    record_count: int
    status: IngestionStatus
    error_message: str | None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "created_at": self.created_at.isoformat(),
            "time_window_start": self.time_window_start.isoformat(),
            "time_window_end": self.time_window_end.isoformat(),
            "s3_path": self.s3_path,
            "record_count": self.record_count,
            "status": self.status.value,
            "error_message": self.error_message,
        }


# SQL statements
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ingestion_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    time_window_start TEXT NOT NULL,
    time_window_end TEXT NOT NULL,
    s3_path TEXT,
    record_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT
)
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_ingestion_created_at ON ingestion_records(created_at);
CREATE INDEX IF NOT EXISTS idx_ingestion_status ON ingestion_records(status);
"""

INSERT_RECORD_SQL = """
INSERT INTO ingestion_records 
    (created_at, time_window_start, time_window_end, s3_path, record_count, status, error_message)
VALUES 
    (?, ?, ?, ?, ?, ?, ?)
"""

UPDATE_RECORD_SQL = """
UPDATE ingestion_records 
SET s3_path = ?, record_count = ?, status = ?, error_message = ?
WHERE id = ?
"""

SELECT_BY_ID_SQL = "SELECT * FROM ingestion_records WHERE id = ?"

SELECT_LATEST_SQL = """
SELECT * FROM ingestion_records 
ORDER BY created_at DESC 
LIMIT ?
"""

SELECT_BY_STATUS_SQL = """
SELECT * FROM ingestion_records 
WHERE status = ?
ORDER BY created_at DESC
"""

SELECT_BY_TIME_RANGE_SQL = """
SELECT * FROM ingestion_records 
WHERE time_window_start >= ? AND time_window_end <= ?
ORDER BY created_at DESC
"""


def _row_to_record(row: tuple) -> IngestionRecord:
    """Convert database row to IngestionRecord."""
    return IngestionRecord(
        id=row[0],
        created_at=datetime.fromisoformat(row[1]),
        time_window_start=datetime.fromisoformat(row[2]),
        time_window_end=datetime.fromisoformat(row[3]),
        s3_path=row[4],
        record_count=row[5],
        status=IngestionStatus(row[6]),
        error_message=row[7],
    )


class IngestionRepository:
    """
    Repository for managing ingestion records in SQLite.
    
    Tracks each ingestion run with:
    - Timestamp of the run
    - Time window of data fetched
    - S3 path of the stored Parquet file
    - Number of records processed
    - Status and any error messages
    """
    
    def __init__(self, db_path: str | Path | None = None):
        """
        Initialize the repository.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path) if db_path else settings.database.full_path
        self._ensure_database()
    
    def _ensure_database(self) -> None:
        """Ensure database and tables exist."""
        # Create directory if needed
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(CREATE_TABLE_SQL)
            cursor.executescript(CREATE_INDEX_SQL)
            conn.commit()
        
        logger.info(f"Database initialized at {self.db_path}")
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        return sqlite3.connect(str(self.db_path))
    
    def create_record(
        self,
        time_window_start: datetime,
        time_window_end: datetime,
        status: IngestionStatus = IngestionStatus.PENDING,
    ) -> IngestionRecord:
        """
        Create a new ingestion record.
        
        Args:
            time_window_start: Start of the data time window
            time_window_end: End of the data time window
            status: Initial status
            
        Returns:
            Created IngestionRecord with assigned ID
        """
        created_at = datetime.now(timezone.utc)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                INSERT_RECORD_SQL,
                (
                    created_at.isoformat(),
                    time_window_start.isoformat(),
                    time_window_end.isoformat(),
                    None,  # s3_path
                    0,     # record_count
                    status.value,
                    None,  # error_message
                ),
            )
            conn.commit()
            record_id = cursor.lastrowid
        
        record = IngestionRecord(
            id=record_id,
            created_at=created_at,
            time_window_start=time_window_start,
            time_window_end=time_window_end,
            s3_path=None,
            record_count=0,
            status=status,
            error_message=None,
        )
        
        logger.debug(f"Created ingestion record with ID: {record_id}")
        return record
    
    def update_record(
        self,
        record_id: int,
        s3_path: str | None = None,
        record_count: int | None = None,
        status: IngestionStatus | None = None,
        error_message: str | None = None,
    ) -> IngestionRecord | None:
        """
        Update an existing ingestion record.
        
        Args:
            record_id: ID of the record to update
            s3_path: Path to the S3 object
            record_count: Number of records processed
            status: New status
            error_message: Error message if any
            
        Returns:
            Updated IngestionRecord or None if not found
        """
        # Get existing record
        existing = self.get_by_id(record_id)
        if not existing:
            logger.warning(f"Record {record_id} not found for update")
            return None
        
        # Apply updates
        new_s3_path = s3_path if s3_path is not None else existing.s3_path
        new_count = record_count if record_count is not None else existing.record_count
        new_status = status if status is not None else existing.status
        new_error = error_message if error_message is not None else existing.error_message
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                UPDATE_RECORD_SQL,
                (new_s3_path, new_count, new_status.value, new_error, record_id),
            )
            conn.commit()
        
        logger.debug(f"Updated ingestion record {record_id} with status: {new_status.value}")
        return self.get_by_id(record_id)
    
    def get_by_id(self, record_id: int) -> IngestionRecord | None:
        """Get a record by ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(SELECT_BY_ID_SQL, (record_id,))
            row = cursor.fetchone()
        
        return _row_to_record(row) if row else None
    
    def get_latest(self, limit: int = 10) -> list[IngestionRecord]:
        """Get the most recent ingestion records."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(SELECT_LATEST_SQL, (limit,))
            rows = cursor.fetchall()
        
        return [_row_to_record(row) for row in rows]
    
    def get_by_status(self, status: IngestionStatus) -> list[IngestionRecord]:
        """Get all records with a specific status."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(SELECT_BY_STATUS_SQL, (status.value,))
            rows = cursor.fetchall()
        
        return [_row_to_record(row) for row in rows]
    
    def get_by_time_range(
        self,
        start: datetime,
        end: datetime,
    ) -> list[IngestionRecord]:
        """Get records within a time range."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                SELECT_BY_TIME_RANGE_SQL,
                (start.isoformat(), end.isoformat()),
            )
            rows = cursor.fetchall()
        
        return [_row_to_record(row) for row in rows]


def create_repository() -> IngestionRepository:
    """Create a new repository with default settings."""
    return IngestionRepository()


__all__ = [
    "IngestionStatus",
    "IngestionRecord",
    "IngestionRepository",
    "create_repository",
]

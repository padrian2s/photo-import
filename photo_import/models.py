"""
Database models and schema for photo import tool.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class FileStatus(str, Enum):
    """Status of a file in the import process."""
    PENDING = "pending"      # Scanned but not copied
    COPIED = "copied"        # Successfully copied
    FAILED = "failed"        # Copy failed
    SKIPPED = "skipped"      # Skipped (e.g., no EXIF date)


class BatchStatus(str, Enum):
    """Status of a batch operation."""
    SCANNING = "scanning"    # Currently scanning
    SCANNED = "scanned"      # Scan complete, ready for copy
    COPYING = "copying"      # Currently copying
    COMPLETED = "completed"  # All files copied
    FAILED = "failed"        # Batch failed
    PAUSED = "paused"        # Paused/interrupted


@dataclass
class PhotoFile:
    """Represents a photo file with its metadata."""
    id: Optional[int]
    batch_id: int
    source_path: str
    filename: str
    file_size: int
    file_extension: str
    exif_date: Optional[datetime]
    file_creation_date: datetime
    file_modification_date: datetime
    target_path: Optional[str]
    status: FileStatus
    error_message: Optional[str]
    scanned_at: datetime
    copied_at: Optional[datetime]
    checksum: Optional[str]  # MD5 for duplicate detection


@dataclass
class Batch:
    """Represents a batch of files to be processed."""
    id: Optional[int]
    source_directory: str
    target_directory: str
    status: BatchStatus
    total_files: int
    scanned_files: int
    copied_files: int
    failed_files: int
    skipped_files: int
    started_at: datetime
    scan_completed_at: Optional[datetime]
    copy_started_at: Optional[datetime]
    completed_at: Optional[datetime]
    last_processed_path: Optional[str]  # For resume capability


# SQL Schema
SCHEMA_SQL = """
-- Batches table to track import operations
CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_directory TEXT NOT NULL,
    target_directory TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'scanning',
    total_files INTEGER DEFAULT 0,
    scanned_files INTEGER DEFAULT 0,
    copied_files INTEGER DEFAULT 0,
    failed_files INTEGER DEFAULT 0,
    skipped_files INTEGER DEFAULT 0,
    started_at TIMESTAMP NOT NULL,
    scan_completed_at TIMESTAMP,
    copy_started_at TIMESTAMP,
    completed_at TIMESTAMP,
    last_processed_path TEXT
);

-- Photo files table to track individual files
CREATE TABLE IF NOT EXISTS photo_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL,
    source_path TEXT NOT NULL UNIQUE,
    filename TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    file_extension TEXT NOT NULL,
    exif_date TIMESTAMP,
    file_creation_date TIMESTAMP NOT NULL,
    file_modification_date TIMESTAMP NOT NULL,
    target_path TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    scanned_at TIMESTAMP NOT NULL,
    copied_at TIMESTAMP,
    checksum TEXT,
    FOREIGN KEY (batch_id) REFERENCES batches(id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_photo_files_batch_id ON photo_files(batch_id);
CREATE INDEX IF NOT EXISTS idx_photo_files_status ON photo_files(status);
CREATE INDEX IF NOT EXISTS idx_photo_files_source_path ON photo_files(source_path);
CREATE INDEX IF NOT EXISTS idx_photo_files_checksum ON photo_files(checksum);
CREATE INDEX IF NOT EXISTS idx_photo_files_exif_date ON photo_files(exif_date);
"""

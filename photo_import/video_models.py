"""
Database models and schema for video import tool.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class VideoFileStatus(str, Enum):
    """Status of a video file in the import process."""
    PENDING = "pending"      # Scanned but not copied
    COPIED = "copied"        # Successfully copied
    FAILED = "failed"        # Copy failed
    SKIPPED = "skipped"      # Skipped (e.g., no date)


class VideoBatchStatus(str, Enum):
    """Status of a batch operation."""
    SCANNING = "scanning"    # Currently scanning
    SCANNED = "scanned"      # Scan complete, ready for copy
    COPYING = "copying"      # Currently copying
    COMPLETED = "completed"  # All files copied
    FAILED = "failed"        # Batch failed
    PAUSED = "paused"        # Paused/interrupted


@dataclass
class VideoFile:
    """Represents a video file with its metadata."""
    id: Optional[int]
    batch_id: int
    source_path: str
    filename: str
    file_size: int
    file_extension: str
    metadata_date: Optional[datetime]
    file_creation_date: datetime
    file_modification_date: datetime
    target_path: Optional[str]
    status: VideoFileStatus
    error_message: Optional[str]
    scanned_at: datetime
    copied_at: Optional[datetime]
    checksum: Optional[str]  # MD5 for duplicate detection


@dataclass
class VideoBatch:
    """Represents a batch of video files to be processed."""
    id: Optional[int]
    source_directory: str
    target_directory: str
    status: VideoBatchStatus
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


# SQL Schema for video database
VIDEO_SCHEMA_SQL = """
-- Video batches table to track import operations
CREATE TABLE IF NOT EXISTS video_batches (
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

-- Video files table to track individual files
CREATE TABLE IF NOT EXISTS video_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL,
    source_path TEXT NOT NULL UNIQUE,
    filename TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    file_extension TEXT NOT NULL,
    metadata_date TIMESTAMP,
    file_creation_date TIMESTAMP NOT NULL,
    file_modification_date TIMESTAMP NOT NULL,
    target_path TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    scanned_at TIMESTAMP NOT NULL,
    copied_at TIMESTAMP,
    checksum TEXT,
    FOREIGN KEY (batch_id) REFERENCES video_batches(id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_video_files_batch_id ON video_files(batch_id);
CREATE INDEX IF NOT EXISTS idx_video_files_status ON video_files(status);
CREATE INDEX IF NOT EXISTS idx_video_files_source_path ON video_files(source_path);
CREATE INDEX IF NOT EXISTS idx_video_files_checksum ON video_files(checksum);
CREATE INDEX IF NOT EXISTS idx_video_files_metadata_date ON video_files(metadata_date);
"""

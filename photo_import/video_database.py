"""
Database operations for video import tool.
"""

import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from contextlib import contextmanager

from .video_models import (
    VideoFile, VideoBatch, VideoFileStatus, VideoBatchStatus, VIDEO_SCHEMA_SQL
)

logger = logging.getLogger(__name__)


class VideoDatabase:
    """SQLite database handler for video import operations."""

    def __init__(self, db_path: str = "video_import.db"):
        """Initialize database connection."""
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        with self._get_connection() as conn:
            conn.executescript(VIDEO_SCHEMA_SQL)
            conn.commit()

    @contextmanager
    def _get_connection(self):
        """Get a database connection with proper settings."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield conn
        finally:
            conn.close()

    def create_batch(
        self,
        source_directory: str,
        target_directory: str
    ) -> VideoBatch:
        """Create a new batch for processing."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO video_batches
                (source_directory, target_directory, status, started_at)
                VALUES (?, ?, ?, ?)
                """,
                (source_directory, target_directory, VideoBatchStatus.SCANNING.value, datetime.now())
            )
            conn.commit()

            return self.get_batch(cursor.lastrowid)

    def get_batch(self, batch_id: int) -> Optional[VideoBatch]:
        """Get a batch by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM video_batches WHERE id = ?",
                (batch_id,)
            ).fetchone()

            if row:
                return self._row_to_batch(row)
            return None

    def get_active_batch(self, source_directory: str) -> Optional[VideoBatch]:
        """Get an active (non-completed) batch for a source directory."""
        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM video_batches
                WHERE source_directory = ?
                AND status NOT IN ('completed', 'failed')
                ORDER BY id DESC LIMIT 1
                """,
                (source_directory,)
            ).fetchone()

            if row:
                return self._row_to_batch(row)
            return None

    def get_latest_batch(self) -> Optional[VideoBatch]:
        """Get the most recent batch."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM video_batches ORDER BY id DESC LIMIT 1"
            ).fetchone()

            if row:
                return self._row_to_batch(row)
            return None

    def list_batches(self, limit: int = 10) -> List[VideoBatch]:
        """List recent batches."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM video_batches ORDER BY id DESC LIMIT ?",
                (limit,)
            ).fetchall()

            return [self._row_to_batch(row) for row in rows]

    def update_batch_status(
        self,
        batch_id: int,
        status: VideoBatchStatus,
        total_files: Optional[int] = None,
        scanned_files: Optional[int] = None,
        last_processed_path: Optional[str] = None
    ):
        """Update batch status and counts."""
        updates = ["status = ?"]
        params = [status.value]

        if total_files is not None:
            updates.append("total_files = ?")
            params.append(total_files)

        if scanned_files is not None:
            updates.append("scanned_files = ?")
            params.append(scanned_files)

        if last_processed_path is not None:
            updates.append("last_processed_path = ?")
            params.append(last_processed_path)

        if status == VideoBatchStatus.SCANNED:
            updates.append("scan_completed_at = ?")
            params.append(datetime.now())
        elif status == VideoBatchStatus.COPYING:
            updates.append("copy_started_at = ?")
            params.append(datetime.now())
        elif status == VideoBatchStatus.COMPLETED:
            updates.append("completed_at = ?")
            params.append(datetime.now())

        params.append(batch_id)

        with self._get_connection() as conn:
            conn.execute(
                f"UPDATE video_batches SET {', '.join(updates)} WHERE id = ?",
                params
            )
            conn.commit()

    def update_batch_counts(self, batch_id: int):
        """Update batch file counts from actual file statuses."""
        with self._get_connection() as conn:
            stats = conn.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'copied' THEN 1 ELSE 0 END) as copied,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as skipped
                FROM video_files WHERE batch_id = ?
                """,
                (batch_id,)
            ).fetchone()

            conn.execute(
                """
                UPDATE video_batches
                SET total_files = ?, copied_files = ?, failed_files = ?, skipped_files = ?
                WHERE id = ?
                """,
                (stats['total'], stats['copied'], stats['failed'], stats['skipped'], batch_id)
            )
            conn.commit()

    def add_video_file(self, video: VideoFile) -> int:
        """Add a video file record."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO video_files
                (batch_id, source_path, filename, file_size, file_extension,
                 metadata_date, file_creation_date, file_modification_date,
                 status, scanned_at, checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    video.batch_id,
                    video.source_path,
                    video.filename,
                    video.file_size,
                    video.file_extension,
                    video.metadata_date,
                    video.file_creation_date,
                    video.file_modification_date,
                    video.status.value,
                    video.scanned_at,
                    video.checksum,
                )
            )
            conn.commit()
            return cursor.lastrowid

    def add_video_files_bulk(self, videos: List[VideoFile]):
        """Add multiple video file records in bulk."""
        with self._get_connection() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO video_files
                (batch_id, source_path, filename, file_size, file_extension,
                 metadata_date, file_creation_date, file_modification_date,
                 status, scanned_at, checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        v.batch_id,
                        v.source_path,
                        v.filename,
                        v.file_size,
                        v.file_extension,
                        v.metadata_date,
                        v.file_creation_date,
                        v.file_modification_date,
                        v.status.value,
                        v.scanned_at,
                        v.checksum,
                    )
                    for v in videos
                ]
            )
            conn.commit()

    def file_exists(self, source_path: str) -> bool:
        """Check if a file has already been processed."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM video_files WHERE source_path = ?",
                (source_path,)
            ).fetchone()
            return row is not None

    def get_pending_files(
        self,
        batch_id: int,
        limit: Optional[int] = None
    ) -> List[VideoFile]:
        """Get pending files for a batch."""
        query = """
            SELECT * FROM video_files
            WHERE batch_id = ? AND status = 'pending'
            ORDER BY id
        """
        if limit:
            query += f" LIMIT {limit}"

        with self._get_connection() as conn:
            rows = conn.execute(query, (batch_id,)).fetchall()
            return [self._row_to_file(row) for row in rows]

    def get_files_by_status(
        self,
        batch_id: int,
        status: VideoFileStatus,
        limit: Optional[int] = None
    ) -> List[VideoFile]:
        """Get files with a specific status."""
        query = """
            SELECT * FROM video_files
            WHERE batch_id = ? AND status = ?
            ORDER BY id
        """
        if limit:
            query += f" LIMIT {limit}"

        with self._get_connection() as conn:
            rows = conn.execute(query, (batch_id, status.value)).fetchall()
            return [self._row_to_file(row) for row in rows]

    def update_file_status(
        self,
        file_id: int,
        status: VideoFileStatus,
        target_path: Optional[str] = None,
        error_message: Optional[str] = None
    ):
        """Update file status."""
        updates = ["status = ?"]
        params = [status.value]

        if target_path is not None:
            updates.append("target_path = ?")
            params.append(target_path)

        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)

        if status == VideoFileStatus.COPIED:
            updates.append("copied_at = ?")
            params.append(datetime.now())

        params.append(file_id)

        with self._get_connection() as conn:
            conn.execute(
                f"UPDATE video_files SET {', '.join(updates)} WHERE id = ?",
                params
            )
            conn.commit()

    def get_batch_stats(self, batch_id: int) -> dict:
        """Get statistics for a batch."""
        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status = 'copied' THEN 1 ELSE 0 END) as copied,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as skipped,
                    SUM(CASE WHEN metadata_date IS NOT NULL THEN 1 ELSE 0 END) as with_metadata,
                    SUM(file_size) as total_size
                FROM video_files WHERE batch_id = ?
                """,
                (batch_id,)
            ).fetchone()

            return dict(row)

    def _row_to_batch(self, row: sqlite3.Row) -> VideoBatch:
        """Convert a database row to a VideoBatch object."""
        return VideoBatch(
            id=row['id'],
            source_directory=row['source_directory'],
            target_directory=row['target_directory'],
            status=VideoBatchStatus(row['status']),
            total_files=row['total_files'],
            scanned_files=row['scanned_files'],
            copied_files=row['copied_files'],
            failed_files=row['failed_files'],
            skipped_files=row['skipped_files'],
            started_at=self._parse_datetime(row['started_at']),
            scan_completed_at=self._parse_datetime(row['scan_completed_at']),
            copy_started_at=self._parse_datetime(row['copy_started_at']),
            completed_at=self._parse_datetime(row['completed_at']),
            last_processed_path=row['last_processed_path'],
        )

    def _row_to_file(self, row: sqlite3.Row) -> VideoFile:
        """Convert a database row to a VideoFile object."""
        return VideoFile(
            id=row['id'],
            batch_id=row['batch_id'],
            source_path=row['source_path'],
            filename=row['filename'],
            file_size=row['file_size'],
            file_extension=row['file_extension'],
            metadata_date=self._parse_datetime(row['metadata_date']),
            file_creation_date=self._parse_datetime(row['file_creation_date']),
            file_modification_date=self._parse_datetime(row['file_modification_date']),
            target_path=row['target_path'],
            status=VideoFileStatus(row['status']),
            error_message=row['error_message'],
            scanned_at=self._parse_datetime(row['scanned_at']),
            copied_at=self._parse_datetime(row['copied_at']),
            checksum=row['checksum'],
        )

    @staticmethod
    def _parse_datetime(value) -> Optional[datetime]:
        """Parse datetime from database value."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return None

"""
SQLite database layer for photo import tool.
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator, List, Optional

from .models import (
    Batch, BatchStatus, PhotoFile, FileStatus, SCHEMA_SQL
)

DEFAULT_DB_PATH = "photo_import.db"


class Database:
    """SQLite database manager for photo imports."""

    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self._init_db()

    def _init_db(self):
        """Initialize database with schema."""
        with self._get_connection() as conn:
            conn.executescript(SCHEMA_SQL)

    @contextmanager
    def _get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Get a database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # -------------------------------------------------------------------------
    # Batch Operations
    # -------------------------------------------------------------------------

    def create_batch(
        self,
        source_directory: str,
        target_directory: str,
    ) -> Batch:
        """Create a new batch for import operation."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO batches (source_directory, target_directory, status, started_at)
                VALUES (?, ?, ?, ?)
                """,
                (source_directory, target_directory, BatchStatus.SCANNING.value, datetime.now())
            )
            batch_id = cursor.lastrowid

        return self.get_batch(batch_id)

    def get_batch(self, batch_id: int) -> Optional[Batch]:
        """Get a batch by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()

        if not row:
            return None

        return self._row_to_batch(row)

    def get_active_batch(self, source_directory: str) -> Optional[Batch]:
        """Get an active (non-completed) batch for a source directory."""
        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM batches
                WHERE source_directory = ?
                AND status NOT IN (?, ?)
                ORDER BY id DESC LIMIT 1
                """,
                (source_directory, BatchStatus.COMPLETED.value, BatchStatus.FAILED.value)
            ).fetchone()

        if not row:
            return None

        return self._row_to_batch(row)

    def get_latest_batch(self) -> Optional[Batch]:
        """Get the most recent batch."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM batches ORDER BY id DESC LIMIT 1"
            ).fetchone()

        if not row:
            return None

        return self._row_to_batch(row)

    def list_batches(self, limit: int = 10) -> List[Batch]:
        """List recent batches."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM batches ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

        return [self._row_to_batch(row) for row in rows]

    def update_batch_status(
        self,
        batch_id: int,
        status: BatchStatus,
        **kwargs
    ):
        """Update batch status and optional fields."""
        fields = ["status = ?"]
        values = [status.value]

        # Handle optional timestamp fields
        if status == BatchStatus.SCANNED:
            fields.append("scan_completed_at = ?")
            values.append(datetime.now())
        elif status == BatchStatus.COPYING:
            fields.append("copy_started_at = ?")
            values.append(datetime.now())
        elif status == BatchStatus.COMPLETED:
            fields.append("completed_at = ?")
            values.append(datetime.now())

        # Handle additional kwargs
        for key, value in kwargs.items():
            if key in ('total_files', 'scanned_files', 'copied_files',
                       'failed_files', 'skipped_files', 'last_processed_path'):
                fields.append(f"{key} = ?")
                values.append(value)

        values.append(batch_id)

        with self._get_connection() as conn:
            conn.execute(
                f"UPDATE batches SET {', '.join(fields)} WHERE id = ?",
                values
            )

    def update_batch_counts(self, batch_id: int):
        """Update batch file counts from photo_files table."""
        with self._get_connection() as conn:
            # Get counts
            counts = conn.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as copied,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as skipped
                FROM photo_files WHERE batch_id = ?
                """,
                (FileStatus.COPIED.value, FileStatus.FAILED.value,
                 FileStatus.SKIPPED.value, batch_id)
            ).fetchone()

            conn.execute(
                """
                UPDATE batches SET
                    total_files = ?, scanned_files = ?,
                    copied_files = ?, failed_files = ?, skipped_files = ?
                WHERE id = ?
                """,
                (counts['total'], counts['total'], counts['copied'],
                 counts['failed'], counts['skipped'], batch_id)
            )

    def _row_to_batch(self, row: sqlite3.Row) -> Batch:
        """Convert database row to Batch object."""
        return Batch(
            id=row['id'],
            source_directory=row['source_directory'],
            target_directory=row['target_directory'],
            status=BatchStatus(row['status']),
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

    # -------------------------------------------------------------------------
    # PhotoFile Operations
    # -------------------------------------------------------------------------

    def add_photo_file(self, photo: PhotoFile) -> int:
        """Add a photo file record to the database."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO photo_files (
                    batch_id, source_path, filename, file_size, file_extension,
                    exif_date, file_creation_date, file_modification_date,
                    target_path, status, error_message, scanned_at, checksum
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    photo.batch_id, photo.source_path, photo.filename,
                    photo.file_size, photo.file_extension, photo.exif_date,
                    photo.file_creation_date, photo.file_modification_date,
                    photo.target_path, photo.status.value, photo.error_message,
                    photo.scanned_at, photo.checksum
                )
            )
            return cursor.lastrowid

    def add_photo_files_bulk(self, photos: List[PhotoFile]):
        """Add multiple photo files in a single transaction."""
        with self._get_connection() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO photo_files (
                    batch_id, source_path, filename, file_size, file_extension,
                    exif_date, file_creation_date, file_modification_date,
                    target_path, status, error_message, scanned_at, checksum
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        p.batch_id, p.source_path, p.filename,
                        p.file_size, p.file_extension, p.exif_date,
                        p.file_creation_date, p.file_modification_date,
                        p.target_path, p.status.value, p.error_message,
                        p.scanned_at, p.checksum
                    )
                    for p in photos
                ]
            )

    def get_photo_file(self, file_id: int) -> Optional[PhotoFile]:
        """Get a photo file by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM photo_files WHERE id = ?", (file_id,)
            ).fetchone()

        if not row:
            return None

        return self._row_to_photo_file(row)

    def get_photo_by_path(self, source_path: str) -> Optional[PhotoFile]:
        """Get a photo file by source path."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM photo_files WHERE source_path = ?", (source_path,)
            ).fetchone()

        if not row:
            return None

        return self._row_to_photo_file(row)

    def get_pending_files(
        self,
        batch_id: int,
        limit: Optional[int] = None
    ) -> List[PhotoFile]:
        """Get pending files for a batch."""
        query = """
            SELECT * FROM photo_files
            WHERE batch_id = ? AND status = ?
            ORDER BY source_path
        """
        params = [batch_id, FileStatus.PENDING.value]

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()

        return [self._row_to_photo_file(row) for row in rows]

    def get_files_by_status(
        self,
        batch_id: int,
        status: FileStatus,
        limit: Optional[int] = None
    ) -> List[PhotoFile]:
        """Get files by status for a batch."""
        query = """
            SELECT * FROM photo_files
            WHERE batch_id = ? AND status = ?
            ORDER BY source_path
        """
        params = [batch_id, status.value]

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()

        return [self._row_to_photo_file(row) for row in rows]

    def update_file_status(
        self,
        file_id: int,
        status: FileStatus,
        target_path: Optional[str] = None,
        error_message: Optional[str] = None,
    ):
        """Update a file's status."""
        fields = ["status = ?"]
        values = [status.value]

        if target_path is not None:
            fields.append("target_path = ?")
            values.append(target_path)

        if error_message is not None:
            fields.append("error_message = ?")
            values.append(error_message)

        if status == FileStatus.COPIED:
            fields.append("copied_at = ?")
            values.append(datetime.now())

        values.append(file_id)

        with self._get_connection() as conn:
            conn.execute(
                f"UPDATE photo_files SET {', '.join(fields)} WHERE id = ?",
                values
            )

    def file_exists(self, source_path: str) -> bool:
        """Check if a file already exists in the database."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM photo_files WHERE source_path = ?", (source_path,)
            ).fetchone()
        return row is not None

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
                    SUM(CASE WHEN exif_date IS NOT NULL THEN 1 ELSE 0 END) as with_exif,
                    SUM(file_size) as total_size
                FROM photo_files WHERE batch_id = ?
                """,
                (batch_id,)
            ).fetchone()

        return dict(row)

    def _row_to_photo_file(self, row: sqlite3.Row) -> PhotoFile:
        """Convert database row to PhotoFile object."""
        return PhotoFile(
            id=row['id'],
            batch_id=row['batch_id'],
            source_path=row['source_path'],
            filename=row['filename'],
            file_size=row['file_size'],
            file_extension=row['file_extension'],
            exif_date=self._parse_datetime(row['exif_date']),
            file_creation_date=self._parse_datetime(row['file_creation_date']),
            file_modification_date=self._parse_datetime(row['file_modification_date']),
            target_path=row['target_path'],
            status=FileStatus(row['status']),
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
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        return None

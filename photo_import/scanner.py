"""
Photo scanner module - discovers and catalogs photos from directories.

Uses parallel processing for fast scanning of large photo collections.
"""

import hashlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional, Callable, List
import multiprocessing

from .database import Database
from .exif_reader import (
    get_exif_date, get_file_dates, is_supported_photo, SUPPORTED_EXTENSIONS
)
from .models import Batch, BatchStatus, PhotoFile, FileStatus

logger = logging.getLogger(__name__)

# Chunk size for calculating MD5 checksum
CHECKSUM_CHUNK_SIZE = 65536  # 64KB for faster I/O

# Batch size for bulk inserts
BULK_INSERT_SIZE = 500

# Default number of worker threads (I/O bound, so more than CPU count is fine)
DEFAULT_WORKERS = min(32, (multiprocessing.cpu_count() or 1) * 4)


def calculate_checksum(filepath: Path) -> str:
    """Calculate MD5 checksum of a file."""
    md5 = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(CHECKSUM_CHUNK_SIZE), b''):
            md5.update(chunk)
    return md5.hexdigest()


def discover_photos(directory: Path) -> Generator[Path, None, None]:
    """
    Recursively discover all photo files in a directory.

    Yields Path objects for each supported photo file.
    """
    for root, dirs, files in os.walk(directory):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]

        for filename in files:
            # Skip hidden files
            if filename.startswith('.'):
                continue

            filepath = Path(root) / filename
            if is_supported_photo(filepath):
                yield filepath


def discover_photos_fast(directory: Path) -> List[Path]:
    """
    Recursively discover all photo files in a directory.

    Returns a list for parallel processing.
    """
    photos = []
    for root, dirs, files in os.walk(directory):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]

        for filename in files:
            # Skip hidden files
            if filename.startswith('.'):
                continue

            filepath = Path(root) / filename
            if is_supported_photo(filepath):
                photos.append(filepath)

    return photos


def process_single_file(
    filepath: Path,
    batch_id: int,
    calculate_checksums: bool
) -> Optional[PhotoFile]:
    """
    Process a single file - extract metadata and create PhotoFile record.

    This function is designed to be called in parallel.
    """
    try:
        creation_date, modification_date = get_file_dates(filepath)
        exif_date = get_exif_date(filepath)

        checksum = None
        if calculate_checksums:
            try:
                checksum = calculate_checksum(filepath)
            except Exception as e:
                logger.debug(f"Failed to calculate checksum for {filepath}: {e}")

        stat = filepath.stat()

        return PhotoFile(
            id=None,
            batch_id=batch_id,
            source_path=str(filepath),
            filename=filepath.name,
            file_size=stat.st_size,
            file_extension=filepath.suffix.lower(),
            exif_date=exif_date,
            file_creation_date=creation_date,
            file_modification_date=modification_date,
            target_path=None,
            status=FileStatus.PENDING,
            error_message=None,
            scanned_at=datetime.now(),
            copied_at=None,
            checksum=checksum,
        )
    except Exception as e:
        logger.warning(f"Failed to process {filepath}: {e}")
        return None


class PhotoScanner:
    """Scanner to discover and catalog photos in a directory."""

    def __init__(
        self,
        db: Database,
        calculate_checksums: bool = True,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        num_workers: Optional[int] = None,
    ):
        """
        Initialize the scanner.

        Args:
            db: Database instance
            calculate_checksums: Whether to calculate MD5 checksums
            progress_callback: Optional callback(scanned, total, current_file)
            num_workers: Number of parallel workers (default: auto)
        """
        self.db = db
        self.calculate_checksums = calculate_checksums
        self.progress_callback = progress_callback
        self.num_workers = num_workers or DEFAULT_WORKERS

    def scan(
        self,
        source_directory: str | Path,
        target_directory: str | Path,
        resume: bool = True,
    ) -> Batch:
        """
        Scan a directory for photos and catalog them using parallel processing.

        Args:
            source_directory: Directory to scan for photos
            target_directory: Target directory for organized copies
            resume: Whether to resume an existing scan if found

        Returns:
            Batch object representing the scan operation
        """
        source_directory = Path(source_directory).resolve()
        target_directory = Path(target_directory).resolve()

        if not source_directory.exists():
            raise FileNotFoundError(f"Source directory not found: {source_directory}")

        if not source_directory.is_dir():
            raise NotADirectoryError(f"Source is not a directory: {source_directory}")

        # Check for existing batch to resume
        batch = None
        if resume:
            batch = self.db.get_active_batch(str(source_directory))
            if batch and batch.status in (BatchStatus.SCANNED, BatchStatus.COPYING, BatchStatus.COMPLETED):
                logger.info(f"Found existing batch #{batch.id} in status {batch.status}")
                return batch

        # Create new batch if not resuming
        if not batch:
            batch = self.db.create_batch(
                source_directory=str(source_directory),
                target_directory=str(target_directory)
            )
            logger.info(f"Created new batch #{batch.id}")

        # Discover all photos (fast scan)
        logger.info(f"Discovering photos in {source_directory}...")
        all_photos = discover_photos_fast(source_directory)
        total_files = len(all_photos)
        logger.info(f"Found {total_files} photo files to scan")

        if total_files == 0:
            self.db.update_batch_status(batch.id, BatchStatus.SCANNED)
            return self.db.get_batch(batch.id)

        self.db.update_batch_status(
            batch.id, BatchStatus.SCANNING, total_files=total_files
        )

        # Filter out already processed files
        files_to_process = []
        for filepath in all_photos:
            if not self.db.file_exists(str(filepath)):
                files_to_process.append(filepath)

        skipped = total_files - len(files_to_process)
        if skipped > 0:
            logger.info(f"Skipping {skipped} already processed files")

        logger.info(f"Processing {len(files_to_process)} files with {self.num_workers} workers...")

        # Process files in parallel
        scanned = skipped
        photo_buffer = []

        try:
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                # Submit all tasks
                future_to_path = {
                    executor.submit(
                        process_single_file,
                        filepath,
                        batch.id,
                        self.calculate_checksums
                    ): filepath
                    for filepath in files_to_process
                }

                # Process results as they complete
                for future in as_completed(future_to_path):
                    filepath = future_to_path[future]
                    scanned += 1

                    try:
                        photo = future.result()
                        if photo:
                            photo_buffer.append(photo)

                            # Bulk insert when buffer is full
                            if len(photo_buffer) >= BULK_INSERT_SIZE:
                                self.db.add_photo_files_bulk(photo_buffer)
                                photo_buffer.clear()

                    except Exception as e:
                        logger.warning(f"Error processing {filepath}: {e}")

                    # Update progress
                    if self.progress_callback:
                        self.progress_callback(scanned, total_files, str(filepath))

                    # Update batch progress periodically
                    if scanned % 500 == 0:
                        self.db.update_batch_status(
                            batch.id, BatchStatus.SCANNING,
                            scanned_files=scanned,
                            last_processed_path=str(filepath)
                        )

            # Insert remaining photos
            if photo_buffer:
                self.db.add_photo_files_bulk(photo_buffer)

            # Mark scan as complete
            self.db.update_batch_counts(batch.id)
            self.db.update_batch_status(batch.id, BatchStatus.SCANNED)

            logger.info(f"Scan complete: {scanned} files processed")

        except KeyboardInterrupt:
            # Save progress before exiting
            if photo_buffer:
                self.db.add_photo_files_bulk(photo_buffer)
            self.db.update_batch_counts(batch.id)
            self.db.update_batch_status(
                batch.id, BatchStatus.PAUSED,
                scanned_files=scanned
            )
            logger.info("Scan interrupted, progress saved")
            raise

        except Exception as e:
            logger.error(f"Scan failed: {e}")
            if photo_buffer:
                self.db.add_photo_files_bulk(photo_buffer)
            self.db.update_batch_status(
                batch.id, BatchStatus.PAUSED,
                scanned_files=scanned
            )
            raise

        return self.db.get_batch(batch.id)

"""
Video scanner module - discovers and catalogs videos from directories.

Uses parallel processing for fast scanning of large video collections.
"""

import hashlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional, Callable, List
import multiprocessing

from .video_database import VideoDatabase
from .video_reader import (
    get_video_date, get_file_dates, is_supported_video, SUPPORTED_VIDEO_EXTENSIONS
)
from .video_models import VideoBatch, VideoBatchStatus, VideoFile, VideoFileStatus

logger = logging.getLogger(__name__)

# Chunk size for calculating MD5 checksum
CHECKSUM_CHUNK_SIZE = 65536  # 64KB

# Batch size for bulk inserts
BULK_INSERT_SIZE = 500

# Default number of worker threads
DEFAULT_WORKERS = min(32, (multiprocessing.cpu_count() or 1) * 4)


def calculate_checksum(filepath: Path) -> str:
    """Calculate MD5 checksum of a file."""
    md5 = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(CHECKSUM_CHUNK_SIZE), b''):
            md5.update(chunk)
    return md5.hexdigest()


def discover_videos_fast(directory: Path) -> List[Path]:
    """
    Recursively discover all video files in a directory.

    Returns a list for parallel processing.
    """
    videos = []
    for root, dirs, files in os.walk(directory):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]

        for filename in files:
            # Skip hidden files
            if filename.startswith('.'):
                continue

            filepath = Path(root) / filename
            if is_supported_video(filepath):
                videos.append(filepath)

    return videos


def process_single_video(
    filepath: Path,
    batch_id: int,
    calculate_checksums: bool
) -> Optional[VideoFile]:
    """
    Process a single video file - extract metadata and create VideoFile record.

    This function is designed to be called in parallel.
    """
    try:
        creation_date, modification_date = get_file_dates(filepath)
        metadata_date = get_video_date(filepath)

        checksum = None
        if calculate_checksums:
            try:
                checksum = calculate_checksum(filepath)
            except Exception as e:
                logger.debug(f"Failed to calculate checksum for {filepath}: {e}")

        stat = filepath.stat()

        return VideoFile(
            id=None,
            batch_id=batch_id,
            source_path=str(filepath),
            filename=filepath.name,
            file_size=stat.st_size,
            file_extension=filepath.suffix.lower(),
            metadata_date=metadata_date,
            file_creation_date=creation_date,
            file_modification_date=modification_date,
            target_path=None,
            status=VideoFileStatus.PENDING,
            error_message=None,
            scanned_at=datetime.now(),
            copied_at=None,
            checksum=checksum,
        )
    except Exception as e:
        logger.warning(f"Failed to process {filepath}: {e}")
        return None


class VideoScanner:
    """Scanner to discover and catalog videos in a directory."""

    def __init__(
        self,
        db: VideoDatabase,
        calculate_checksums: bool = False,  # Default off for videos (large files)
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        num_workers: Optional[int] = None,
    ):
        """
        Initialize the scanner.

        Args:
            db: VideoDatabase instance
            calculate_checksums: Whether to calculate MD5 checksums (slow for videos)
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
    ) -> VideoBatch:
        """
        Scan a directory for videos and catalog them using parallel processing.

        Args:
            source_directory: Directory to scan for videos
            target_directory: Target directory for organized copies
            resume: Whether to resume an existing scan if found

        Returns:
            VideoBatch object representing the scan operation
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
            if batch and batch.status in (VideoBatchStatus.SCANNED, VideoBatchStatus.COPYING, VideoBatchStatus.COMPLETED):
                logger.info(f"Found existing batch #{batch.id} in status {batch.status}")
                return batch

        # Create new batch if not resuming
        if not batch:
            batch = self.db.create_batch(
                source_directory=str(source_directory),
                target_directory=str(target_directory)
            )
            logger.info(f"Created new batch #{batch.id}")

        # Discover all videos (fast scan)
        logger.info(f"Discovering videos in {source_directory}...")
        all_videos = discover_videos_fast(source_directory)
        total_files = len(all_videos)
        logger.info(f"Found {total_files} video files to scan")

        if total_files == 0:
            self.db.update_batch_status(batch.id, VideoBatchStatus.SCANNED)
            return self.db.get_batch(batch.id)

        self.db.update_batch_status(
            batch.id, VideoBatchStatus.SCANNING, total_files=total_files
        )

        # Filter out already processed files
        files_to_process = []
        for filepath in all_videos:
            if not self.db.file_exists(str(filepath)):
                files_to_process.append(filepath)

        skipped = total_files - len(files_to_process)
        if skipped > 0:
            logger.info(f"Skipping {skipped} already processed files")

        logger.info(f"Processing {len(files_to_process)} files with {self.num_workers} workers...")

        # Process files in parallel
        scanned = skipped
        video_buffer = []

        try:
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                # Submit all tasks
                future_to_path = {
                    executor.submit(
                        process_single_video,
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
                        video = future.result()
                        if video:
                            video_buffer.append(video)

                            # Bulk insert when buffer is full
                            if len(video_buffer) >= BULK_INSERT_SIZE:
                                self.db.add_video_files_bulk(video_buffer)
                                video_buffer.clear()

                    except Exception as e:
                        logger.warning(f"Error processing {filepath}: {e}")

                    # Update progress
                    if self.progress_callback:
                        self.progress_callback(scanned, total_files, str(filepath))

                    # Update batch progress periodically
                    if scanned % 100 == 0:
                        self.db.update_batch_status(
                            batch.id, VideoBatchStatus.SCANNING,
                            scanned_files=scanned,
                            last_processed_path=str(filepath)
                        )

            # Insert remaining videos
            if video_buffer:
                self.db.add_video_files_bulk(video_buffer)

            # Mark scan as complete
            self.db.update_batch_counts(batch.id)
            self.db.update_batch_status(batch.id, VideoBatchStatus.SCANNED)

            logger.info(f"Scan complete: {scanned} files processed")

        except KeyboardInterrupt:
            # Save progress before exiting
            if video_buffer:
                self.db.add_video_files_bulk(video_buffer)
            self.db.update_batch_counts(batch.id)
            self.db.update_batch_status(
                batch.id, VideoBatchStatus.PAUSED,
                scanned_files=scanned
            )
            logger.info("Scan interrupted, progress saved")
            raise

        except Exception as e:
            logger.error(f"Scan failed: {e}")
            if video_buffer:
                self.db.add_video_files_bulk(video_buffer)
            self.db.update_batch_status(
                batch.id, VideoBatchStatus.PAUSED,
                scanned_files=scanned
            )
            raise

        return self.db.get_batch(batch.id)

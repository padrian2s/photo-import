"""
Photo copier module - copies photos to date-based directory structure.
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from .database import Database
from .models import BatchStatus, PhotoFile, FileStatus

logger = logging.getLogger(__name__)

# Number of files to process per commit
COMMIT_BATCH_SIZE = 50


def generate_target_path(
    target_base: Path,
    photo: PhotoFile,
    use_file_date_fallback: bool = True,
) -> Path:
    """
    Generate target path based on EXIF date or file creation date.

    Creates a structure like: target_base/YYYY_MM_DD/filename

    Priority:
    1. EXIF DateTimeOriginal (actual photo creation date)
    2. File creation date (if use_file_date_fallback=True)
    3. File modification date (final fallback)

    Args:
        target_base: Base directory for organized photos
        photo: PhotoFile record with dates
        use_file_date_fallback: If True, use file date when no EXIF

    Returns:
        Target path (always returns a valid path using file date as fallback)
    """
    # Prefer EXIF date
    date = photo.exif_date

    # Fall back to file creation date, then modification date
    if date is None and use_file_date_fallback:
        # Prefer file creation date over modification date
        date = photo.file_creation_date or photo.file_modification_date

    # Final fallback - should never happen but be safe
    if date is None:
        date = photo.file_modification_date or datetime.now()

    # Format: YYYY_MM_DD
    date_folder = date.strftime("%Y_%m_%d")
    return target_base / date_folder / photo.filename


def resolve_filename_conflict(target_path: Path) -> Path:
    """
    Resolve filename conflicts by adding a numeric suffix.

    Example: photo.jpg -> photo_1.jpg -> photo_2.jpg
    """
    if not target_path.exists():
        return target_path

    stem = target_path.stem
    suffix = target_path.suffix
    parent = target_path.parent

    counter = 1
    while True:
        new_name = f"{stem}_{counter}{suffix}"
        new_path = parent / new_name
        if not new_path.exists():
            return new_path
        counter += 1


class PhotoCopier:
    """Copies photos to organized directory structure."""

    def __init__(
        self,
        db: Database,
        use_file_date_fallback: bool = True,
        skip_no_exif: bool = False,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ):
        """
        Initialize the copier.

        Args:
            db: Database instance
            use_file_date_fallback: Use file date when EXIF not available
            skip_no_exif: Skip files without EXIF date
            progress_callback: Optional callback(copied, total, current_file)
        """
        self.db = db
        self.use_file_date_fallback = use_file_date_fallback
        self.skip_no_exif = skip_no_exif
        self.progress_callback = progress_callback

    def copy(self, batch_id: int, dry_run: bool = False) -> dict:
        """
        Copy all pending files in a batch to target directory.

        Args:
            batch_id: Batch ID to process
            dry_run: If True, simulate copy without actually copying

        Returns:
            Dictionary with copy statistics
        """
        batch = self.db.get_batch(batch_id)
        if not batch:
            raise ValueError(f"Batch {batch_id} not found")

        if batch.status not in (BatchStatus.SCANNED, BatchStatus.COPYING, BatchStatus.PAUSED):
            raise ValueError(
                f"Batch {batch_id} is in status {batch.status}, cannot copy"
            )

        target_base = Path(batch.target_directory)

        # Update batch status to copying
        self.db.update_batch_status(batch_id, BatchStatus.COPYING)

        stats = {
            'total': 0,
            'copied': 0,
            'skipped': 0,
            'failed': 0,
            'start_time': datetime.now(),
        }

        try:
            # Process pending files
            pending_files = self.db.get_pending_files(batch_id)
            stats['total'] = len(pending_files)

            logger.info(f"Starting copy of {stats['total']} files")

            for i, photo in enumerate(pending_files):
                try:
                    result = self._copy_file(photo, target_base, dry_run)

                    if result == 'copied':
                        stats['copied'] += 1
                    elif result == 'skipped':
                        stats['skipped'] += 1
                    else:
                        stats['failed'] += 1

                except Exception as e:
                    logger.error(f"Error processing {photo.source_path}: {e}")
                    self.db.update_file_status(
                        photo.id, FileStatus.FAILED, error_message=str(e)
                    )
                    stats['failed'] += 1

                # Progress callback
                if self.progress_callback:
                    self.progress_callback(
                        i + 1, stats['total'], photo.source_path
                    )

                # Periodic batch count update
                if (i + 1) % COMMIT_BATCH_SIZE == 0:
                    self.db.update_batch_counts(batch_id)

            # Final update
            self.db.update_batch_counts(batch_id)

            # Mark batch as complete if no pending files remain
            remaining = self.db.get_pending_files(batch_id, limit=1)
            if not remaining:
                self.db.update_batch_status(batch_id, BatchStatus.COMPLETED)
                logger.info("Batch completed successfully")
            else:
                logger.info(f"{len(remaining)} files still pending")

        except KeyboardInterrupt:
            logger.info("Copy interrupted by user")
            self.db.update_batch_status(batch_id, BatchStatus.PAUSED)
            raise

        except Exception as e:
            logger.error(f"Copy failed: {e}")
            self.db.update_batch_status(batch_id, BatchStatus.PAUSED)
            raise

        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']

        return stats

    def _copy_file(
        self,
        photo: PhotoFile,
        target_base: Path,
        dry_run: bool,
    ) -> str:
        """
        Copy a single file.

        Returns: 'copied', 'skipped', or 'failed'
        """
        source_path = Path(photo.source_path)

        # Check source exists
        if not source_path.exists():
            self.db.update_file_status(
                photo.id, FileStatus.FAILED,
                error_message="Source file no longer exists"
            )
            return 'failed'

        # Skip files without EXIF if requested
        if self.skip_no_exif and photo.exif_date is None:
            self.db.update_file_status(
                photo.id, FileStatus.SKIPPED,
                error_message="No EXIF date available"
            )
            return 'skipped'

        # Generate target path (uses file creation date as fallback)
        target_path = generate_target_path(
            target_base, photo, self.use_file_date_fallback
        )

        # Resolve conflicts
        target_path = resolve_filename_conflict(target_path)

        if dry_run:
            logger.info(f"[DRY RUN] Would copy: {source_path} -> {target_path}")
            self.db.update_file_status(
                photo.id, FileStatus.COPIED,
                target_path=str(target_path)
            )
            return 'copied'

        # Create target directory
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy file with metadata preservation
        try:
            shutil.copy2(source_path, target_path)
            logger.debug(f"Copied: {source_path} -> {target_path}")

            self.db.update_file_status(
                photo.id, FileStatus.COPIED,
                target_path=str(target_path)
            )
            return 'copied'

        except Exception as e:
            self.db.update_file_status(
                photo.id, FileStatus.FAILED,
                error_message=f"Copy failed: {e}"
            )
            return 'failed'

    def retry_failed(self, batch_id: int) -> dict:
        """
        Retry copying failed files.

        Returns:
            Dictionary with retry statistics
        """
        # Reset failed files to pending
        batch = self.db.get_batch(batch_id)
        if not batch:
            raise ValueError(f"Batch {batch_id} not found")

        failed_files = self.db.get_files_by_status(batch_id, FileStatus.FAILED)

        for photo in failed_files:
            self.db.update_file_status(photo.id, FileStatus.PENDING)

        logger.info(f"Reset {len(failed_files)} failed files to pending")

        # Re-run copy
        return self.copy(batch_id)

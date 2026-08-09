"""
Photo copier module - copies photos to date-based directory structure.
"""

import logging
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from .database import Database
from .models import BatchStatus, PhotoFile, FileStatus

logger = logging.getLogger(__name__)

# Number of files to process per commit
COMMIT_BATCH_SIZE = 50

# Imports never land in the bare target - they go under this folder
ORGANIZED_DIRNAME = "organized_photos"


def organized_base(target_base: Path) -> Path:
    """
    Return the folder imports are written into.

    Always `target_base/organized_photos`, unless the target already is that
    folder (so pointing straight at the library does not nest it twice).
    """
    target_base = Path(target_base)
    if target_base.name == ORGANIZED_DIRNAME:
        return target_base
    return target_base / ORGANIZED_DIRNAME


def date_directory(date: datetime) -> Path:
    """Hierarchical date folders: 2026-08-09 -> 2026/08/09."""
    return Path(date.strftime("%Y"), date.strftime("%m"), date.strftime("%d"))


def generate_target_path(
    target_base: Path,
    photo: PhotoFile,
    use_file_date_fallback: bool = True,
) -> Path:
    """
    Generate target path based on EXIF date or file creation date.

    Creates a structure like: target_base/organized_photos/YYYY/MM/DD/filename

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

    return organized_base(target_base) / date_directory(date) / photo.filename


def resolve_filename_conflict(target_path: Path) -> Path:
    """
    Resolve filename conflicts by adding a numeric suffix.

    Example: photo.jpg -> photo_1.jpg -> photo_2.jpg

    Keeps original filename intact (IMG_9994.jpg -> IMG_9994_1.jpg)
    """
    if not target_path.exists():
        return target_path

    stem = target_path.stem
    suffix = target_path.suffix
    parent = target_path.parent

    # Always use original stem, just add counter
    counter = 1

    # No limit - just keep incrementing until we find a free slot
    while True:
        new_name = f"{stem}_{counter}{suffix}"
        new_path = parent / new_name
        if not new_path.exists():
            if counter > 100:
                logger.info(f"High conflict count ({counter}): {target_path.name} -> {new_name}")
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

        # Update batch status to copying (a dry run must not change the batch)
        if not dry_run:
            self.db.update_batch_status(batch_id, BatchStatus.COPYING)

        stats = {
            'total': 0,
            'copied': 0,
            'skipped': 0,
            'failed': 0,
            'conflicts': 0,
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
                    elif result == 'conflict':
                        stats['conflicts'] += 1
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
                if not dry_run and (i + 1) % COMMIT_BATCH_SIZE == 0:
                    self.db.update_batch_counts(batch_id)

            if not dry_run:
                # Final update
                self.db.update_batch_counts(batch_id)

                # Mark batch as complete if nothing is pending or waiting on a decision
                remaining = self.db.get_pending_files(batch_id, limit=1)
                unresolved = self.db.get_files_by_status(batch_id, FileStatus.CONFLICT, limit=1)
                if not remaining and not unresolved:
                    self.db.update_batch_status(batch_id, BatchStatus.COMPLETED)
                    logger.info("Batch completed successfully")
                elif unresolved:
                    self.db.update_batch_status(batch_id, BatchStatus.PAUSED)
                    logger.info(f"{stats['conflicts']} files need a conflict decision")
                else:
                    logger.info(f"{len(remaining)} files still pending")

        except KeyboardInterrupt:
            logger.info("Copy interrupted by user")
            if not dry_run:
                self.db.update_batch_status(batch_id, BatchStatus.PAUSED)
            raise

        except Exception as e:
            logger.error(f"Copy failed: {e}")
            if not dry_run:
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

        # Never overwrite silently - park the file until the user decides
        if target_path.exists():
            if not dry_run:
                self.db.update_file_status(
                    photo.id, FileStatus.CONFLICT,
                    target_path=str(target_path),
                    error_message="A different file with this name is already in the library"
                    if target_path.stat().st_size != photo.file_size
                    else "The same file is already in the library",
                )
            return 'conflict'

        if dry_run:
            # Simulation only - the database is left untouched so the real
            # copy can still run afterwards
            logger.info(f"[DRY RUN] Would copy: {source_path} -> {target_path}")
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

    def resolve_conflicts(
        self,
        batch_id: int,
        action: str,
        file_ids: Optional[list] = None,
    ) -> dict:
        """
        Apply the user's decision to files parked as conflicts.

        Args:
            batch_id: Batch the conflicts belong to
            action: 'overwrite', 'keep_both' or 'skip'
            file_ids: Only these files (default: every conflict in the batch)

        Returns:
            Dictionary with resolution statistics
        """
        if action not in ('overwrite', 'keep_both', 'skip'):
            raise ValueError(f"Unknown conflict action: {action}")

        conflicts = self.db.get_files_by_status(batch_id, FileStatus.CONFLICT)
        if file_ids:
            wanted = set(file_ids)
            conflicts = [photo for photo in conflicts if photo.id in wanted]

        stats = {
            'total': len(conflicts),
            'copied': 0,
            'skipped': 0,
            'failed': 0,
            'start_time': datetime.now(),
        }

        for index, photo in enumerate(conflicts):
            try:
                if action == 'skip':
                    self.db.update_file_status(
                        photo.id, FileStatus.SKIPPED,
                        error_message="Kept the file already in the library"
                    )
                    stats['skipped'] += 1
                else:
                    source_path = Path(photo.source_path)
                    if not source_path.exists():
                        raise FileNotFoundError("Source file no longer exists")

                    target_path = Path(photo.target_path)
                    if action == 'keep_both':
                        target_path = resolve_filename_conflict(target_path)

                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_path, target_path)
                    self.db.update_file_status(
                        photo.id, FileStatus.COPIED, target_path=str(target_path)
                    )
                    stats['copied'] += 1

            except Exception as e:
                logger.error(f"Failed to resolve conflict for {photo.source_path}: {e}")
                self.db.update_file_status(
                    photo.id, FileStatus.FAILED, error_message=f"Conflict resolution failed: {e}"
                )
                stats['failed'] += 1

            if self.progress_callback:
                self.progress_callback(index + 1, stats['total'], photo.source_path)

        self.db.update_batch_counts(batch_id)

        # Close the batch once nothing is pending or waiting on a decision
        if not self.db.get_pending_files(batch_id, limit=1) and \
                not self.db.get_files_by_status(batch_id, FileStatus.CONFLICT, limit=1):
            self.db.update_batch_status(batch_id, BatchStatus.COMPLETED)

        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
        return stats

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

        # A completed/failed batch would be rejected by copy(), so reopen it
        if batch.status not in (BatchStatus.SCANNED, BatchStatus.COPYING, BatchStatus.PAUSED):
            self.db.update_batch_status(batch_id, BatchStatus.PAUSED)

        # Re-run copy
        return self.copy(batch_id)

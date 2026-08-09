"""
Video copier module - copies videos to date-based directory structure.
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from .copier import date_directory, organized_base
from .video_database import VideoDatabase
from .video_models import VideoBatchStatus, VideoFile, VideoFileStatus

logger = logging.getLogger(__name__)

# Number of files to process per commit
COMMIT_BATCH_SIZE = 50


def generate_target_path(
    target_base: Path,
    video: VideoFile,
    use_file_date_fallback: bool = True,
) -> Path:
    """
    Generate target path based on metadata date or file creation date.

    Creates a structure like: target_base/organized_photos/YYYY/MM/DD/filename

    Priority:
    1. Video metadata creation date (from ffprobe)
    2. File creation date (if use_file_date_fallback=True)
    3. File modification date (final fallback)

    Args:
        target_base: Base directory for organized videos
        video: VideoFile record with dates
        use_file_date_fallback: If True, use file date when no metadata

    Returns:
        Target path (always returns a valid path using file date as fallback)
    """
    # Prefer metadata date
    date = video.metadata_date

    # Fall back to file creation date, then modification date
    if date is None and use_file_date_fallback:
        date = video.file_creation_date or video.file_modification_date

    # Final fallback - should never happen but be safe
    if date is None:
        date = video.file_modification_date or datetime.now()

    return organized_base(target_base) / date_directory(date) / video.filename


def resolve_filename_conflict(target_path: Path) -> Path:
    """
    Resolve filename conflicts by adding a numeric suffix.

    Example: video.mp4 -> video_1.mp4 -> video_2.mp4

    Keeps original filename intact (VID_20200315.mp4 -> VID_20200315_1.mp4)
    """
    if not target_path.exists():
        return target_path

    stem = target_path.stem
    suffix = target_path.suffix
    parent = target_path.parent

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


class VideoCopier:
    """Copies videos to organized directory structure."""

    def __init__(
        self,
        db: VideoDatabase,
        use_file_date_fallback: bool = True,
        skip_no_metadata: bool = False,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ):
        """
        Initialize the copier.

        Args:
            db: VideoDatabase instance
            use_file_date_fallback: Use file date when metadata not available
            skip_no_metadata: Skip files without metadata date
            progress_callback: Optional callback(copied, total, current_file)
        """
        self.db = db
        self.use_file_date_fallback = use_file_date_fallback
        self.skip_no_metadata = skip_no_metadata
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

        # Allow copying if batch is in a copyable state OR if there are pending files
        # (handles case where batch was marked complete but retry reset files to pending)
        allowed_statuses = (VideoBatchStatus.SCANNED, VideoBatchStatus.COPYING,
                           VideoBatchStatus.PAUSED, VideoBatchStatus.COMPLETED)
        if batch.status not in allowed_statuses:
            raise ValueError(
                f"Batch {batch_id} is in status {batch.status}, cannot copy"
            )

        target_base = Path(batch.target_directory)

        # Update batch status to copying (a dry run must not change the batch)
        if not dry_run:
            self.db.update_batch_status(batch_id, VideoBatchStatus.COPYING)

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

            logger.info(f"Starting copy of {stats['total']} video files")

            for i, video in enumerate(pending_files):
                try:
                    result = self._copy_file(video, target_base, dry_run)

                    if result == 'copied':
                        stats['copied'] += 1
                    elif result == 'skipped':
                        stats['skipped'] += 1
                    elif result == 'conflict':
                        stats['conflicts'] += 1
                    else:
                        stats['failed'] += 1

                except Exception as e:
                    logger.error(f"Error processing {video.source_path}: {e}")
                    self.db.update_file_status(
                        video.id, VideoFileStatus.FAILED, error_message=str(e)
                    )
                    stats['failed'] += 1

                # Progress callback
                if self.progress_callback:
                    self.progress_callback(
                        i + 1, stats['total'], video.source_path
                    )

                # Periodic batch count update
                if not dry_run and (i + 1) % COMMIT_BATCH_SIZE == 0:
                    self.db.update_batch_counts(batch_id)

            if not dry_run:
                # Final update
                self.db.update_batch_counts(batch_id)

                # Mark batch as complete if nothing is pending or waiting on a decision
                remaining = self.db.get_pending_files(batch_id, limit=1)
                unresolved = self.db.get_files_by_status(batch_id, VideoFileStatus.CONFLICT, limit=1)
                if not remaining and not unresolved:
                    self.db.update_batch_status(batch_id, VideoBatchStatus.COMPLETED)
                    logger.info("Video batch completed successfully")
                elif unresolved:
                    self.db.update_batch_status(batch_id, VideoBatchStatus.PAUSED)
                    logger.info(f"{stats['conflicts']} video files need a conflict decision")
                else:
                    logger.info(f"{len(remaining)} video files still pending")

        except KeyboardInterrupt:
            logger.info("Video copy interrupted by user")
            if not dry_run:
                self.db.update_batch_status(batch_id, VideoBatchStatus.PAUSED)
            raise

        except Exception as e:
            logger.error(f"Video copy failed: {e}")
            if not dry_run:
                self.db.update_batch_status(batch_id, VideoBatchStatus.PAUSED)
            raise

        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']

        return stats

    def _copy_file(
        self,
        video: VideoFile,
        target_base: Path,
        dry_run: bool,
    ) -> str:
        """
        Copy a single video file.

        Returns: 'copied', 'skipped', or 'failed'
        """
        source_path = Path(video.source_path)

        # Check source exists
        if not source_path.exists():
            error_msg = f"Source file no longer exists: {source_path}"
            logger.warning(error_msg)
            self.db.update_file_status(
                video.id, VideoFileStatus.FAILED,
                error_message="Source file no longer exists"
            )
            return 'failed'

        # Skip files without metadata if requested
        if self.skip_no_metadata and video.metadata_date is None:
            self.db.update_file_status(
                video.id, VideoFileStatus.SKIPPED,
                error_message="No metadata date available"
            )
            return 'skipped'

        # Generate target path (uses file creation date as fallback)
        target_path = generate_target_path(
            target_base, video, self.use_file_date_fallback
        )

        # Never overwrite silently - park the file until the user decides
        if target_path.exists():
            if not dry_run:
                self.db.update_file_status(
                    video.id, VideoFileStatus.CONFLICT,
                    target_path=str(target_path),
                    error_message="A different file with this name is already in the library"
                    if target_path.stat().st_size != video.file_size
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
                video.id, VideoFileStatus.COPIED,
                target_path=str(target_path)
            )
            return 'copied'

        except Exception as e:
            self.db.update_file_status(
                video.id, VideoFileStatus.FAILED,
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
        Apply the user's decision to videos parked as conflicts.

        Args:
            batch_id: Batch the conflicts belong to
            action: 'overwrite', 'keep_both' or 'skip'
            file_ids: Only these files (default: every conflict in the batch)

        Returns:
            Dictionary with resolution statistics
        """
        if action not in ('overwrite', 'keep_both', 'skip'):
            raise ValueError(f"Unknown conflict action: {action}")

        conflicts = self.db.get_files_by_status(batch_id, VideoFileStatus.CONFLICT)
        if file_ids:
            wanted = set(file_ids)
            conflicts = [video for video in conflicts if video.id in wanted]

        stats = {
            'total': len(conflicts),
            'copied': 0,
            'skipped': 0,
            'failed': 0,
            'start_time': datetime.now(),
        }

        for index, video in enumerate(conflicts):
            try:
                if action == 'skip':
                    self.db.update_file_status(
                        video.id, VideoFileStatus.SKIPPED,
                        error_message="Kept the file already in the library"
                    )
                    stats['skipped'] += 1
                else:
                    source_path = Path(video.source_path)
                    if not source_path.exists():
                        raise FileNotFoundError("Source file no longer exists")

                    target_path = Path(video.target_path)
                    if action == 'keep_both':
                        target_path = resolve_filename_conflict(target_path)

                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_path, target_path)
                    self.db.update_file_status(
                        video.id, VideoFileStatus.COPIED, target_path=str(target_path)
                    )
                    stats['copied'] += 1

            except Exception as e:
                logger.error(f"Failed to resolve conflict for {video.source_path}: {e}")
                self.db.update_file_status(
                    video.id, VideoFileStatus.FAILED, error_message=f"Conflict resolution failed: {e}"
                )
                stats['failed'] += 1

            if self.progress_callback:
                self.progress_callback(index + 1, stats['total'], video.source_path)

        self.db.update_batch_counts(batch_id)

        if not self.db.get_pending_files(batch_id, limit=1) and \
                not self.db.get_files_by_status(batch_id, VideoFileStatus.CONFLICT, limit=1):
            self.db.update_batch_status(batch_id, VideoBatchStatus.COMPLETED)

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

        failed_files = self.db.get_files_by_status(batch_id, VideoFileStatus.FAILED)

        for video in failed_files:
            self.db.update_file_status(video.id, VideoFileStatus.PENDING)

        logger.info(f"Reset {len(failed_files)} failed video files to pending")

        # Re-run copy
        return self.copy(batch_id)

"""
Command-line interface for photo import tool.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from . import __version__
from .database import Database
from .scanner import PhotoScanner
from .copier import PhotoCopier
from .models import BatchStatus, FileStatus
from .video_database import VideoDatabase
from .video_scanner import VideoScanner
from .video_copier import VideoCopier
from .video_models import VideoBatchStatus, VideoFileStatus


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def format_duration(delta) -> str:
    """Format timedelta to human-readable string."""
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


def progress_bar(current: int, total: int, width: int = 40) -> str:
    """Generate a text progress bar."""
    if total == 0:
        return "[" + "=" * width + "]"

    filled = int(width * current / total)
    bar = "=" * filled + "-" * (width - filled)
    percent = 100 * current / total
    return f"[{bar}] {percent:.1f}% ({current}/{total})"


class CLI:
    """Command-line interface handler."""

    def __init__(self, db_path: str = "photo_import.db", video_db_path: str = "video_import.db"):
        self.db = Database(db_path)
        self.video_db = VideoDatabase(video_db_path)

    def scan(
        self,
        source: str,
        target: str,
        no_checksum: bool = False,
        resume: bool = True,
        workers: Optional[int] = None,
    ):
        """Scan source directory for photos."""
        source_path = Path(source).resolve()
        target_path = Path(target).resolve()

        # Import here to get default workers count
        from .scanner import DEFAULT_WORKERS
        num_workers = workers or DEFAULT_WORKERS

        print(f"\n📸 Photo Import Tool v{__version__}")
        print("=" * 50)
        print(f"Source: {source_path}")
        print(f"Target: {target_path}")
        print(f"Checksums: {'disabled' if no_checksum else 'enabled'}")
        print(f"Workers: {num_workers} (parallel threads)")
        print("=" * 50)

        def progress_callback(scanned: int, total: int, current_file: str):
            bar = progress_bar(scanned, total)
            filename = Path(current_file).name
            # Truncate filename if too long
            if len(filename) > 30:
                filename = filename[:27] + "..."
            print(f"\r{bar} {filename:<35}", end="", flush=True)

        scanner = PhotoScanner(
            self.db,
            calculate_checksums=not no_checksum,
            progress_callback=progress_callback,
            num_workers=num_workers,
        )

        try:
            batch = scanner.scan(source_path, target_path, resume=resume)
            print()  # New line after progress bar

            # Show results
            stats = self.db.get_batch_stats(batch.id)
            total = stats['total'] or 0
            with_exif = stats['with_exif'] or 0
            print("\n✅ Scan Complete!")
            print("-" * 40)
            print(f"Batch ID: {batch.id}")
            print(f"Total files: {total}")
            print(f"Files with EXIF date: {with_exif}")
            print(f"Files without EXIF date: {total - with_exif}")
            print(f"Total size: {format_size(stats['total_size'] or 0)}")
            print("-" * 40)
            print(f"\nTo copy files, run: photo-import copy --batch {batch.id}")

        except KeyboardInterrupt:
            print("\n\n⚠️  Scan interrupted. Progress has been saved.")
            print("Run the same command to resume.")
            sys.exit(1)

        except Exception as e:
            print(f"\n\n❌ Error: {e}")
            logging.exception("Scan failed")
            sys.exit(1)

    def copy(
        self,
        batch_id: Optional[int] = None,
        dry_run: bool = False,
        skip_no_exif: bool = False,
        use_file_date: bool = True,
    ):
        """Copy scanned photos to target directory."""
        # Get batch
        if batch_id is None:
            batch = self.db.get_latest_batch()
            if not batch:
                print("❌ No batches found. Run 'scan' first.")
                sys.exit(1)
            batch_id = batch.id
        else:
            batch = self.db.get_batch(batch_id)
            if not batch:
                print(f"❌ Batch {batch_id} not found.")
                sys.exit(1)

        stats = self.db.get_batch_stats(batch_id)

        print(f"\n📸 Photo Import Tool v{__version__}")
        print("=" * 50)
        print(f"Batch: #{batch_id}")
        print(f"Status: {batch.status.value}")
        print(f"Source: {batch.source_directory}")
        print(f"Target: {batch.target_directory}")
        print(f"Pending files: {stats['pending']}")
        if dry_run:
            print("Mode: DRY RUN (no files will be copied)")
        print("=" * 50)

        if stats['pending'] == 0:
            print("\n✅ No pending files to copy.")
            return

        def progress_callback(copied: int, total: int, current_file: str):
            bar = progress_bar(copied, total)
            filename = Path(current_file).name
            if len(filename) > 30:
                filename = filename[:27] + "..."
            print(f"\r{bar} {filename:<35}", end="", flush=True)

        copier = PhotoCopier(
            self.db,
            use_file_date_fallback=use_file_date,
            skip_no_exif=skip_no_exif,
            progress_callback=progress_callback
        )

        try:
            result = copier.copy(batch_id, dry_run=dry_run)
            print()  # New line after progress bar

            # Show results
            print("\n✅ Copy Complete!")
            print("-" * 40)
            print(f"Files copied: {result['copied']}")
            print(f"Files skipped: {result['skipped']}")
            print(f"Files failed: {result['failed']}")
            print(f"Duration: {format_duration(result['duration'])}")
            print("-" * 40)

            if result['failed'] > 0:
                print(f"\n⚠️  {result['failed']} files failed to copy.")
                print(f"Run 'photo-import status --batch {batch_id}' for details.")
                print(f"Run 'photo-import retry --batch {batch_id}' to retry failed files.")

        except KeyboardInterrupt:
            print("\n\n⚠️  Copy interrupted. Progress has been saved.")
            print(f"Run 'photo-import copy --batch {batch_id}' to resume.")
            sys.exit(1)

        except Exception as e:
            print(f"\n\n❌ Error: {e}")
            logging.exception("Copy failed")
            sys.exit(1)

    def status(self, batch_id: Optional[int] = None, show_failed: bool = False):
        """Show status of batches."""
        if batch_id is not None:
            batch = self.db.get_batch(batch_id)
            if not batch:
                print(f"❌ Batch {batch_id} not found.")
                sys.exit(1)
            batches = [batch]
        else:
            batches = self.db.list_batches(limit=10)

        if not batches:
            print("No batches found.")
            return

        print(f"\n📸 Photo Import Tool v{__version__}")
        print("=" * 70)

        for batch in batches:
            stats = self.db.get_batch_stats(batch.id)

            status_emoji = {
                BatchStatus.SCANNING: "🔍",
                BatchStatus.SCANNED: "📋",
                BatchStatus.COPYING: "📤",
                BatchStatus.COMPLETED: "✅",
                BatchStatus.FAILED: "❌",
                BatchStatus.PAUSED: "⏸️",
            }.get(batch.status, "❓")

            print(f"\nBatch #{batch.id} {status_emoji} {batch.status.value}")
            print("-" * 60)
            print(f"  Source:    {batch.source_directory}")
            print(f"  Target:    {batch.target_directory}")
            print(f"  Started:   {batch.started_at.strftime('%Y-%m-%d %H:%M:%S')}")

            if batch.scan_completed_at:
                print(f"  Scanned:   {batch.scan_completed_at.strftime('%Y-%m-%d %H:%M:%S')}")
            if batch.copy_started_at:
                print(f"  Copy began: {batch.copy_started_at.strftime('%Y-%m-%d %H:%M:%S')}")
            if batch.completed_at:
                print(f"  Completed: {batch.completed_at.strftime('%Y-%m-%d %H:%M:%S')}")

            print(f"\n  Files:")
            print(f"    Total:    {stats['total']}")
            print(f"    Pending:  {stats['pending']}")
            print(f"    Copied:   {stats['copied']}")
            print(f"    Skipped:  {stats['skipped']}")
            print(f"    Failed:   {stats['failed']}")
            print(f"    With EXIF: {stats['with_exif']}")
            print(f"    Size:     {format_size(stats['total_size'] or 0)}")

            # Show failed files if requested
            if show_failed and stats['failed'] > 0:
                failed_files = self.db.get_files_by_status(batch.id, FileStatus.FAILED, limit=20)
                print(f"\n  Failed files (showing up to 20):")
                for f in failed_files:
                    print(f"    - {f.filename}: {f.error_message}")

        print("\n" + "=" * 70)

    def retry(self, batch_id: int):
        """Retry failed files in a batch."""
        batch = self.db.get_batch(batch_id)
        if not batch:
            print(f"❌ Batch {batch_id} not found.")
            sys.exit(1)

        stats = self.db.get_batch_stats(batch_id)
        if stats['failed'] == 0:
            print("✅ No failed files to retry.")
            return

        print(f"\nRetrying {stats['failed']} failed files...")

        def progress_callback(copied: int, total: int, current_file: str):
            bar = progress_bar(copied, total)
            print(f"\r{bar}", end="", flush=True)

        copier = PhotoCopier(self.db, progress_callback=progress_callback)

        try:
            result = copier.retry_failed(batch_id)
            print()

            print("\n✅ Retry Complete!")
            print(f"Files copied: {result['copied']}")
            print(f"Files still failed: {result['failed']}")

        except Exception as e:
            print(f"\n❌ Error: {e}")
            sys.exit(1)

    def list_batches(self, limit: int = 10):
        """List all batches."""
        self.status(batch_id=None)

    # ===========================================
    # Video operations
    # ===========================================

    def video_scan(
        self,
        source: str,
        target: str,
        no_checksum: bool = True,  # Default off for videos (large files)
        resume: bool = True,
        workers: Optional[int] = None,
    ):
        """Scan source directory for videos."""
        source_path = Path(source).resolve()
        target_path = Path(target).resolve()

        from .video_scanner import DEFAULT_WORKERS
        num_workers = workers or DEFAULT_WORKERS

        print(f"\n🎬 Video Import Tool v{__version__}")
        print("=" * 50)
        print(f"Source: {source_path}")
        print(f"Target: {target_path}")
        print(f"Checksums: {'enabled' if not no_checksum else 'disabled (default for videos)'}")
        print(f"Workers: {num_workers} (parallel threads)")
        print("=" * 50)

        def progress_callback(scanned: int, total: int, current_file: str):
            bar = progress_bar(scanned, total)
            filename = Path(current_file).name
            if len(filename) > 30:
                filename = filename[:27] + "..."
            print(f"\r{bar} {filename:<35}", end="", flush=True)

        scanner = VideoScanner(
            self.video_db,
            calculate_checksums=not no_checksum,
            progress_callback=progress_callback,
            num_workers=num_workers,
        )

        try:
            batch = scanner.scan(source_path, target_path, resume=resume)
            print()  # New line after progress bar

            # Show results
            stats = self.video_db.get_batch_stats(batch.id)
            total = stats['total'] or 0
            with_metadata = stats['with_metadata'] or 0
            print("\n✅ Video Scan Complete!")
            print("-" * 40)
            print(f"Batch ID: {batch.id}")
            print(f"Total files: {total}")
            print(f"Files with metadata date: {with_metadata}")
            print(f"Files without metadata date: {total - with_metadata}")
            print(f"Total size: {format_size(stats['total_size'] or 0)}")
            print("-" * 40)
            print(f"\nTo copy files, run: photo-import video-copy --batch {batch.id}")

        except KeyboardInterrupt:
            print("\n\n⚠️  Scan interrupted. Progress has been saved.")
            print("Run the same command to resume.")
            sys.exit(1)

        except Exception as e:
            print(f"\n\n❌ Error: {e}")
            logging.exception("Video scan failed")
            sys.exit(1)

    def video_copy(
        self,
        batch_id: Optional[int] = None,
        dry_run: bool = False,
        skip_no_metadata: bool = False,
        use_file_date: bool = True,
    ):
        """Copy scanned videos to target directory."""
        # Get batch
        if batch_id is None:
            batch = self.video_db.get_latest_batch()
            if not batch:
                print("❌ No video batches found. Run 'video-scan' first.")
                sys.exit(1)
            batch_id = batch.id
        else:
            batch = self.video_db.get_batch(batch_id)
            if not batch:
                print(f"❌ Video batch {batch_id} not found.")
                sys.exit(1)

        stats = self.video_db.get_batch_stats(batch_id)

        print(f"\n🎬 Video Import Tool v{__version__}")
        print("=" * 50)
        print(f"Batch: #{batch_id}")
        print(f"Status: {batch.status.value}")
        print(f"Source: {batch.source_directory}")
        print(f"Target: {batch.target_directory}")
        print(f"Pending files: {stats['pending']}")
        if dry_run:
            print("Mode: DRY RUN (no files will be copied)")
        print("=" * 50)

        if stats['pending'] == 0:
            print("\n✅ No pending video files to copy.")
            return

        def progress_callback(copied: int, total: int, current_file: str):
            bar = progress_bar(copied, total)
            filename = Path(current_file).name
            if len(filename) > 30:
                filename = filename[:27] + "..."
            print(f"\r{bar} {filename:<35}", end="", flush=True)

        copier = VideoCopier(
            self.video_db,
            use_file_date_fallback=use_file_date,
            skip_no_metadata=skip_no_metadata,
            progress_callback=progress_callback
        )

        try:
            result = copier.copy(batch_id, dry_run=dry_run)
            print()  # New line after progress bar

            # Show results
            print("\n✅ Video Copy Complete!")
            print("-" * 40)
            print(f"Files copied: {result['copied']}")
            print(f"Files skipped: {result['skipped']}")
            print(f"Files failed: {result['failed']}")
            print(f"Duration: {format_duration(result['duration'])}")
            print("-" * 40)

            if result['failed'] > 0:
                print(f"\n⚠️  {result['failed']} files failed to copy.")
                print(f"Run 'photo-import video-status --batch {batch_id}' for details.")
                print(f"Run 'photo-import video-retry --batch {batch_id}' to retry failed files.")

        except KeyboardInterrupt:
            print("\n\n⚠️  Copy interrupted. Progress has been saved.")
            print(f"Run 'photo-import video-copy --batch {batch_id}' to resume.")
            sys.exit(1)

        except Exception as e:
            print(f"\n\n❌ Error: {e}")
            logging.exception("Video copy failed")
            sys.exit(1)

    def video_status(self, batch_id: Optional[int] = None, show_failed: bool = False):
        """Show status of video batches."""
        if batch_id is not None:
            batch = self.video_db.get_batch(batch_id)
            if not batch:
                print(f"❌ Video batch {batch_id} not found.")
                sys.exit(1)
            batches = [batch]
        else:
            batches = self.video_db.list_batches(limit=10)

        if not batches:
            print("No video batches found.")
            return

        print(f"\n🎬 Video Import Tool v{__version__}")
        print("=" * 70)

        for batch in batches:
            stats = self.video_db.get_batch_stats(batch.id)

            status_emoji = {
                VideoBatchStatus.SCANNING: "🔍",
                VideoBatchStatus.SCANNED: "📋",
                VideoBatchStatus.COPYING: "📤",
                VideoBatchStatus.COMPLETED: "✅",
                VideoBatchStatus.FAILED: "❌",
                VideoBatchStatus.PAUSED: "⏸️",
            }.get(batch.status, "❓")

            print(f"\nVideo Batch #{batch.id} {status_emoji} {batch.status.value}")
            print("-" * 60)
            print(f"  Source:    {batch.source_directory}")
            print(f"  Target:    {batch.target_directory}")
            print(f"  Started:   {batch.started_at.strftime('%Y-%m-%d %H:%M:%S')}")

            if batch.scan_completed_at:
                print(f"  Scanned:   {batch.scan_completed_at.strftime('%Y-%m-%d %H:%M:%S')}")
            if batch.copy_started_at:
                print(f"  Copy began: {batch.copy_started_at.strftime('%Y-%m-%d %H:%M:%S')}")
            if batch.completed_at:
                print(f"  Completed: {batch.completed_at.strftime('%Y-%m-%d %H:%M:%S')}")

            print(f"\n  Files:")
            print(f"    Total:        {stats['total']}")
            print(f"    Pending:      {stats['pending']}")
            print(f"    Copied:       {stats['copied']}")
            print(f"    Skipped:      {stats['skipped']}")
            print(f"    Failed:       {stats['failed']}")
            print(f"    With metadata: {stats['with_metadata']}")
            print(f"    Size:         {format_size(stats['total_size'] or 0)}")

            # Show failed files if requested
            if show_failed and stats['failed'] > 0:
                failed_files = self.video_db.get_files_by_status(batch.id, VideoFileStatus.FAILED, limit=20)
                print(f"\n  Failed files (showing up to 20):")
                for f in failed_files:
                    print(f"    - {f.filename}: {f.error_message}")

        print("\n" + "=" * 70)

    def video_retry(self, batch_id: int):
        """Retry failed video files in a batch."""
        batch = self.video_db.get_batch(batch_id)
        if not batch:
            print(f"❌ Video batch {batch_id} not found.")
            sys.exit(1)

        stats = self.video_db.get_batch_stats(batch_id)
        if stats['failed'] == 0:
            print("✅ No failed video files to retry.")
            return

        print(f"\nRetrying {stats['failed']} failed video files...")

        def progress_callback(copied: int, total: int, current_file: str):
            bar = progress_bar(copied, total)
            print(f"\r{bar}", end="", flush=True)

        copier = VideoCopier(self.video_db, progress_callback=progress_callback)

        try:
            result = copier.retry_failed(batch_id)
            print()

            print("\n✅ Video Retry Complete!")
            print(f"Files copied: {result['copied']}")
            print(f"Files still failed: {result['failed']}")

        except Exception as e:
            print(f"\n❌ Error: {e}")
            sys.exit(1)

    def video_list_batches(self, limit: int = 10):
        """List all video batches."""
        self.video_status(batch_id=None)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Photo Import Tool - Organize photos by date",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scan a directory for photos
  photo-import scan /path/to/photos /path/to/organized

  # Copy scanned photos
  photo-import copy --batch 1

  # Check photo status
  photo-import status

  # Retry failed photo files
  photo-import retry --batch 1

  # Scan a directory for videos
  photo-import video-scan /path/to/videos /path/to/organized

  # Copy scanned videos
  photo-import video-copy --batch 1

  # Check video status
  photo-import video-status

  # Retry failed video files
  photo-import video-retry --batch 1

  # Expand flat date directories (2012_05_20 -> 2012/05/20)
  photo-import expand /path/to/photos

  # Browse photos/videos in web browser
  photo-import serve /path/to/photos
"""
    )

    parser.add_argument(
        '--version', action='version', version=f'photo-import {__version__}'
    )
    parser.add_argument(
        '--db', default='photo_import.db',
        help='Path to photo SQLite database file (default: photo_import.db)'
    )
    parser.add_argument(
        '--video-db', default='video_import.db',
        help='Path to video SQLite database file (default: video_import.db)'
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='Enable verbose logging'
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Scan command
    scan_parser = subparsers.add_parser('scan', help='Scan directory for photos')
    scan_parser.add_argument('source', help='Source directory to scan')
    scan_parser.add_argument('target', help='Target directory for organized photos')
    scan_parser.add_argument(
        '--no-checksum', action='store_true',
        help='Skip MD5 checksum calculation (faster)'
    )
    scan_parser.add_argument(
        '--no-resume', action='store_true',
        help='Start fresh, do not resume existing scan'
    )
    scan_parser.add_argument(
        '--workers', '-w', type=int,
        help='Number of parallel workers (default: auto, typically 4x CPU cores)'
    )

    # Copy command
    copy_parser = subparsers.add_parser('copy', help='Copy scanned photos to target')
    copy_parser.add_argument(
        '--batch', '-b', type=int,
        help='Batch ID to copy (default: latest)'
    )
    copy_parser.add_argument(
        '--dry-run', action='store_true',
        help='Simulate copy without actually copying files'
    )
    copy_parser.add_argument(
        '--skip-no-exif', action='store_true',
        help='Skip files without EXIF date'
    )
    copy_parser.add_argument(
        '--no-file-date', action='store_true',
        help='Do not use file date as fallback when EXIF is missing'
    )

    # Status command
    status_parser = subparsers.add_parser('status', help='Show batch status')
    status_parser.add_argument(
        '--batch', '-b', type=int,
        help='Show specific batch (default: all recent)'
    )
    status_parser.add_argument(
        '--show-failed', action='store_true',
        help='Show details of failed files'
    )

    # Retry command
    retry_parser = subparsers.add_parser('retry', help='Retry failed files')
    retry_parser.add_argument(
        '--batch', '-b', type=int, required=True,
        help='Batch ID to retry'
    )

    # List command
    list_parser = subparsers.add_parser('list', help='List all batches')
    list_parser.add_argument(
        '--limit', type=int, default=10,
        help='Number of batches to show'
    )

    # Expand command
    expand_parser = subparsers.add_parser(
        'expand',
        help='Expand flat date directories into hierarchy (2012_05_20 -> 2012/05/20)'
    )
    expand_parser.add_argument('source', help='Source directory containing date-named folders')
    expand_parser.add_argument(
        '--target', '-t',
        help='Target directory (default: same as source, in-place expansion)'
    )
    expand_parser.add_argument(
        '--dry-run', action='store_true',
        help='Simulate expansion without making changes'
    )
    expand_parser.add_argument(
        '--move', action='store_true',
        help='Move files instead of copying (only when target differs from source)'
    )

    # Serve command
    serve_parser = subparsers.add_parser('serve', help='Start web browser for photo navigation')
    serve_parser.add_argument('directory', help='Directory to serve')
    serve_parser.add_argument(
        '--port', '-p', type=int, default=8080,
        help='Port number (default: 8080)'
    )
    serve_parser.add_argument(
        '--host', default='127.0.0.1',
        help='Host to bind to (default: 127.0.0.1)'
    )
    serve_parser.add_argument(
        '--no-browser', action='store_true',
        help='Do not open browser automatically'
    )

    # ===========================================
    # Video commands
    # ===========================================

    # Video scan command
    video_scan_parser = subparsers.add_parser('video-scan', help='Scan directory for videos')
    video_scan_parser.add_argument('source', help='Source directory to scan')
    video_scan_parser.add_argument('target', help='Target directory for organized videos')
    video_scan_parser.add_argument(
        '--checksum', action='store_true',
        help='Calculate MD5 checksums (slow for videos, off by default)'
    )
    video_scan_parser.add_argument(
        '--no-resume', action='store_true',
        help='Start fresh, do not resume existing scan'
    )
    video_scan_parser.add_argument(
        '--workers', '-w', type=int,
        help='Number of parallel workers (default: auto, typically 4x CPU cores)'
    )

    # Video copy command
    video_copy_parser = subparsers.add_parser('video-copy', help='Copy scanned videos to target')
    video_copy_parser.add_argument(
        '--batch', '-b', type=int,
        help='Batch ID to copy (default: latest)'
    )
    video_copy_parser.add_argument(
        '--dry-run', action='store_true',
        help='Simulate copy without actually copying files'
    )
    video_copy_parser.add_argument(
        '--skip-no-metadata', action='store_true',
        help='Skip files without metadata date'
    )
    video_copy_parser.add_argument(
        '--no-file-date', action='store_true',
        help='Do not use file date as fallback when metadata is missing'
    )

    # Video status command
    video_status_parser = subparsers.add_parser('video-status', help='Show video batch status')
    video_status_parser.add_argument(
        '--batch', '-b', type=int,
        help='Show specific batch (default: all recent)'
    )
    video_status_parser.add_argument(
        '--show-failed', action='store_true',
        help='Show details of failed files'
    )

    # Video retry command
    video_retry_parser = subparsers.add_parser('video-retry', help='Retry failed video files')
    video_retry_parser.add_argument(
        '--batch', '-b', type=int, required=True,
        help='Batch ID to retry'
    )

    # Video list command
    video_list_parser = subparsers.add_parser('video-list', help='List all video batches')
    video_list_parser.add_argument(
        '--limit', type=int, default=10,
        help='Number of batches to show'
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    setup_logging(args.verbose)
    cli = CLI(args.db, args.video_db)

    if args.command == 'scan':
        cli.scan(
            args.source,
            args.target,
            no_checksum=args.no_checksum,
            resume=not args.no_resume,
            workers=args.workers,
        )
    elif args.command == 'copy':
        cli.copy(
            batch_id=args.batch,
            dry_run=args.dry_run,
            skip_no_exif=args.skip_no_exif,
            use_file_date=not args.no_file_date,
        )
    elif args.command == 'status':
        cli.status(batch_id=args.batch, show_failed=args.show_failed)
    elif args.command == 'retry':
        cli.retry(args.batch)
    elif args.command == 'list':
        cli.list_batches(args.limit)
    elif args.command == 'expand':
        expand_directories_cmd(
            args.source,
            args.target,
            dry_run=args.dry_run,
            move_files=args.move,
        )
    elif args.command == 'serve':
        serve_photos_cmd(
            args.directory,
            port=args.port,
            host=args.host,
            open_browser=not args.no_browser,
        )
    # Video commands
    elif args.command == 'video-scan':
        cli.video_scan(
            args.source,
            args.target,
            no_checksum=not args.checksum,
            resume=not args.no_resume,
            workers=args.workers,
        )
    elif args.command == 'video-copy':
        cli.video_copy(
            batch_id=args.batch,
            dry_run=args.dry_run,
            skip_no_metadata=args.skip_no_metadata,
            use_file_date=not args.no_file_date,
        )
    elif args.command == 'video-status':
        cli.video_status(batch_id=args.batch, show_failed=args.show_failed)
    elif args.command == 'video-retry':
        cli.video_retry(args.batch)
    elif args.command == 'video-list':
        cli.video_list_batches(args.limit)


def expand_directories_cmd(
    source: str,
    target: Optional[str] = None,
    dry_run: bool = False,
    move_files: bool = False,
):
    """Expand flat date directories into hierarchical structure."""
    from .expander import expand_directories

    source_path = Path(source).resolve()
    target_path = Path(target).resolve() if target else source_path

    print(f"\n📁 Directory Expander")
    print("=" * 50)
    print(f"Source: {source_path}")
    print(f"Target: {target_path}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    if target and target_path != source_path:
        print(f"Operation: {'MOVE' if move_files else 'COPY'}")
    else:
        print("Operation: IN-PLACE REORGANIZATION")
    print("=" * 50)

    def progress_callback(processed: int, total: int, current_dir: str):
        bar = progress_bar(processed, total)
        if len(current_dir) > 25:
            current_dir = current_dir[:22] + "..."
        print(f"\r{bar} {current_dir:<30}", end="", flush=True)

    try:
        result = expand_directories(
            str(source_path),
            str(target_path) if target else None,
            dry_run=dry_run,
            move_files=move_files,
            progress_callback=progress_callback,
        )
        print()  # New line after progress

        print("\n✅ Expansion Complete!")
        print("-" * 40)
        print(f"Directories processed: {result.dirs_processed}")
        print(f"Directories skipped: {result.dirs_skipped}")
        print(f"Files moved: {result.files_moved}")

        if result.errors:
            print(f"\n⚠️  {len(result.errors)} errors occurred:")
            for path, error in result.errors[:10]:
                print(f"  - {path}: {error}")
            if len(result.errors) > 10:
                print(f"  ... and {len(result.errors) - 10} more")

    except KeyboardInterrupt:
        print("\n\n⚠️  Operation interrupted.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


def serve_photos_cmd(
    directory: str,
    port: int = 8080,
    host: str = "127.0.0.1",
    open_browser: bool = True,
):
    """Start the photo browser web server."""
    from .web_server import run_server

    dir_path = Path(directory).resolve()

    if not dir_path.is_dir():
        print(f"❌ Directory not found: {dir_path}")
        sys.exit(1)

    try:
        run_server(
            str(dir_path),
            port=port,
            host=host,
            open_browser=open_browser,
        )
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

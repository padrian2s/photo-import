"""
Video metadata extraction from video files.

Extracts creation date from video metadata using file system dates
and optional ffprobe if available.
"""

import os
import subprocess
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# Supported video extensions
SUPPORTED_VIDEO_EXTENSIONS = {
    # Common formats
    '.mp4', '.m4v', '.mov', '.avi', '.mkv', '.wmv', '.flv',
    # HD video formats
    '.mts', '.m2ts', '.ts',
    # Mobile formats
    '.3gp', '.3g2',
    # Web formats
    '.webm', '.ogv',
    # Other
    '.mpg', '.mpeg', '.vob', '.divx', '.asf',
}


def is_supported_video(filepath: str | Path) -> bool:
    """Check if the file is a supported video format."""
    ext = Path(filepath).suffix.lower()
    return ext in SUPPORTED_VIDEO_EXTENSIONS


def get_video_date_ffprobe(filepath: str | Path) -> Optional[datetime]:
    """
    Extract creation date using ffprobe (if available).

    Looks for creation_time in format metadata.
    """
    try:
        result = subprocess.run(
            [
                'ffprobe', '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                str(filepath)
            ],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode == 0:
            data = json.loads(result.stdout)
            tags = data.get('format', {}).get('tags', {})

            # Try different tag names
            for tag in ['creation_time', 'date', 'DATE']:
                if tag in tags:
                    date_str = tags[tag]
                    # Try to parse ISO format
                    try:
                        # Handle formats like "2020-03-15T10:30:45.000000Z"
                        if 'T' in date_str:
                            date_str = date_str.split('.')[0].replace('Z', '')
                            return datetime.fromisoformat(date_str)
                        else:
                            return datetime.strptime(date_str, '%Y-%m-%d')
                    except (ValueError, TypeError):
                        continue

    except FileNotFoundError:
        logger.debug("ffprobe not found, using file dates only")
    except subprocess.TimeoutExpired:
        logger.debug(f"ffprobe timeout for {filepath}")
    except Exception as e:
        logger.debug(f"ffprobe failed for {filepath}: {e}")

    return None


def get_file_dates(filepath: str | Path) -> Tuple[datetime, datetime]:
    """
    Get file creation and modification dates from filesystem.

    Returns:
        Tuple of (creation_date, modification_date)
    """
    filepath = Path(filepath)
    stat = filepath.stat()

    # On macOS/Windows, st_birthtime is the creation time
    # On Linux, st_ctime is the metadata change time (not creation)
    try:
        creation_time = datetime.fromtimestamp(stat.st_birthtime)
    except AttributeError:
        # Linux fallback - use the earlier of ctime and mtime
        creation_time = datetime.fromtimestamp(min(stat.st_ctime, stat.st_mtime))

    modification_time = datetime.fromtimestamp(stat.st_mtime)

    return creation_time, modification_time


def get_video_date(filepath: str | Path) -> Optional[datetime]:
    """
    Extract the original creation date from video metadata.

    Tries ffprobe first, returns None if no metadata date found.
    File dates are handled separately as fallback.
    """
    filepath = Path(filepath)

    if not filepath.exists():
        logger.warning(f"File not found: {filepath}")
        return None

    if not is_supported_video(filepath):
        logger.debug(f"Unsupported file type: {filepath}")
        return None

    # Try ffprobe
    date = get_video_date_ffprobe(filepath)
    if date:
        return date

    logger.debug(f"No metadata date found for: {filepath}")
    return None


def get_video_metadata(filepath: str | Path) -> dict:
    """
    Get comprehensive metadata for a video file.

    Returns a dictionary with:
        - metadata_date: datetime or None
        - creation_date: datetime
        - modification_date: datetime
        - file_size: int
        - extension: str
    """
    filepath = Path(filepath)
    creation_date, modification_date = get_file_dates(filepath)

    return {
        'metadata_date': get_video_date(filepath),
        'creation_date': creation_date,
        'modification_date': modification_date,
        'file_size': filepath.stat().st_size,
        'extension': filepath.suffix.lower(),
    }

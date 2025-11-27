"""
EXIF metadata extraction from photo files.

Supports JPEG, TIFF, PNG, HEIC, and RAW formats.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import logging

try:
    from PIL import Image
    from PIL.ExifTags import TAGS
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

try:
    import exifread
    HAS_EXIFREAD = True
except ImportError:
    HAS_EXIFREAD = False

logger = logging.getLogger(__name__)

# Supported photo extensions
SUPPORTED_EXTENSIONS = {
    # JPEG
    '.jpg', '.jpeg', '.jpe', '.jif', '.jfif',
    # TIFF
    '.tif', '.tiff',
    # PNG (limited EXIF support)
    '.png',
    # RAW formats
    '.raw', '.cr2', '.cr3', '.nef', '.arw', '.dng', '.orf', '.rw2', '.pef', '.srw',
    # HEIC/HEIF (iPhone)
    '.heic', '.heif',
    # Other
    '.webp', '.bmp',
}

# EXIF date tags to try, in order of preference
EXIF_DATE_TAGS = [
    'EXIF DateTimeOriginal',      # When photo was taken
    'EXIF DateTimeDigitized',     # When photo was digitized
    'Image DateTime',              # Last modification in camera
    'DateTimeOriginal',
    'DateTimeDigitized',
    'DateTime',
]

# PIL EXIF tag IDs
PIL_DATE_TAGS = [
    36867,  # DateTimeOriginal
    36868,  # DateTimeDigitized
    306,    # DateTime
]

# Date formats commonly found in EXIF
EXIF_DATE_FORMATS = [
    '%Y:%m:%d %H:%M:%S',
    '%Y-%m-%d %H:%M:%S',
    '%Y/%m/%d %H:%M:%S',
    '%Y:%m:%d',
    '%Y-%m-%d',
]


def is_supported_photo(filepath: str | Path) -> bool:
    """Check if the file is a supported photo format."""
    ext = Path(filepath).suffix.lower()
    return ext in SUPPORTED_EXTENSIONS


def parse_exif_date(date_string: str) -> Optional[datetime]:
    """Parse EXIF date string to datetime object."""
    if not date_string:
        return None

    # Clean up the string
    date_string = str(date_string).strip()

    # Handle subsecond precision if present
    if '.' in date_string:
        date_string = date_string.split('.')[0]

    for fmt in EXIF_DATE_FORMATS:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue

    logger.debug(f"Could not parse date string: {date_string}")
    return None


def get_exif_date_with_exifread(filepath: str | Path) -> Optional[datetime]:
    """Extract EXIF date using exifread library."""
    if not HAS_EXIFREAD:
        return None

    try:
        with open(filepath, 'rb') as f:
            tags = exifread.process_file(f, details=False, stop_tag='DateTimeOriginal')

            for tag_name in EXIF_DATE_TAGS:
                if tag_name in tags:
                    date_str = str(tags[tag_name])
                    date = parse_exif_date(date_str)
                    if date:
                        return date
    except Exception as e:
        logger.debug(f"exifread failed for {filepath}: {e}")

    return None


def get_exif_date_with_pil(filepath: str | Path) -> Optional[datetime]:
    """Extract EXIF date using PIL/Pillow library."""
    if not HAS_PIL:
        return None

    try:
        with Image.open(filepath) as img:
            exif_data = img._getexif()
            if exif_data:
                for tag_id in PIL_DATE_TAGS:
                    if tag_id in exif_data:
                        date_str = exif_data[tag_id]
                        date = parse_exif_date(date_str)
                        if date:
                            return date
    except Exception as e:
        logger.debug(f"PIL failed for {filepath}: {e}")

    return None


def get_exif_date(filepath: str | Path) -> Optional[datetime]:
    """
    Extract the original creation date from EXIF metadata.

    Tries multiple methods and returns the first successful result.
    Returns None if no EXIF date can be extracted.
    """
    filepath = Path(filepath)

    if not filepath.exists():
        logger.warning(f"File not found: {filepath}")
        return None

    if not is_supported_photo(filepath):
        logger.debug(f"Unsupported file type: {filepath}")
        return None

    # Try exifread first (better for RAW formats)
    date = get_exif_date_with_exifread(filepath)
    if date:
        return date

    # Fall back to PIL
    date = get_exif_date_with_pil(filepath)
    if date:
        return date

    logger.debug(f"No EXIF date found for: {filepath}")
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


def get_photo_metadata(filepath: str | Path) -> dict:
    """
    Get comprehensive metadata for a photo file.

    Returns a dictionary with:
        - exif_date: datetime or None
        - creation_date: datetime
        - modification_date: datetime
        - file_size: int
        - extension: str
    """
    filepath = Path(filepath)
    creation_date, modification_date = get_file_dates(filepath)

    return {
        'exif_date': get_exif_date(filepath),
        'creation_date': creation_date,
        'modification_date': modification_date,
        'file_size': filepath.stat().st_size,
        'extension': filepath.suffix.lower(),
    }

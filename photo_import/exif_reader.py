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

# exifread logs a warning for anything it cannot parse. The UI asks it about
# every file the user opens, so those are expected, not worth printing.
logging.getLogger('exifread').setLevel(logging.ERROR)

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


# Tags worth a line of their own, in the order a photographer reads them.
# Everything else still shows up under "all tags".
CURATED_TAGS = [
    'EXIF LensModel',
    'EXIF DateTimeOriginal',
    'EXIF ExposureTime',
    'EXIF FNumber',
    'EXIF ISOSpeedRatings',
    'EXIF FocalLength',
    'EXIF FocalLengthIn35mmFilm',
    'EXIF ExposureProgram',
    'EXIF Flash',
    'EXIF WhiteBalance',
    'Image Orientation',
    'Image Software',
]

# Binary blobs and thumbnails - useless in a table, huge in JSON
SKIP_TAG_PARTS = ('MakerNote', 'Thumbnail', 'ImageDescription Padding',
                  'Tag 0x', 'Padding')

MAX_TAG_LENGTH = 200


def _ratio(value) -> Optional[float]:
    """A single rational, however the file happened to spell it.

    exifread usually hands back a Ratio (a Fraction), but a tag written as a
    pair of longs arrives as two plain ints - and Pillow gives tuples.
    """
    if value is None:
        return None

    try:
        if isinstance(value, (list, tuple)):
            if len(value) >= 2 and value[1]:
                return float(value[0]) / float(value[1])
            return float(value[0]) if value else None
        if isinstance(value, int):
            return float(value)
        # Fraction, Ratio, Decimal, float - all answer to float()
        return float(value)
    except (TypeError, ValueError, ZeroDivisionError, IndexError):
        return None


def _tag_ratio(tag) -> Optional[float]:
    """The rational behind an exifread tag - Ratio, or [num, den]."""
    values = getattr(tag, 'values', None)
    if not values:
        return None

    first = values[0]
    if isinstance(first, int) and len(values) >= 2 and isinstance(values[1], int):
        return _ratio([first, values[1]])
    return _ratio(first)


def _format_shutter(seconds) -> Optional[str]:
    """1/250 s, or 2.5 s for the long ones."""
    if seconds is None or seconds <= 0:
        return None
    if seconds >= 1:
        return f"{seconds:g} s"
    return f"1/{round(1 / seconds)} s"


def _format_aperture(number) -> Optional[str]:
    return f"f/{number:g}" if number else None


def _format_focal(length) -> Optional[str]:
    return f"{length:g} mm" if length else None


def _gps_degrees(coordinate, reference) -> Optional[float]:
    """exifread keeps GPS as [degrees, minutes, seconds] plus a N/S/E/W ref."""
    try:
        parts = [_ratio(part) for part in coordinate.values]
        if len(parts) < 3 or any(part is None for part in parts):
            return None
        degrees = parts[0] + parts[1] / 60 + parts[2] / 3600
        if str(reference).strip().upper() in ('S', 'W'):
            degrees = -degrees
        return round(degrees, 5)
    except (AttributeError, TypeError, ValueError):
        return None


def _camera_name(make: str, model: str) -> Optional[str]:
    """Canon writes "Canon" and "Canon EOS R5" - do not say Canon twice."""
    make, model = (make or '').strip(), (model or '').strip()
    if model and make and model.lower().startswith(make.lower()):
        return model
    return ' '.join(part for part in (make, model) if part) or None


def _exifread_summary(filepath: Path) -> Optional[dict]:
    """Every tag exifread can find, curated into fields plus the raw list."""
    if not HAS_EXIFREAD:
        return None

    try:
        with open(filepath, 'rb') as handle:
            tags = exifread.process_file(handle, details=False)
    except Exception as e:  # noqa: BLE001 - a corrupt file is not an error here
        logger.debug(f"exifread failed for {filepath}: {e}")
        return None

    if not tags:
        return None

    def text(name) -> str:
        return str(tags[name]).strip() if name in tags else ''

    fields = []

    def add(label, value):
        if value:
            fields.append([label, str(value)])

    add('Camera', _camera_name(text('Image Make'), text('Image Model')))
    add('Lens', text('EXIF LensModel') or text('MakerNote LensModel'))

    taken = parse_exif_date(text('EXIF DateTimeOriginal') or text('Image DateTime'))
    add('Taken', taken.strftime('%Y-%m-%d %H:%M:%S') if taken else None)

    if 'EXIF ExposureTime' in tags:
        add('Exposure', _format_shutter(_tag_ratio(tags['EXIF ExposureTime'])))
    if 'EXIF FNumber' in tags:
        add('Aperture', _format_aperture(_tag_ratio(tags['EXIF FNumber'])))

    add('ISO', text('EXIF ISOSpeedRatings') or text('EXIF PhotographicSensitivity'))

    if 'EXIF FocalLength' in tags:
        focal = _format_focal(_tag_ratio(tags['EXIF FocalLength']))
        equivalent = text('EXIF FocalLengthIn35mmFilm')
        if focal and equivalent and equivalent != '0' and focal != f"{equivalent} mm":
            focal = f"{focal} ({equivalent} mm eq.)"
        add('Focal length', focal)

    width = text('EXIF ExifImageWidth') or text('Image ImageWidth')
    height = text('EXIF ExifImageLength') or text('Image ImageLength')
    if width and height:
        add('Dimensions', f"{width} \u00d7 {height}")

    add('Orientation', text('Image Orientation'))
    add('Flash', text('EXIF Flash'))
    add('White balance', text('EXIF WhiteBalance'))
    add('Exposure program', text('EXIF ExposureProgram'))
    add('Software', text('Image Software'))

    if 'GPS GPSLatitude' in tags and 'GPS GPSLongitude' in tags:
        latitude = _gps_degrees(tags['GPS GPSLatitude'], text('GPS GPSLatitudeRef'))
        longitude = _gps_degrees(tags['GPS GPSLongitude'], text('GPS GPSLongitudeRef'))
        if latitude is not None and longitude is not None:
            add('GPS', f"{latitude}, {longitude}")

    raw = []
    for name in sorted(tags):
        if any(part in name for part in SKIP_TAG_PARTS):
            continue
        value = str(tags[name]).strip()
        if not value or not value.isprintable():
            continue
        if len(value) > MAX_TAG_LENGTH:
            value = value[:MAX_TAG_LENGTH] + '\u2026'
        raw.append([name, value])

    if not fields and not raw:
        return None

    return {'fields': fields, 'tags': raw, 'source': 'exifread'}


def _pillow_summary(filepath: Path) -> Optional[dict]:
    """What Pillow can see - the way in for HEIC, and a backstop for JPEG."""
    if not HAS_PIL:
        return None

    try:
        from PIL.ExifTags import GPSTAGS

        with Image.open(filepath) as image:
            size = image.size
            exif = image.getexif()
            named = {TAGS.get(tag_id, str(tag_id)): value for tag_id, value in exif.items()}
            try:
                detail = exif.get_ifd(0x8769)   # the Exif sub-IFD holds the exposure
                named.update({TAGS.get(tag_id, str(tag_id)): value
                              for tag_id, value in detail.items()})
            except Exception:  # noqa: BLE001 - not every file has one
                pass
            try:
                gps = exif.get_ifd(0x8825)
                gps_named = {GPSTAGS.get(tag_id, str(tag_id)): value
                             for tag_id, value in gps.items()}
            except Exception:  # noqa: BLE001
                gps_named = {}
    except Exception as e:  # noqa: BLE001
        logger.debug(f"Pillow failed for {filepath}: {e}")
        return None

    fields = []

    def add(label, value):
        if value not in (None, '', 0):
            fields.append([label, str(value)])

    def plain(name):
        value = named.get(name)
        return str(value).strip().strip('\x00') if value is not None else ''

    add('Camera', _camera_name(plain('Make'), plain('Model')))
    add('Lens', plain('LensModel'))

    taken = parse_exif_date(plain('DateTimeOriginal') or plain('DateTime'))
    add('Taken', taken.strftime('%Y-%m-%d %H:%M:%S') if taken else None)

    add('Exposure', _format_shutter(_ratio(named.get('ExposureTime'))))
    add('Aperture', _format_aperture(_ratio(named.get('FNumber'))))
    add('ISO', plain('ISOSpeedRatings') or plain('PhotographicSensitivity'))

    focal = _format_focal(_ratio(named.get('FocalLength')))
    equivalent = plain('FocalLengthIn35mmFilm')
    if focal and equivalent and equivalent != '0' and focal != f"{equivalent} mm":
        focal = f"{focal} ({equivalent} mm eq.)"
    add('Focal length', focal)

    # Pillow knows the real size; the EXIF tags still say the pre-crop one
    add('Dimensions', f"{size[0]} \u00d7 {size[1]}" if size else None)
    add('Orientation', plain('Orientation'))
    add('Software', plain('Software'))

    if 'GPSLatitude' in gps_named and 'GPSLongitude' in gps_named:
        def degrees(parts, reference):
            try:
                value = (float(parts[0]) + float(parts[1]) / 60 + float(parts[2]) / 3600)
                if str(reference).strip().upper() in ('S', 'W'):
                    value = -value
                return round(value, 5)
            except (TypeError, ValueError, IndexError):
                return None

        latitude = degrees(gps_named['GPSLatitude'], gps_named.get('GPSLatitudeRef', ''))
        longitude = degrees(gps_named['GPSLongitude'], gps_named.get('GPSLongitudeRef', ''))
        if latitude is not None and longitude is not None:
            add('GPS', f"{latitude}, {longitude}")

    raw = []
    for name in sorted(named):
        if any(part in str(name) for part in SKIP_TAG_PARTS):
            continue
        value = str(named[name]).strip().strip('\x00')
        if not value or not value.isprintable():
            continue
        if len(value) > MAX_TAG_LENGTH:
            value = value[:MAX_TAG_LENGTH] + '\u2026'
        raw.append([name, value])

    if not fields and not raw:
        return None

    return {'fields': fields, 'tags': raw, 'source': 'pillow'}


def read_camera_fields(filepath: str | Path) -> tuple:
    """Just the camera, lens and date - the cheap read, for indexing a library.

    read_exif_summary() builds the whole tag table, which is wasted work when
    walking a hundred thousand files. Make and Model sit in the first IFD, so
    exifread can stop as soon as it has the date and never touch the maker
    note. Returns (camera, lens, taken_at); any of them may be None.
    """
    filepath = Path(filepath)

    if HAS_EXIFREAD:
        try:
            with open(filepath, 'rb') as handle:
                tags = exifread.process_file(handle, details=False,
                                             stop_tag='DateTimeOriginal')
            if tags:
                def text(name):
                    return str(tags[name]).strip().strip('\x00') if name in tags else ''

                camera = _camera_name(text('Image Make'), text('Image Model'))
                lens = text('EXIF LensModel') or None
                taken = parse_exif_date(text('EXIF DateTimeOriginal')
                                        or text('Image DateTime'))
                if camera or taken:
                    return camera, lens, taken
        except Exception as e:  # noqa: BLE001 - an unreadable file is not fatal
            logger.debug(f"camera read failed for {filepath}: {e}")

    if HAS_PIL:
        try:
            with Image.open(filepath) as image:
                exif = image.getexif()
                named = {TAGS.get(tag_id, str(tag_id)): value
                         for tag_id, value in exif.items()}
                try:
                    detail = exif.get_ifd(0x8769)
                    named.update({TAGS.get(tag_id, str(tag_id)): value
                                  for tag_id, value in detail.items()})
                except Exception:  # noqa: BLE001
                    pass

            def plain(name):
                value = named.get(name)
                return str(value).strip().strip('\x00') if value is not None else ''

            camera = _camera_name(plain('Make'), plain('Model'))
            taken = parse_exif_date(plain('DateTimeOriginal') or plain('DateTime'))
            return camera, (plain('LensModel') or None), taken
        except Exception as e:  # noqa: BLE001
            logger.debug(f"camera read via Pillow failed for {filepath}: {e}")

    return None, None, None


def read_exif_summary(filepath: str | Path) -> dict:
    """What the camera wrote into a photo, ready to put on screen.

    RAW and JPEG come through exifread, HEIC through Pillow; whichever finds
    something first wins. A file with nothing to say returns empty lists
    rather than raising - "no EXIF" is an answer, not a failure.
    """
    filepath = Path(filepath)
    empty = {'fields': [], 'tags': [], 'source': None}

    if not filepath.is_file():
        return empty

    for reader in (_exifread_summary, _pillow_summary):
        try:
            summary = reader(filepath)
        except Exception as e:  # noqa: BLE001 - never fail a page over metadata
            logger.debug(f"{reader.__name__} failed for {filepath}: {e}")
            continue
        if summary and (summary['fields'] or summary['tags']):
            _add_dimensions(filepath, summary)
            return summary

    return empty


def _add_dimensions(filepath: Path, summary: dict):
    """Not every camera writes the size into EXIF, but the pixels know it."""
    if not HAS_PIL or any(label == 'Dimensions' for label, _ in summary['fields']):
        return

    try:
        with Image.open(filepath) as image:
            width, height = image.size
    except Exception:  # noqa: BLE001 - RAW and video simply do not open
        return

    # right after the exposure block, where a reader looks for it
    summary['fields'].append(['Dimensions', f"{width} \u00d7 {height}"])


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

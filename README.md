# Photo Import Tool

A command-line tool to scan directories for photos, extract EXIF creation dates, and organize them into a date-based directory structure (`YYYY_MM_DD`).

## Features

- **EXIF Date Extraction**: Reads original creation date from EXIF metadata (not file system dates)
- **Multiple Format Support**: JPEG, TIFF, PNG, RAW formats (CR2, NEF, ARW, DNG, etc.), HEIC/HEIF
- **Batch Processing**: Tracks progress in SQLite database for resumable operations
- **Resume Support**: Interruptions are saved; resume by running the same command
- **Duplicate Detection**: MD5 checksums to identify duplicate files
- **Dry Run Mode**: Preview what would happen without copying files
- **Progress Tracking**: Real-time progress bars and detailed statistics

## Installation

```bash
# Clone or download the project
cd photo_import

# Install with pip
pip install -e .

# Or install dependencies directly
pip install -r requirements.txt
```

For HEIC support (iPhone photos):
```bash
pip install pillow-heif
```

## Usage

### 1. Scan Source Directory

First, scan your source directory to catalog all photos:

```bash
photo-import scan /path/to/source/photos /path/to/target/organized
```

Options:
- `--no-checksum`: Skip MD5 calculation (faster but no duplicate detection)
- `--no-resume`: Start fresh instead of resuming an existing scan

### 2. Copy Photos

After scanning, copy the photos to the target directory:

```bash
photo-import copy --batch 1
```

Options:
- `--batch N`: Specify batch ID (default: latest)
- `--dry-run`: Simulate copy without actually moving files
- `--skip-no-exif`: Skip files without EXIF date instead of using file date
- `--no-file-date`: Don't use file modification date as fallback

### 3. Check Status

View status of batches:

```bash
photo-import status
photo-import status --batch 1 --show-failed
```

### 4. Retry Failed Files

If some files failed to copy:

```bash
photo-import retry --batch 1
```

## Directory Structure

Photos are organized into folders by date:

```
target/
├── 2024_01_15/
│   ├── IMG_1234.jpg
│   └── DSC_5678.jpg
├── 2024_01_16/
│   └── photo.jpg
└── unknown_date/
    └── no_exif.jpg
```

## Database

All progress is stored in `photo_import.db` (SQLite). This enables:

- **Resume capability**: If interrupted, just run the same command to continue
- **Batch tracking**: Multiple import operations tracked separately
- **Audit trail**: See what was copied, when, and any errors

### Database Schema

- `batches`: Tracks import operations (source, target, progress, timestamps)
- `photo_files`: Individual file records (paths, EXIF date, status, checksum)

## Examples

```bash
# Full workflow
photo-import scan ~/Photos/Camera ~/Photos/Organized
photo-import copy --batch 1

# Check what will happen without copying
photo-import copy --batch 1 --dry-run

# Resume interrupted scan
photo-import scan ~/Photos/Camera ~/Photos/Organized

# View all batches
photo-import list

# See failed files
photo-import status --batch 1 --show-failed

# Retry failed files
photo-import retry --batch 1
```

## Supported Formats

| Format | Extensions |
|--------|------------|
| JPEG | .jpg, .jpeg, .jpe, .jif, .jfif |
| TIFF | .tif, .tiff |
| PNG | .png |
| RAW | .raw, .cr2, .cr3, .nef, .arw, .dng, .orf, .rw2, .pef, .srw |
| HEIC | .heic, .heif |
| Other | .webp, .bmp |

## EXIF Date Priority

The tool looks for dates in this order:
1. `EXIF DateTimeOriginal` - When the photo was taken
2. `EXIF DateTimeDigitized` - When the photo was digitized
3. `Image DateTime` - Last modification in camera

If no EXIF date is found:
- By default, uses file modification date as fallback
- With `--skip-no-exif`, files are skipped
- Otherwise, placed in `unknown_date/` folder

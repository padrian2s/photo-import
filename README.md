# Photo Import Tool

A command-line tool to scan directories for photos, extract EXIF creation dates, and organize them into a date-based directory structure (`YYYY_MM_DD`).

## Features

- **EXIF Date Extraction**: Reads original creation date from EXIF metadata (not file system dates)
- **Multiple Format Support**: JPEG, TIFF, PNG, RAW formats (CR2, NEF, ARW, DNG, etc.), HEIC/HEIF
- **Batch Processing**: Tracks progress in SQLite database for resumable operations
- **Resume Support**: Interruptions are saved; resume by running the same command
- **Duplicate Detection**: MD5 checksums to identify duplicate files
- **Dry Run Mode**: Preview what would happen without copying files (the database is left untouched)
- **Progress Tracking**: Real-time progress bars and detailed statistics
- **Web UI**: `photo-import serve` browses the library and runs scan/copy/retry/expand from the browser

## Installation

```bash
# Clone or download the project
cd photo_import

# Install with pip
pip install -e .

# Or install dependencies directly
pip install -r requirements.txt
```

For HEIC support (iPhone photos) - also needed for HEIC thumbnails and conflict
previews in the web UI:
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

### 5. Web UI

The web interface exposes the same operations as the CLI, so a full import can be
done without the terminal:

```bash
photo-import serve /path/to/target/organized
```

Tabs in the UI:

| Tab | CLI equivalent |
|-----|----------------|
| **Browse** | browse the served folder, thumbnails, lightbox, keyboard navigation, favorites, **V** for every photo below the current folder, and **B** for the files of a single import |
| **Favorites** | every starred photo/video in one grid |
| **Storage** | size in GB and photo/video counts per folder - per year inside the library - with drill-down |
| **Import** | `scan` / `video-scan`, plus a shortcut for `copy` / `video-copy` of the latest batch |
| **Batches** | `status`, `list`, `copy --dry-run`, `copy`, `retry`, and conflict review (photos and videos) |
| **Tools** | `expand`, job history, server info |

Notes:

- The UI is dark by default and has a light side; the ◐ button in the top right
  switches between them and remembers the choice in the browser.
- A running job shows as a pill in the top bar (name, progress, elapsed); click
  it for the detail panel with the current file, counts and **Stop**. Progress is
  saved exactly like a `Ctrl+C` in the CLI, so re-running resumes.
- The bar above the grid holds the breadcrumbs and whatever is narrowing the
  grid; the bar below it holds the file count, the pages and how many items to
  show. **T** hides the folder tree when the grid wants the room.
- **List** (**L**) is a real table - name, kind, size and modified date - and
  **Grid** (**G**) is the thumbnail wall.
- One job runs at a time; starting a second one returns a "still running" error.
- Source and target folders can be typed or picked with the **Browse** button, which walks the local filesystem, and **Current** fills in the folder currently open in the Browse tab.
- The line under the target field spells out the exact destination (`.../organized_photos/YYYY/MM/DD/`) before anything is copied.
- Under the source field, **Mounted volumes** lists what is plugged in (`/Volumes`, `/media`, `/mnt`), plus the served folder (★) and the home folder; one click fills in the source. The ↻ button rescans after plugging in a card.
- The server uses the databases passed to the command: `photo-import --db my.db --video-db my_video.db serve ...`.
- **S** opens a size dialog for whatever is selected in the hierarchy - the folder focused in the tree, otherwise the folder tile selected in the grid, otherwise the folder you are in. It shows the total size, photo/RAW/video counts and a per-subfolder table you can drill into; **Open in Storage** hands it over to the full view.
- The **import picker** in the toolbar (**B**) lists every import that copied
  files, newest first - `#8 - 2024-07-14 18:20 - 412 photos - 100MSDCF`. Pick one
  and the grid shows only that batch's files, wherever they landed in the tree,
  with the folder printed under each tile. A chip in the path bar names the
  import and counts what it cannot show - files deleted since, and files that
  landed outside the served folder; the × on it (or **B** again, or clicking any
  folder) goes back to where you were. **Batches - Show N imported files** opens the same view.
- **V** flattens a year, month or day into one thumbnail grid - every photo below it, paginated, with the folder printed under each tile. Press **V** again (or walk to another folder) to go back to the normal listing.
- RAW files (`.arw`, `.orf`, `.cr2`, `.nef`, `.dng`, ...) are listed and counted as photos, shown with a RAW tile instead of a thumbnail - Pillow cannot render them.
- **Storage** measures every subfolder of the one you are looking at, so opening it on `organized_photos` gives the per-year breakdown. Results are cached until you press **Rescan**.
- Favorites are stored in `photo_favorites.db` (`--favorites-db` to move it), keyed by the path inside the served folder. Star a file from the grid, from the lightbox, or with the **F** key; **Clean up missing** forgets favorites whose file is gone. They work in `--no-import` mode too.
- The import tools read and write anywhere on the machine. They are meant for a server bound to localhost (the default); use `--no-import` to serve a browse-only UI.

## Directory Structure

Imports always land in `organized_photos/` inside the target, split by year/month/day:

```
target/
└── organized_photos/
    ├── 2024/
    │   └── 01/
    │       ├── 15/
    │       │   ├── IMG_1234.jpg
    │       │   └── DSC_5678.jpg
    │       └── 16/
    │           └── photo.jpg
    └── 2025/
        └── 07/
            └── 04/
                └── VID_0001.mp4
```

Photos and videos share the same tree. If the target already *is* an
`organized_photos` folder, it is used as-is instead of being nested again - so
`--target /Volumes/poze` and `--target /Volumes/poze/organized_photos` write to
the same place.

Files with no EXIF/metadata date fall back to the file date, so nothing is ever
written outside this structure.

## Conflicts

Existing files are **never overwritten and never silently renamed**. When a file
with the same name already sits in the destination folder, the import parks it
as a `conflict` and the batch stays `paused` until you decide.

In the web UI, **Batches → Review conflicts** shows both files side by side -
imported vs. already in the library - with previews, sizes and dates, and a badge
saying whether the two are byte-identical. For each file (or for all remaining at
once) you pick:

- **Keep existing** - the imported file is skipped
- **Keep both** - copied next to it as `name_1.jpg`
- **Replace with imported** - overwrites, after a confirmation

Conflicts still waiting on a decision are counted in `status` and in the batch
list.

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
- By default, uses file creation date, then modification date, as fallback
- With `--skip-no-exif`, files are skipped instead

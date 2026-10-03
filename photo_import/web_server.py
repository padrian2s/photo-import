"""
Web server for browsing photos in hierarchical directory structure.
"""

import json
import mimetypes
import os
import urllib.parse
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from io import BytesIO

from .expander import get_directory_tree, list_images_in_directory
from .favorites import FavoritesStore
from .jobs import JobBusy, JobManager, PHOTO, VIDEO

# Try to import PIL for thumbnail generation
try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# Optional - teaches Pillow to read HEIC/HEIF (iPhone photos)
try:
    import pillow_heif
    pillow_heif.register_heif_opener()
    HAS_HEIF = True
except Exception:  # noqa: BLE001 - the library is optional
    HAS_HEIF = False


class PhotoBrowserHandler(SimpleHTTPRequestHandler):
    """HTTP request handler for photo browser."""

    root_directory: str = "."
    thumbnail_size: tuple = (200, 200)
    job_manager: Optional[JobManager] = None
    favorites: Optional[FavoritesStore] = None

    def __init__(self, *args, **kwargs):
        # Set the directory before calling parent __init__
        self.directory = self.root_directory
        super().__init__(*args, directory=self.root_directory, **kwargs)

    def do_GET(self):
        """Handle GET requests."""
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        # API endpoints
        if path == '/api/tree':
            self.send_tree()
        elif path == '/api/config':
            self.send_config()
        elif path == '/api/batches':
            self.send_batches(query)
        elif path == '/api/batch':
            self.send_batch_detail(query)
        elif path == '/api/jobs':
            self.send_jobs()
        elif path == '/api/job':
            self.send_job(query)
        elif path == '/api/fs':
            self.send_fs_listing(query)
        elif path == '/api/volumes':
            self.send_volumes()
        elif path == '/api/favorites':
            self.send_favorites()
        elif path == '/api/stats':
            self.send_directory_stats(query)
        elif path == '/api/all':
            self.send_recursive_media(query)
        elif path == '/api/batch-index':
            self.send_batch_index(query)
        elif path == '/api/batch-media':
            self.send_batch_media(query)
        elif path == '/api/conflicts':
            self.send_conflicts(query)
        elif path == '/api/preview':
            self.send_preview(query)
        elif path == '/api/list':
            dir_path = query.get('path', ['.'])[0]
            self.send_file_list(dir_path)
        elif path == '/api/images':
            dir_path = query.get('path', ['.'])[0]
            self.send_image_list(dir_path)
        elif path.startswith('/api/thumbnail/'):
            image_path = path[len('/api/thumbnail/'):]
            image_path = urllib.parse.unquote(image_path)
            self.send_thumbnail(image_path)
        elif path.startswith('/photo/'):
            image_path = path[len('/photo/'):]
            image_path = urllib.parse.unquote(image_path)
            self.send_photo(image_path)
        elif path == '/' or path == '/index.html':
            self.send_index()
        elif path == '/styles.css':
            self.send_styles()
        elif path == '/app.js':
            self.send_javascript()
        elif path == '/favicon.ico':
            self.send_favicon()
        else:
            # Return 404 for unknown paths (don't fall through to file system)
            self.send_error(404, "Not found")

    def do_POST(self):
        """Handle POST requests - import operations."""
        path = urllib.parse.urlparse(self.path).path

        # Favorites work even in browse-only mode
        if path in ('/api/favorites', '/api/favorites/prune'):
            try:
                if path.endswith('/prune'):
                    self.prune_favorites()
                else:
                    self.toggle_favorite(self.read_json_body())
            except ValueError as exc:
                self.send_json({"error": str(exc)}, 400)
            return

        handlers = {
            '/api/scan': self.start_scan_job,
            '/api/copy': self.start_copy_job,
            '/api/retry': self.start_retry_job,
            '/api/expand': self.start_expand_job,
            '/api/job/cancel': self.cancel_job,
            '/api/conflicts/resolve': self.start_resolve_job,
        }

        handler = handlers.get(path)
        if not handler:
            self.send_json({"error": "Not found"}, 404)
            return

        if self.job_manager is None:
            self.send_json({"error": "Import operations are disabled"}, 503)
            return

        try:
            payload = self.read_json_body()
        except ValueError as exc:
            self.send_json({"error": str(exc)}, 400)
            return

        try:
            handler(payload)
        except JobBusy as exc:
            self.send_json({"error": str(exc)}, 409)
        except (ValueError, FileNotFoundError, NotADirectoryError) as exc:
            self.send_json({"error": str(exc)}, 400)
        except Exception as exc:  # noqa: BLE001 - surfaced to the UI
            self.send_json({"error": f"Unexpected error: {exc}"}, 500)

    def read_json_body(self) -> dict:
        """Read and parse the JSON request body."""
        length = int(self.headers.get('Content-Length') or 0)
        if length <= 0:
            return {}
        if length > 1_000_000:
            raise ValueError("Request body too large")

        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(f"Invalid JSON body: {exc}") from exc

        if not isinstance(data, dict):
            raise ValueError("Request body must be a JSON object")
        return data

    def send_json(self, data: dict, status: int = 200):
        """Send JSON response."""
        content = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(content))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(content)

    # -------------------------------------------------------------------------
    # Import API (mirrors the CLI commands)
    # -------------------------------------------------------------------------

    def send_config(self):
        """Send server configuration to the UI."""
        from .scanner import DEFAULT_WORKERS

        manager = self.job_manager
        self.send_json({
            "root": self.root_directory,
            "has_pil": HAS_PIL,
            "has_heif": HAS_HEIF,
            "favorites": self.favorites.count() if self.favorites else 0,
            "import_enabled": manager is not None,
            "db_path": str(Path(manager.db_path).resolve()) if manager else None,
            "video_db_path": str(Path(manager.video_db_path).resolve()) if manager else None,
            "default_workers": DEFAULT_WORKERS,
            "home": str(Path.home()),
        })

    def send_batches(self, query: dict):
        """Send the batch list with per-batch statistics."""
        media = query.get('media', [PHOTO])[0]
        limit = min(int(query.get('limit', ['20'])[0] or 20), 100)

        if self.job_manager is None:
            self.send_json({"error": "Import operations are disabled"}, 503)
            return

        try:
            db = self.job_manager.database_for(media)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, 400)
            return

        batches = [
            batch_to_dict(batch, db.get_batch_stats(batch.id))
            for batch in db.list_batches(limit=limit)
        ]
        self.send_json({"media": media, "batches": batches})

    def send_batch_detail(self, query: dict):
        """Send a single batch with its failed files."""
        media = query.get('media', [PHOTO])[0]
        batch_id = query.get('id', [None])[0]

        if self.job_manager is None:
            self.send_json({"error": "Import operations are disabled"}, 503)
            return
        if not batch_id or not batch_id.isdigit():
            self.send_json({"error": "A numeric batch id is required"}, 400)
            return

        try:
            db = self.job_manager.database_for(media)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, 400)
            return

        batch = db.get_batch(int(batch_id))
        if not batch:
            self.send_json({"error": f"Batch {batch_id} not found"}, 404)
            return

        if media == VIDEO:
            from .video_models import VideoFileStatus as Status
        else:
            from .models import FileStatus as Status

        failed = db.get_files_by_status(batch.id, Status.FAILED, limit=50)
        self.send_json({
            "media": media,
            "batch": batch_to_dict(batch, db.get_batch_stats(batch.id)),
            "failed_files": [
                {"filename": f.filename, "source_path": f.source_path, "error": f.error_message}
                for f in failed
            ],
        })

    def send_jobs(self):
        """Send recent jobs and the currently running one."""
        if self.job_manager is None:
            self.send_json({"jobs": [], "active": None, "import_enabled": False})
            return

        active = self.job_manager.active_job()
        self.send_json({
            "jobs": [job.to_dict() for job in self.job_manager.list_jobs()],
            "active": active.id if active else None,
            "import_enabled": True,
        })

    def send_job(self, query: dict):
        """Send a single job by id."""
        job_id = query.get('id', [None])[0]
        if self.job_manager is None or not job_id or not job_id.isdigit():
            self.send_json({"error": "Job not found"}, 404)
            return

        job = self.job_manager.get(int(job_id))
        if not job:
            self.send_json({"error": "Job not found"}, 404)
            return

        self.send_json(job.to_dict())

    def send_fs_listing(self, query: dict):
        """List subdirectories of an absolute path (directory picker)."""
        if self.job_manager is None:
            self.send_json({"error": "Import operations are disabled"}, 503)
            return

        raw_path = query.get('path', [''])[0] or self.root_directory
        target = Path(raw_path).expanduser()
        if not target.is_absolute():
            target = Path(self.root_directory) / target

        try:
            target = target.resolve()
        except OSError as exc:
            self.send_json({"error": str(exc)}, 400)
            return

        if not target.is_dir():
            self.send_json({"error": f"Not a directory: {target}"}, 404)
            return

        dirs = []
        try:
            for item in sorted(target.iterdir(), key=lambda x: x.name.lower()):
                if item.name.startswith('.'):
                    continue
                if item.is_dir():
                    dirs.append({"name": item.name, "path": str(item)})
        except PermissionError:
            self.send_json({"error": f"Permission denied: {target}"}, 403)
            return

        parent = str(target.parent) if target.parent != target else None
        self.send_json({"path": str(target), "parent": parent, "dirs": dirs})

    def send_favorites(self):
        """Send every favorite, newest first, with its current file info."""
        if self.favorites is None:
            self.send_json({"favorites": [], "total": 0})
            return

        root = Path(self.root_directory).resolve()
        items = []
        for entry in self.favorites.list():
            full_path = root / entry["path"]
            extension = full_path.suffix.lower()
            folder = str(Path(entry["path"]).parent)
            info = {
                "path": entry["path"],
                "name": full_path.name,
                "folder": folder if folder != '.' else '',
                "added_at": entry["added_at"],
                "is_dir": False,
                "extension": extension,
                "is_image": extension in IMAGE_EXTENSIONS,
                "is_video": extension in VIDEO_EXTENSIONS,
                # RAW is a photo the browser cannot draw - it gets its own tile
                "is_raw": extension in RAW_EXTENSIONS,
                "favorite": True,
                "missing": not full_path.exists(),
            }
            if not info["missing"]:
                stat = full_path.stat()
                info["size"] = stat.st_size
                info["modified"] = stat.st_mtime
            items.append(info)

        self.send_json({"favorites": items, "total": len(items)})

    def toggle_favorite(self, payload: dict):
        """POST /api/favorites - add or remove a favorite."""
        if self.favorites is None:
            self.send_json({"error": "Favorites are unavailable"}, 503)
            return

        relative_path = str(payload.get('path', '')).strip()
        if not relative_path or relative_path == '.':
            raise ValueError("path is required")

        root = Path(self.root_directory).resolve()
        try:
            full_path = (root / relative_path).resolve()
            full_path.relative_to(root)
        except (ValueError, OSError):
            raise ValueError("Path is outside the served folder")

        wanted = payload.get('favorite')
        if wanted is None:
            wanted = not self.favorites.contains(relative_path)

        if wanted:
            if not full_path.is_file():
                raise ValueError("File not found")
            self.favorites.add(relative_path)
        else:
            self.favorites.remove(relative_path)

        self.send_json({
            "path": relative_path,
            "favorite": bool(wanted),
            "total": self.favorites.count(),
        })

    def send_directory_stats(self, query: dict):
        """Size and file counts for each subfolder - years, when pointed at the library."""
        relative_path = query.get('path', ['.'])[0] or '.'
        refresh = query.get('refresh', ['0'])[0] == '1'

        root = Path(self.root_directory).resolve()
        target = root if relative_path == '.' else root / relative_path

        if not target.is_dir():
            self.send_json({"error": "Directory not found"}, 404)
            return

        cache_key = str(target)
        cached = None if refresh else STATS_CACHE.get(cache_key)
        if cached is None:
            cached = collect_directory_stats(target, root)
            STATS_CACHE[cache_key] = cached

        self.send_json(cached)

    def prune_favorites(self):
        """POST /api/favorites/prune - forget favorites whose file is gone."""
        if self.favorites is None:
            self.send_json({"error": "Favorites are unavailable"}, 503)
            return

        removed = self.favorites.prune_missing(Path(self.root_directory).resolve())
        self.send_json({"removed": removed, "total": self.favorites.count()})

    def send_conflicts(self, query: dict):
        """List files parked as conflicts, with both sides described."""
        if self.job_manager is None:
            self.send_json({"error": "Import operations are disabled"}, 503)
            return

        media = query.get('media', [PHOTO])[0]
        batch_id = query.get('batch_id', [None])[0]
        if not batch_id or not batch_id.isdigit():
            self.send_json({"error": "A numeric batch_id is required"}, 400)
            return

        try:
            db = self.job_manager.database_for(media)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, 400)
            return

        page = max(1, int(query.get('page', ['1'])[0] or 1))
        per_page = min(int(query.get('per_page', ['12'])[0] or 12), 50)

        if media == VIDEO:
            from .video_models import VideoFileStatus as Status
        else:
            from .models import FileStatus as Status

        all_conflicts = db.get_files_by_status(int(batch_id), Status.CONFLICT)
        total = len(all_conflicts)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = min(page, total_pages)
        window = all_conflicts[(page - 1) * per_page: page * per_page]

        self.send_json({
            "media": media,
            "batch_id": int(batch_id),
            "conflicts": [describe_conflict(record, media) for record in window],
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": total_pages,
            },
        })

    def start_resolve_job(self, payload: dict):
        """POST /api/conflicts/resolve - apply the user's decision to conflicts."""
        batch_id = payload.get('batch_id')
        if not batch_id:
            raise ValueError("batch_id is required")

        file_ids = payload.get('file_ids')
        if file_ids is not None:
            file_ids = [int(value) for value in file_ids]

        job = self.job_manager.start_resolve(
            payload.get('media', PHOTO),
            batch_id=int(batch_id),
            action=str(payload.get('action', '')),
            file_ids=file_ids,
        )
        self.send_json(job.to_dict(), 202)

    def send_volumes(self):
        """List mounted volumes so an import source can be picked in one click."""
        if self.job_manager is None:
            self.send_json({"error": "Import operations are disabled"}, 503)
            return

        entries = []
        seen = set()

        def add(name: str, path: str, kind: str):
            resolved = str(Path(path))
            if resolved in seen:
                return
            try:
                if not Path(resolved).is_dir():
                    return
            except OSError:
                return
            seen.add(resolved)
            entries.append({"name": name, "path": resolved, "kind": kind})

        served = Path(self.root_directory)
        add(served.name or str(served), str(served), "served")

        for mount_root in ('/Volumes', '/media', '/mnt'):
            base = Path(mount_root)
            if not base.is_dir():
                continue
            try:
                for item in sorted(base.iterdir(), key=lambda x: x.name.lower()):
                    if item.name.startswith('.'):
                        continue
                    if item.is_dir():
                        add(item.name, str(item), "volume")
            except (PermissionError, OSError):
                continue

        add("Home", str(Path.home()), "home")

        self.send_json({"volumes": entries, "root": self.root_directory})

    def start_scan_job(self, payload: dict):
        """POST /api/scan - equivalent of `photo-import scan` / `video-scan`."""
        media = payload.get('media', PHOTO)
        workers = payload.get('workers')
        job = self.job_manager.start_scan(
            media,
            source=str(payload.get('source', '')).strip(),
            target=str(payload.get('target', '')).strip(),
            checksum=bool(payload.get('checksum', media == PHOTO)),
            resume=bool(payload.get('resume', True)),
            workers=int(workers) if workers else None,
        )
        self.send_json(job.to_dict(), 202)

    def start_copy_job(self, payload: dict):
        """POST /api/copy - equivalent of `photo-import copy` / `video-copy`."""
        batch_id = payload.get('batch_id')
        job = self.job_manager.start_copy(
            payload.get('media', PHOTO),
            batch_id=int(batch_id) if batch_id else None,
            dry_run=bool(payload.get('dry_run', False)),
            skip_no_date=bool(payload.get('skip_no_date', False)),
            use_file_date=bool(payload.get('use_file_date', True)),
        )
        self.send_json(job.to_dict(), 202)

    def start_retry_job(self, payload: dict):
        """POST /api/retry - equivalent of `photo-import retry` / `video-retry`."""
        batch_id = payload.get('batch_id')
        if not batch_id:
            raise ValueError("batch_id is required")

        job = self.job_manager.start_retry(payload.get('media', PHOTO), int(batch_id))
        self.send_json(job.to_dict(), 202)

    def start_expand_job(self, payload: dict):
        """POST /api/expand - equivalent of `photo-import expand`."""
        target = str(payload.get('target', '')).strip()
        job = self.job_manager.start_expand(
            source=str(payload.get('source', '')).strip(),
            target=target or None,
            dry_run=bool(payload.get('dry_run', False)),
            move_files=bool(payload.get('move', False)),
        )
        self.send_json(job.to_dict(), 202)

    def cancel_job(self, payload: dict):
        """POST /api/job/cancel - stop a running job (progress is saved)."""
        job_id = payload.get('id')
        if not job_id:
            raise ValueError("id is required")

        if not self.job_manager.cancel(int(job_id)):
            self.send_json({"error": "Job is not running"}, 409)
            return

        self.send_json({"cancelled": int(job_id)})

    def send_tree(self):
        """Send directory tree as JSON - only immediate children (lazy load)."""
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        relative_path = query.get('path', ['.'])[0]

        root = Path(self.root_directory).resolve()
        target = root / relative_path if relative_path != '.' else root

        if not target.is_dir():
            self.send_json({"error": "Directory not found"}, 404)
            return

        # Only get immediate subdirectories (no recursion)
        children = []
        try:
            for item in sorted(target.iterdir(), key=lambda x: x.name.lower()):
                if item.name.startswith('.'):
                    continue
                if item.is_dir():
                    # Check if this directory has subdirectories (for expand arrow)
                    has_children = any(
                        sub.is_dir() and not sub.name.startswith('.')
                        for sub in item.iterdir()
                    ) if item.is_dir() else False

                    children.append({
                        "name": item.name,
                        "path": str(item.relative_to(root)),
                        "has_children": has_children,
                    })
        except PermissionError:
            pass

        self.send_json({
            "path": relative_path,
            "name": target.name or "Root",
            "children": children,
        })

    def send_file_list(self, relative_path: str):
        """Send file list for a directory with pagination."""
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        page = int(query.get('page', ['1'])[0])
        per_page = int(query.get('per_page', ['50'])[0])  # Default 50 items per page
        per_page = min(per_page, 200)  # Max 200 per page
        sort_by = query.get('sort', ['name'])[0]  # name, size, created, modified, accessed
        sort_order = query.get('order', ['asc'])[0]  # asc, desc

        root = Path(self.root_directory).resolve()
        target = root / relative_path if relative_path != '.' else root

        if not target.is_dir():
            self.send_json({"error": "Directory not found"}, 404)
            return

        # Separate directories, images, and other files
        dirs = []
        images = []
        other_files = []
        favorite_paths = self.favorites.all_paths() if self.favorites else set()
        try:
            for item in target.iterdir():
                if item.name.startswith('.'):
                    continue

                try:
                    stat = item.stat()
                except (PermissionError, OSError):
                    continue

                relative_path = str(item.relative_to(root))
                info = {
                    "name": item.name,
                    "path": relative_path,
                    "is_dir": item.is_dir(),
                    "modified": stat.st_mtime,
                    "accessed": stat.st_atime,
                    "created": getattr(stat, 'st_birthtime', stat.st_ctime),
                }

                if item.is_file():
                    ext = item.suffix.lower()
                    info["extension"] = ext
                    info["size"] = stat.st_size
                    info["is_image"] = ext in IMAGE_EXTENSIONS
                    info["is_video"] = ext in VIDEO_EXTENSIONS
                    info["is_raw"] = ext in RAW_EXTENSIONS
                    info["favorite"] = relative_path in favorite_paths
                    if info["is_image"] or info["is_video"] or info["is_raw"]:
                        images.append(info)
                    else:
                        other_files.append(info)
                else:
                    info["size"] = 0
                    dirs.append(info)
        except PermissionError:
            self.send_json({"error": "Permission denied"}, 403)
            return

        # Sort function
        def sort_key(item):
            if sort_by == 'name':
                return item['name'].lower()
            elif sort_by == 'size':
                return item.get('size', 0)
            elif sort_by == 'created':
                return item.get('created', 0)
            elif sort_by == 'modified':
                return item.get('modified', 0)
            elif sort_by == 'accessed':
                return item.get('accessed', 0)
            return item['name'].lower()

        reverse = sort_order == 'desc'

        # Sort each category
        dirs.sort(key=sort_key, reverse=reverse)
        images.sort(key=sort_key, reverse=reverse)
        other_files.sort(key=sort_key, reverse=reverse)

        # Combine files: images first, then other files
        files = images + other_files

        # Always show all directories, paginate only files
        total_files = len(files)
        total_pages = max(1, (total_files + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))

        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_files = files[start_idx:end_idx]

        # Combine: all dirs first, then paginated files
        items = dirs + paginated_files

        self.send_json({
            "path": relative_path,
            "items": items,
            "parent": str(Path(relative_path).parent) if relative_path != '.' else None,
            "sort": sort_by,
            "order": sort_order,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_files": total_files,
                "total_dirs": len(dirs),
                "total_pages": total_pages,
            }
        })

    def send_recursive_media(self, query: dict):
        """Every photo/video below a folder, flattened - a year or month at a glance."""
        relative_path = query.get('path', ['.'])[0] or '.'
        refresh = query.get('refresh', ['0'])[0] == '1'
        sort_by = query.get('sort', ['name'])[0]
        sort_order = query.get('order', ['asc'])[0]

        root = Path(self.root_directory).resolve()
        target = root if relative_path == '.' else root / relative_path

        if not target.is_dir():
            self.send_json({"error": "Directory not found"}, 404)
            return

        cache_key = str(target)
        entries = None if refresh else RECURSIVE_CACHE.get(cache_key)
        if entries is None:
            entries = collect_media_recursive(target, root)
            # Keep only a few folders cached - these lists can be large
            while len(RECURSIVE_CACHE) >= 4:
                RECURSIVE_CACHE.pop(next(iter(RECURSIVE_CACHE)))
            RECURSIVE_CACHE[cache_key] = entries

        def sort_key(item):
            if sort_by == 'size':
                return item['size']
            if sort_by == 'created':
                return item['created']
            if sort_by == 'modified':
                return item['modified']
            if sort_by == 'accessed':
                return item['accessed']
            return item['path'].lower()

        items = sorted(entries, key=sort_key, reverse=sort_order == 'desc')

        page = max(1, int(query.get('page', ['1'])[0] or 1))
        per_page = min(int(query.get('per_page', ['50'])[0] or 50), 200)
        total = len(items)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = min(page, total_pages)

        window = items[(page - 1) * per_page: page * per_page]
        favorite_paths = self.favorites.all_paths() if self.favorites else set()
        for item in window:
            item["favorite"] = item["path"] in favorite_paths

        self.send_json({
            "path": relative_path,
            "items": window,
            "sort": sort_by,
            "order": sort_order,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_files": total,
                "total_dirs": 0,
                "total_pages": total_pages,
            },
        })

    def send_batch_index(self, query: dict):
        """Every import that put files in the library, newest first.

        Photos and videos in one list so the Browse picker can offer them by
        date - a batch is identified by its media type plus its id.
        """
        limit = min(int(query.get('limit', ['50'])[0] or 50), 200)

        if self.job_manager is None:
            self.send_json({"batches": [], "import_enabled": False})
            return

        root = Path(self.root_directory).resolve()
        batches = []

        for media in (PHOTO, VIDEO):
            db = self.job_manager.database_for(media)
            for batch in db.list_batches(limit=limit):
                stats = db.get_batch_stats(batch.id)
                copied = stats.get('copied') or 0
                if not copied:
                    continue  # nothing was imported, so there is nothing to browse

                imported_at = (
                    batch.completed_at or batch.copy_started_at or batch.started_at
                )
                # The files sit under the target, but a server started on
                # .../organized_photos is deeper than the target it was given -
                # either way there is something to show.
                target = Path(batch.target_directory)
                in_root = (
                    target == root or root in target.parents or target in root.parents
                )
                batches.append({
                    "id": batch.id,
                    "media": media,
                    "status": batch.status.value,
                    "copied": copied,
                    "source_directory": batch.source_directory,
                    "target_directory": batch.target_directory,
                    "imported_at": imported_at.isoformat(timespec='seconds') if imported_at else None,
                    "in_root": in_root,
                })

        batches.sort(key=lambda item: item["imported_at"] or "", reverse=True)
        self.send_json({"batches": batches, "import_enabled": True})

    def send_batch_media(self, query: dict):
        """The files one import put in the library, flattened into one grid."""
        media = query.get('media', [PHOTO])[0]
        raw_id = query.get('id', [None])[0]
        refresh = query.get('refresh', ['0'])[0] == '1'
        sort_by = query.get('sort', ['name'])[0]
        sort_order = query.get('order', ['asc'])[0]

        if self.job_manager is None:
            self.send_json({"error": "Import operations are disabled"}, 503)
            return

        if raw_id is None:
            self.send_json({"error": "Missing batch id"}, 400)
            return

        try:
            db = self.job_manager.database_for(media)
            batch_id = int(raw_id)
        except ValueError as exc:
            self.send_json({"error": f"Invalid batch: {exc}"}, 400)
            return

        batch = db.get_batch(batch_id)
        if not batch:
            self.send_json({"error": f"Batch {batch_id} not found"}, 404)
            return

        root = Path(self.root_directory).resolve()
        cache_key = f"{media}:{batch_id}"
        collected = None if refresh else BATCH_CACHE.get(cache_key)
        if collected is None:
            collected = collect_batch_media(db.get_imported_files(batch_id), root)
            while len(BATCH_CACHE) >= 4:
                BATCH_CACHE.pop(next(iter(BATCH_CACHE)))
            BATCH_CACHE[cache_key] = collected

        entries = collected["entries"]

        def sort_key(item):
            if sort_by == 'size':
                return item['size']
            if sort_by == 'created':
                return item['created']
            if sort_by == 'modified':
                return item['modified']
            if sort_by == 'accessed':
                return item['accessed']
            return item['path'].lower()

        items = sorted(entries, key=sort_key, reverse=sort_order == 'desc')

        page = max(1, int(query.get('page', ['1'])[0] or 1))
        per_page = min(int(query.get('per_page', ['50'])[0] or 50), 200)
        total = len(items)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = min(page, total_pages)

        window = items[(page - 1) * per_page: page * per_page]
        favorite_paths = self.favorites.all_paths() if self.favorites else set()
        for item in window:
            item["favorite"] = item["path"] in favorite_paths

        imported_at = batch.completed_at or batch.copy_started_at or batch.started_at
        self.send_json({
            "batch": {
                "id": batch.id,
                "media": media,
                "status": batch.status.value,
                "source_directory": batch.source_directory,
                "target_directory": batch.target_directory,
                "imported_at": imported_at.isoformat(timespec='seconds') if imported_at else None,
                "shown": total,
                "outside": collected["outside"],
                "missing": collected["missing"],
            },
            "items": window,
            "sort": sort_by,
            "order": sort_order,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_files": total,
                "total_dirs": 0,
                "total_pages": total_pages,
            },
        })

    def send_image_list(self, relative_path: str):
        """Send list of images in a directory."""
        images = list_images_in_directory(self.root_directory, relative_path)
        self.send_json({
            "path": relative_path,
            "images": images,
        })

    def send_thumbnail(self, image_path: str):
        """Send thumbnail of an image."""
        root = Path(self.root_directory).resolve()
        full_path = root / image_path

        if not full_path.is_file():
            self.send_error(404, "Image not found")
            return

        if not HAS_PIL:
            # Fall back to sending the original
            self.send_photo(image_path)
            return

        try:
            content = render_thumbnail(full_path, self.thumbnail_size)
        except Exception as e:
            self.send_error(500, f"Error generating thumbnail: {e}")
            return

        self.send_response(200)
        self.send_header('Content-Type', 'image/jpeg')
        self.send_header('Content-Length', len(content))
        self.send_header('Cache-Control', 'max-age=3600')
        self.end_headers()
        self.wfile.write(content)

    def send_preview(self, query: dict):
        """Send a thumbnail of any absolute path (used by the conflict preview)."""
        if self.job_manager is None:
            self.send_error(403, "Import operations are disabled")
            return

        raw_path = query.get('path', [''])[0]
        if not raw_path:
            self.send_error(400, "path is required")
            return

        full_path = Path(raw_path).expanduser()
        if not full_path.is_file():
            self.send_error(404, "File not found")
            return

        if not HAS_PIL:
            self.send_error(501, "Pillow is not installed")
            return

        try:
            size = int(query.get('size', ['320'])[0])
        except ValueError:
            size = 320
        size = max(80, min(size, 1200))

        try:
            content = render_thumbnail(full_path, (size, size))
        except Exception as e:
            # HEIC without pillow-heif, RAW, video, corrupt file...
            self.send_error(415, f"No preview available: {e}")
            return

        self.send_response(200)
        self.send_header('Content-Type', 'image/jpeg')
        self.send_header('Content-Length', len(content))
        self.send_header('Cache-Control', 'max-age=300')
        self.end_headers()
        self.wfile.write(content)

    def send_photo(self, image_path: str):
        """Send full photo."""
        root = Path(self.root_directory).resolve()
        full_path = root / image_path

        if not full_path.is_file():
            self.send_error(404, "Image not found")
            return

        # Security check - ensure path is within root
        try:
            full_path.resolve().relative_to(root)
        except ValueError:
            self.send_error(403, "Access denied")
            return

        content_type, _ = mimetypes.guess_type(str(full_path))
        if not content_type:
            content_type = 'application/octet-stream'

        try:
            with open(full_path, 'rb') as f:
                content = f.read()

            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', len(content))
            self.send_header('Cache-Control', 'max-age=3600')
            self.end_headers()
            self.wfile.write(content)

        except BrokenPipeError:
            # Client disconnected before we finished sending - ignore
            pass
        except ConnectionResetError:
            # Client reset connection - ignore
            pass
        except Exception as e:
            try:
                self.send_error(500, f"Error reading file: {e}")
            except (BrokenPipeError, ConnectionResetError):
                pass

    def send_index(self):
        """Send the main HTML page."""
        html = get_index_html()
        content = html.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', len(content))
        self.end_headers()
        self.wfile.write(content)

    def send_styles(self):
        """Send CSS styles."""
        css = get_styles_css()
        content = css.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/css; charset=utf-8')
        self.send_header('Content-Length', len(content))
        self.end_headers()
        self.wfile.write(content)

    def send_javascript(self):
        """Send JavaScript."""
        js = get_app_js()
        content = js.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/javascript; charset=utf-8')
        self.send_header('Content-Length', len(content))
        self.end_headers()
        self.wfile.write(content)

    def send_favicon(self):
        """Send a simple favicon (empty 1x1 transparent PNG)."""
        # Minimal 1x1 transparent PNG
        favicon = bytes([
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D,
            0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
            0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00,
            0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
            0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49,
            0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82
        ])
        self.send_response(200)
        self.send_header('Content-Type', 'image/png')
        self.send_header('Content-Length', len(favicon))
        self.send_header('Cache-Control', 'max-age=86400')
        self.end_headers()
        self.wfile.write(favicon)

    def log_message(self, format, *args):
        """Override to reduce log noise."""
        # Only log non-API/photo requests, and handle error cases
        if args and isinstance(args[0], str):
            if '/api/' not in args[0] and '/photo/' not in args[0] and '/favicon' not in args[0]:
                super().log_message(format, *args)


IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.heic', '.heif', '.bmp', '.tiff', '.tif'}
VIDEO_EXTENSIONS = {'.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v', '.wmv', '.flv', '.3gp', '.mts', '.m2ts'}
# Photos too, but Pillow cannot render them - listed and counted, never thumbnailed
RAW_EXTENSIONS = {'.arw', '.orf', '.dng', '.cr2', '.cr3', '.nef', '.rw2', '.pef', '.srw', '.raf', '.raw'}

# Above this, comparing two files byte by byte costs more than it helps
MAX_COMPARE_BYTES = 512 * 1024 * 1024


# Walking a big library is slow, so results are kept until explicitly refreshed
STATS_CACHE: dict = {}
RECURSIVE_CACHE: dict = {}
BATCH_CACHE: dict = {}

# A guard against pointing the flat view at something enormous
MAX_RECURSIVE_ITEMS = 200_000


def collect_media_recursive(directory: Path, root: Path) -> list:
    """Every photo/video under a directory, as flat listing entries."""
    entries = []
    stack = [directory]

    while stack and len(entries) < MAX_RECURSIVE_ITEMS:
        current = stack.pop()
        try:
            children = list(os.scandir(current))
        except (PermissionError, OSError):
            continue

        for child in children:
            if child.name.startswith('.'):
                continue
            try:
                if child.is_dir(follow_symlinks=False):
                    stack.append(Path(child.path))
                    continue
                if not child.is_file(follow_symlinks=False):
                    continue

                extension = Path(child.name).suffix.lower()
                is_image = extension in IMAGE_EXTENSIONS
                is_video = extension in VIDEO_EXTENSIONS
                is_raw = extension in RAW_EXTENSIONS
                if not (is_image or is_video or is_raw):
                    continue

                stat = child.stat(follow_symlinks=False)
                entries.append({
                    "name": child.name,
                    "path": str(Path(child.path).relative_to(root)),
                    "folder": str(Path(child.path).parent.relative_to(root)),
                    "is_dir": False,
                    "extension": extension,
                    "size": stat.st_size,
                    "is_image": is_image,
                    "is_video": is_video,
                    "is_raw": is_raw,
                    "modified": stat.st_mtime,
                    "accessed": stat.st_atime,
                    "created": getattr(stat, 'st_birthtime', stat.st_ctime),
                })
            except OSError:
                continue

    return entries


def collect_batch_media(records: list, root: Path) -> dict:
    """Turn a batch's copied files into listing entries, as the grid wants them.

    A batch spreads its files over many date folders, so the result is flat -
    the same shape the recursive view uses. Files that landed outside the
    served folder cannot be shown here; files deleted since the import are
    kept, flagged as missing.
    """
    entries = []
    outside = 0
    missing = 0

    for record in records:
        target = record.get("target_path")
        if not target:
            continue

        path = Path(target)
        try:
            relative = path.relative_to(root)
        except ValueError:
            try:
                relative = path.resolve().relative_to(root)
            except (ValueError, OSError):
                outside += 1
                continue

        extension = path.suffix.lower()
        entry = {
            "name": path.name,
            "path": str(relative),
            "folder": str(relative.parent),
            "is_dir": False,
            "extension": extension,
            "size": record.get("file_size") or 0,
            "is_image": extension in IMAGE_EXTENSIONS,
            "is_video": extension in VIDEO_EXTENSIONS,
            "is_raw": extension in RAW_EXTENSIONS,
        }

        try:
            stat = (root / relative).stat()
        except OSError:
            missing += 1
            entry["missing"] = True
            entry["modified"] = 0
            entry["accessed"] = 0
            entry["created"] = 0
        else:
            entry["size"] = stat.st_size
            entry["modified"] = stat.st_mtime
            entry["accessed"] = stat.st_atime
            entry["created"] = getattr(stat, 'st_birthtime', stat.st_ctime)

        entries.append(entry)

    return {"entries": entries, "outside": outside, "missing": missing}


def scan_tree(directory: Path) -> dict:
    """Total size and file counts under a directory, by kind."""
    totals = {"files": 0, "images": 0, "videos": 0, "raw": 0, "other": 0, "bytes": 0}
    stack = [directory]

    while stack:
        current = stack.pop()
        try:
            entries = list(os.scandir(current))
        except (PermissionError, OSError):
            continue

        for entry in entries:
            if entry.name.startswith('.'):
                continue
            try:
                if entry.is_dir(follow_symlinks=False):
                    stack.append(Path(entry.path))
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue

                totals["files"] += 1
                totals["bytes"] += entry.stat(follow_symlinks=False).st_size
                extension = Path(entry.name).suffix.lower()
                if extension in IMAGE_EXTENSIONS:
                    totals["images"] += 1
                elif extension in VIDEO_EXTENSIONS:
                    totals["videos"] += 1
                elif extension in RAW_EXTENSIONS:
                    totals["raw"] += 1
                else:
                    totals["other"] += 1
            except OSError:
                continue

    return totals


def collect_directory_stats(target: Path, root: Path) -> dict:
    """Per-subfolder totals plus the files sitting directly in this folder."""
    rows = []
    loose = {"files": 0, "images": 0, "videos": 0, "raw": 0, "other": 0, "bytes": 0}

    try:
        entries = sorted(os.scandir(target), key=lambda e: e.name.lower())
    except (PermissionError, OSError) as exc:
        return {"error": str(exc), "rows": [], "totals": loose}

    for entry in entries:
        if entry.name.startswith('.'):
            continue
        try:
            if entry.is_dir(follow_symlinks=False):
                totals = scan_tree(Path(entry.path))
                totals["name"] = entry.name
                totals["path"] = str(Path(entry.path).relative_to(root))
                rows.append(totals)
            elif entry.is_file(follow_symlinks=False):
                loose["files"] += 1
                loose["bytes"] += entry.stat(follow_symlinks=False).st_size
                extension = Path(entry.name).suffix.lower()
                if extension in IMAGE_EXTENSIONS:
                    loose["images"] += 1
                elif extension in VIDEO_EXTENSIONS:
                    loose["videos"] += 1
                elif extension in RAW_EXTENSIONS:
                    loose["raw"] += 1
                else:
                    loose["other"] += 1
        except OSError:
            continue

    totals = {
        key: sum(row[key] for row in rows) + loose[key]
        for key in ("files", "images", "videos", "raw", "other", "bytes")
    }

    relative = '.' if target == root else str(target.relative_to(root))
    return {
        "path": relative,
        "parent": None if relative == '.' else (str(Path(relative).parent) if str(Path(relative).parent) != '.' else '.'),
        "rows": rows,
        "loose": loose,
        "totals": totals,
        "generated_at": datetime.now().isoformat(timespec='seconds'),
    }


def describe_conflict(record, media: str) -> dict:
    """Describe both sides of a conflict so the UI can show them next to each other."""
    incoming_path = Path(record.source_path)
    existing_path = Path(record.target_path) if record.target_path else None

    def describe(path: Optional[Path], taken_at) -> dict:
        if path is None:
            return {"path": None, "missing": True}
        try:
            stat = path.stat()
        except OSError:
            return {"path": str(path), "missing": True}
        return {
            "path": str(path),
            "name": path.name,
            "size": stat.st_size,
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec='seconds'),
            "taken_at": taken_at.isoformat(timespec='seconds') if taken_at else None,
            "is_image": path.suffix.lower() in IMAGE_EXTENSIONS,
            "missing": False,
        }

    taken_at = getattr(record, 'metadata_date', None) if media == VIDEO else getattr(record, 'exif_date', None)
    incoming = describe(incoming_path, taken_at or record.file_creation_date)
    existing = describe(existing_path, None)

    identical = None
    if not incoming["missing"] and not existing["missing"]:
        if incoming["size"] != existing["size"]:
            identical = False
        elif incoming["size"] <= MAX_COMPARE_BYTES:
            from .scanner import calculate_checksum
            try:
                identical = calculate_checksum(incoming_path) == calculate_checksum(existing_path)
            except OSError:
                identical = None

    return {
        "id": record.id,
        "filename": record.filename,
        "incoming": incoming,
        "existing": existing,
        "identical": identical,
        "reason": record.error_message,
    }


def render_thumbnail(full_path: Path, size: tuple) -> bytes:
    """Render a JPEG thumbnail, honouring the EXIF orientation."""
    with Image.open(full_path) as img:
        try:
            from PIL import ExifTags
            for orientation in ExifTags.TAGS.keys():
                if ExifTags.TAGS[orientation] == 'Orientation':
                    break
            exif = img._getexif()
            if exif:
                orientation_value = exif.get(orientation)
                if orientation_value == 3:
                    img = img.rotate(180, expand=True)
                elif orientation_value == 6:
                    img = img.rotate(270, expand=True)
                elif orientation_value == 8:
                    img = img.rotate(90, expand=True)
        except (AttributeError, KeyError, TypeError):
            pass

        if img.mode in ('RGBA', 'P', 'LA'):
            img = img.convert('RGB')

        img.thumbnail(size, Image.Resampling.LANCZOS)

        buffer = BytesIO()
        img.save(buffer, format='JPEG', quality=85)
        return buffer.getvalue()


def batch_to_dict(batch, stats: dict) -> dict:
    """Serialize a photo/video batch plus its statistics for the UI."""
    def iso(value):
        return value.isoformat(timespec='seconds') if value else None

    return {
        "id": batch.id,
        "source_directory": batch.source_directory,
        "target_directory": batch.target_directory,
        "status": batch.status.value,
        "total_files": batch.total_files,
        "scanned_files": batch.scanned_files,
        "copied_files": batch.copied_files,
        "failed_files": batch.failed_files,
        "skipped_files": batch.skipped_files,
        "started_at": iso(batch.started_at),
        "scan_completed_at": iso(batch.scan_completed_at),
        "copy_started_at": iso(batch.copy_started_at),
        "completed_at": iso(batch.completed_at),
        "stats": {key: (value or 0) for key, value in stats.items()},
    }


class QuietHTTPServer(ThreadingHTTPServer):
    """Threaded HTTP server that silently handles client disconnection errors."""

    daemon_threads = True

    def handle_error(self, request, client_address):
        """Handle errors - suppress broken pipe and connection reset."""
        import sys
        exc_type, exc_value, _ = sys.exc_info()
        if exc_type in (BrokenPipeError, ConnectionResetError):
            # Client disconnected - silently ignore
            pass
        else:
            # Log other errors normally
            super().handle_error(request, client_address)


def run_server(
    directory: str,
    port: int = 8080,
    host: str = "127.0.0.1",
    open_browser: bool = True,
    db_path: str = "photo_import.db",
    video_db_path: str = "video_import.db",
    favorites_db_path: str = "photo_favorites.db",
    enable_import: bool = True,
):
    """
    Run the photo browser web server.

    Args:
        directory: Root directory to serve
        port: Port number
        host: Host to bind to
        open_browser: Whether to open browser automatically
        db_path: Photo database used by the import tools
        video_db_path: Video database used by the import tools
        enable_import: Expose the scan/copy/retry/expand operations in the UI
    """
    # Set class variables for the handler
    PhotoBrowserHandler.root_directory = str(Path(directory).resolve())
    PhotoBrowserHandler.job_manager = (
        JobManager(db_path, video_db_path) if enable_import else None
    )
    PhotoBrowserHandler.favorites = FavoritesStore(favorites_db_path)

    server = QuietHTTPServer((host, port), PhotoBrowserHandler)
    url = f"http://{host}:{port}"

    print(f"\n{'='*50}")
    print(f"Photo Browser Server")
    print(f"{'='*50}")
    print(f"Serving: {PhotoBrowserHandler.root_directory}")
    print(f"URL: {url}")
    print(f"Favorites: {Path(favorites_db_path).resolve()}")
    if enable_import:
        print(f"Photo DB: {Path(db_path).resolve()}")
        print(f"Video DB: {Path(video_db_path).resolve()}")
        print("Import tools: enabled (scan / copy / retry / expand)")
        if host not in ("127.0.0.1", "localhost", "::1"):
            print("\n⚠️  Import tools can read and write anywhere on this machine.")
            print("   Binding to a non-local host exposes them - use --no-import if unsure.")
    else:
        print("Import tools: disabled (browse only)")
    print(f"{'='*50}")
    print("\nPress Ctrl+C to stop\n")

    if open_browser:
        import webbrowser
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        server.shutdown()


def get_index_html() -> str:
    """Return the main HTML page."""
    return '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Photo Browser</title>
    <script>
        // Before the stylesheet paints: no dark flash on a light library
        try {
            var saved = localStorage.getItem('photoBrowserTheme');
            if (saved) document.documentElement.setAttribute('data-theme', saved);
        } catch (e) { /* storage blocked - the dark default stands */ }
    </script>
    <link rel="stylesheet" href="/styles.css">
</head>
<body>
    <div class="app">
        <header class="header">
            <div class="header-row">
                <h1>Photo Browser</h1>
                <nav class="tabs" id="tabs">
                    <button class="tab active" data-view="browse">Browse</button>
                    <button class="tab" data-view="favorites">&#9733; Favorites <span class="tab-count" id="fav-count"></span></button>
                    <button class="tab" data-view="storage">Storage</button>
                    <button class="tab import-only" data-view="import">Import</button>
                    <button class="tab import-only" data-view="batches">Batches</button>
                    <button class="tab import-only" data-view="tools">Tools</button>
                </nav>

                <div class="header-tools">
                    <!-- The running job, folded into one pill; click opens the detail -->
                    <button class="job-pill" id="job-pill" hidden title="Running job - click for details">
                        <span class="job-dot"></span>
                        <span class="job-pill-title" id="job-pill-title"></span>
                        <span class="job-pill-track"><span class="job-pill-fill" id="job-pill-fill"></span></span>
                        <span class="job-pill-meta" id="job-pill-meta"></span>
                    </button>
                    <button class="icon-btn" id="btn-theme" title="Switch light / dark"><span class="theme-mark"></span></button>
                    <button class="btn" id="btn-help" title="Show shortcuts (?)" data-shortcut="?">?</button>

                    <!-- Running / last job -->
                    <div class="job-bar" id="job-bar" hidden>
                        <div class="job-bar-head">
                            <span class="job-title" id="job-title"></span>
                            <span class="job-badge" id="job-badge"></span>
                            <div class="job-actions">
                                <button class="btn danger" id="job-cancel">Stop</button>
                                <button class="btn" id="job-dismiss">Dismiss</button>
                            </div>
                        </div>
                        <div class="progress"><div class="progress-fill" id="job-progress"></div></div>
                        <div class="job-meta">
                            <span id="job-file"></span>
                            <span id="job-counts"></span>
                        </div>
                        <div class="job-result" id="job-result"></div>
                    </div>
                </div>
            </div>
            <div class="header-progress" id="header-progress" hidden></div>
        </header>

        <div class="main view active" id="view-browse">
            <nav class="sidebar" id="sidebar">
                <div class="tree" id="tree"></div>
            </nav>

            <main class="content">
                <!-- Where you are: tree toggle, crumbs, and what is filtering the grid -->
                <div class="path-bar">
                    <button class="btn tiny" id="btn-tree" title="Toggle folder tree (T)" data-shortcut="T">&#9776;</button>
                    <div class="breadcrumb" id="breadcrumb"></div>
                    <div class="chip" id="batch-bar" hidden></div>
                    <div class="chip" id="recursive-chip" hidden>
                        <b>All photos below this folder</b>
                        <button class="chip-close" data-exit-recursive="1" title="Back to the folder listing (V)">&times;</button>
                    </div>
                    <span class="file-count" id="file-count"></span>
                </div>

                <div class="toolbar">
                    <span class="filter-wrapper" data-shortcut="F">
                        <input type="text" class="filter-input" id="filter-input" placeholder="Filter" title="Filter files (F)">
                    </span>
                    <button class="btn" id="btn-clear-filter" title="Clear filter (Esc)" style="display:none;">&times;</button>
                    <button class="btn" id="btn-grid" title="Grid view (G)" data-shortcut="G">Grid</button>
                    <button class="btn" id="btn-list" title="List view (L)" data-shortcut="L">List</button>
                    <select class="btn" id="sort-select" title="Sort by (N M C A Z)">
                        <option value="name" selected>Name</option>
                        <option value="modified">Modified</option>
                        <option value="created">Created</option>
                        <option value="accessed">Accessed</option>
                        <option value="size">Size</option>
                    </select>
                    <button class="btn" id="btn-sort-order" title="Sort order (O)" data-shortcut="O">↑</button>
                    <button class="btn" id="btn-recursive" title="Show every photo below this folder (V)" data-shortcut="V">All photos</button>
                    <span class="shortcut-wrap import-only" data-shortcut="B">
                        <select class="btn" id="batch-select" title="Show only the files of one import (B)">
                            <option value="">Imports</option>
                        </select>
                    </span>
                    <button class="btn import-only" id="btn-import-here" title="Scan this folder as an import source">Import this folder</button>
                    <button class="btn" id="btn-size" title="Size of the selected folder (S)" data-shortcut="S">Size</button>
                </div>

                <div class="file-grid" id="file-grid"></div>

                <div class="status-bar">
                    <span class="status-left" id="status-left"></span>
                    <span class="status-right">
                        <div class="pagination" id="pagination"></div>
                        <select id="per-page-select" title="Items per page (1-4)">
                            <option value="25">25 / page</option>
                            <option value="50" selected>50 / page</option>
                            <option value="100">100 / page</option>
                            <option value="200">200 / page</option>
                        </select>
                    </span>
                </div>
            </main>
        </div>

        <!-- Favorites view -->
        <div class="view" id="view-favorites">
            <main class="content">
                <div class="toolbar">
                    <span class="toolbar-title">Favorites</span>
                    <button class="btn" id="btn-refresh-favorites">Refresh</button>
                    <button class="btn" id="btn-prune-favorites" title="Forget favorites whose file is gone">Clean up missing</button>
                    <span class="file-count" id="favorites-count"></span>
                </div>
                <div class="file-grid" id="favorites-grid"></div>
            </main>
        </div>

        <!-- Storage view: size and counts per folder (per year in the library) -->
        <div class="view panel-view" id="view-storage">
            <div class="card">
                <div class="card-head">
                    <h2>Storage</h2>
                    <button class="btn" id="btn-storage-up">Up</button>
                    <button class="btn" id="btn-storage-refresh" title="Rescan the folder">Rescan</button>
                    <button class="btn" id="btn-storage-browse" title="Open this folder in Browse">Open in Browse</button>
                </div>
                <div class="storage-path" id="storage-path"></div>
                <div class="storage-summary" id="storage-summary"></div>
                <div id="storage-table"></div>
            </div>
        </div>

        <!-- Conflicts view: decide per file what happens -->
        <div class="view panel-view" id="view-conflicts">
            <div class="card">
                <div class="card-head">
                    <h2>Conflicts <span id="conflicts-subtitle" class="card-hint" style="margin:0"></span></h2>
                    <button class="btn" id="btn-back-batches">Back to batches</button>
                </div>
                <p class="card-hint">
                    These files already exist in the library. Nothing is overwritten until you choose.
                </p>
                <div class="bulk-actions">
                    <span>Apply to all remaining:</span>
                    <button class="btn" data-bulk="skip">Keep existing</button>
                    <button class="btn" data-bulk="keep_both">Keep both</button>
                    <button class="btn danger" data-bulk="overwrite">Replace with imported</button>
                </div>
                <div id="conflict-list" class="conflict-list"></div>
                <div class="pagination" id="conflict-pagination"></div>
            </div>
        </div>

        <!-- Import view: scan (photo-import scan / video-scan) -->
        <div class="view panel-view" id="view-import">
            <div class="card">
                <h2>Scan a source folder</h2>
                <p class="card-hint">Catalogs files and their dates into the database. Nothing is copied yet.</p>

                <div class="seg" id="scan-media">
                    <button class="seg-btn active" data-media="photo">Photos</button>
                    <button class="seg-btn" data-media="video">Videos</button>
                </div>

                <div class="field">
                    <label for="scan-source">Source folder</label>
                    <div class="field-row">
                        <input type="text" id="scan-source" placeholder="/Volumes/card/DCIM">
                        <button class="btn" data-pick="scan-source">Browse</button>
                        <button class="btn" data-fill-current="scan-source" title="Use the folder open in Browse">Current</button>
                    </div>
                    <div class="volumes">
                        <div class="volumes-head">
                            <span>Mounted volumes</span>
                            <button class="btn tiny" id="btn-refresh-volumes" title="Rescan mounted volumes">&#8635;</button>
                        </div>
                        <div class="volume-list" id="volume-list"></div>
                    </div>
                </div>

                <div class="field">
                    <label for="scan-target">Target folder</label>
                    <div class="field-row">
                        <input type="text" id="scan-target" placeholder="/Volumes/poze">
                        <button class="btn" data-pick="scan-target">Browse</button>
                        <button class="btn" data-fill-current="scan-target" title="Use the folder open in Browse">Current</button>
                    </div>
                    <div class="dest-hint" id="scan-dest"></div>
                </div>

                <div class="options">
                    <label class="check"><input type="checkbox" id="scan-checksum" checked> Calculate MD5 checksums</label>
                    <label class="check"><input type="checkbox" id="scan-resume" checked> Resume existing scan</label>
                    <label class="check inline-num">Workers <input type="number" id="scan-workers" min="1" max="128" placeholder="auto"></label>
                </div>

                <div class="actions">
                    <button class="btn primary" id="btn-scan">Start scan</button>
                    <span class="form-error" id="scan-error"></span>
                </div>
            </div>

            <div class="card">
                <h2>Copy the latest batch</h2>
                <p class="card-hint">Copies pending files of the most recent batch into its target, organized by date.</p>

                <div class="seg" id="quick-copy-media">
                    <button class="seg-btn active" data-media="photo">Photos</button>
                    <button class="seg-btn" data-media="video">Videos</button>
                </div>

                <div class="latest-batch" id="latest-batch"></div>

                <div class="options">
                    <label class="check"><input type="checkbox" id="copy-dry-run"> Dry run</label>
                    <label class="check"><input type="checkbox" id="copy-skip-no-date"> Skip files without EXIF/metadata date</label>
                    <label class="check"><input type="checkbox" id="copy-use-file-date" checked> Fall back to file date</label>
                </div>

                <div class="actions">
                    <button class="btn primary" id="btn-quick-copy">Copy latest batch</button>
                    <button class="btn" id="btn-all-batches">All batches &rarr;</button>
                    <span class="form-error" id="copy-error"></span>
                </div>
            </div>
        </div>

        <!-- Batches view: status / copy / retry -->
        <div class="view panel-view" id="view-batches">
            <div class="card">
                <div class="card-head">
                    <h2>Batches</h2>
                    <div class="seg" id="batches-media">
                        <button class="seg-btn active" data-media="photo">Photos</button>
                        <button class="seg-btn" data-media="video">Videos</button>
                    </div>
                    <span class="batch-legend">
                        <span><i class="mix-copied"></i>copied</span>
                        <span><i class="mix-skipped"></i>skipped</span>
                        <span><i class="mix-conflicts"></i>conflicts</span>
                        <span><i class="mix-failed"></i>failed</span>
                    </span>
                    <button class="btn" id="btn-refresh-batches">Refresh</button>
                </div>
                <div id="batch-list" class="batch-list"></div>
            </div>
        </div>

        <!-- Tools view: expand + job history -->
        <div class="view panel-view" id="view-tools">
            <div class="card">
                <h2>Expand date folders</h2>
                <p class="card-hint">Turns flat folders like <code>2012_05_20</code> into <code>2012/05/20</code>.</p>

                <div class="field">
                    <label for="expand-source">Source folder</label>
                    <div class="field-row">
                        <input type="text" id="expand-source" placeholder="/Volumes/poze/photos">
                        <button class="btn" data-pick="expand-source">Browse</button>
                        <button class="btn" data-fill-current="expand-source" title="Use the folder open in Browse">Current</button>
                    </div>
                </div>

                <div class="field">
                    <label for="expand-target">Target folder (optional - empty means in place)</label>
                    <div class="field-row">
                        <input type="text" id="expand-target" placeholder="in place">
                        <button class="btn" data-pick="expand-target">Browse</button>
                        <button class="btn" data-clear="expand-target">Clear</button>
                    </div>
                </div>

                <div class="options">
                    <label class="check"><input type="checkbox" id="expand-dry-run" checked> Dry run</label>
                    <label class="check"><input type="checkbox" id="expand-move"> Move files instead of copying</label>
                </div>

                <div class="actions">
                    <button class="btn primary" id="btn-expand">Run expand</button>
                    <span class="form-error" id="expand-error"></span>
                </div>
            </div>

            <div class="card">
                <div class="card-head">
                    <h2>Recent jobs</h2>
                    <button class="btn" id="btn-refresh-jobs">Refresh</button>
                </div>
                <div id="job-history" class="job-history"></div>
            </div>

            <div class="card">
                <h2>Server</h2>
                <div id="server-info" class="server-info"></div>
            </div>
        </div>

        <!-- Lightbox overlay -->
        <div class="lightbox" id="lightbox">
            <div class="lightbox-top">
                <span class="lightbox-name" id="lightbox-name"></span>
                <span class="lightbox-index" id="lightbox-index"></span>
                <button class="lightbox-fav" id="lightbox-fav" title="Favorite (*)">&#9734;</button>
                <button class="lightbox-close" id="lightbox-close" title="Close (Esc)">&times;</button>
            </div>
            <div class="lightbox-content">
                <button class="lightbox-nav lightbox-prev" id="lightbox-prev" title="Previous (&larr;)">&lsaquo;</button>
                <img id="lightbox-img" src="" alt="">
                <video id="lightbox-video" controls style="display:none;"></video>
                <button class="lightbox-nav lightbox-next" id="lightbox-next" title="Next (&rarr;)">&rsaquo;</button>
            </div>
            <div class="lightbox-info" id="lightbox-info"></div>
        </div>

        <!-- Directory picker -->
        <div class="modal" id="dir-modal">
            <div class="modal-box">
                <div class="modal-head">
                    <h3>Choose a folder</h3>
                    <button class="btn" id="dir-close">&times;</button>
                </div>
                <div class="modal-path" id="dir-path"></div>
                <div class="modal-list" id="dir-list"></div>
                <div class="modal-actions">
                    <button class="btn" id="dir-up">Up</button>
                    <button class="btn" id="dir-home">Home</button>
                    <button class="btn primary" id="dir-select">Select this folder</button>
                </div>
            </div>
        </div>

        <!-- Size of the selected folder (S) -->
        <div class="modal" id="size-modal">
            <div class="modal-box size-box">
                <div class="modal-head">
                    <h3 id="size-title">Folder size</h3>
                    <button class="btn" id="size-close">&times;</button>
                </div>
                <div class="modal-path" id="size-path"></div>
                <div class="storage-summary" id="size-summary"></div>
                <div class="modal-list" id="size-table"></div>
                <div class="modal-actions">
                    <button class="btn" id="size-up">Up</button>
                    <button class="btn" id="size-rescan">Rescan</button>
                    <button class="btn primary" id="size-open">Open in Storage</button>
                </div>
            </div>
        </div>

        <!-- Help overlay -->
        <div class="help-overlay" id="help-overlay">
            <div class="help-content">
                <h2>Keyboard shortcuts<span class="help-hint">Press <kbd>?</kbd> or <kbd>Esc</kbd> to close</span></h2>
                <div class="help-columns">
                    <div class="help-section">
                        <h3>View</h3>
                        <div class="help-row"><kbd>G</kbd> Grid view</div>
                        <div class="help-row"><kbd>L</kbd> List view</div>
                        <div class="help-row"><kbd>T</kbd> Toggle folder tree</div>
                        <div class="help-row"><kbd>1-4</kbd> Items per page</div>
                        <div class="help-row"><kbd>?</kbd> This help</div>
                    </div>
                    <div class="help-section">
                        <h3>Sort</h3>
                        <div class="help-row"><kbd>N</kbd> By name</div>
                        <div class="help-row"><kbd>M</kbd> By modified date</div>
                        <div class="help-row"><kbd>C</kbd> By created date</div>
                        <div class="help-row"><kbd>A</kbd> By accessed date</div>
                        <div class="help-row"><kbd>Z</kbd> By size</div>
                        <div class="help-row"><kbd>O</kbd> Flip order</div>
                    </div>
                    <div class="help-section">
                        <h3>Navigate</h3>
                        <div class="help-row"><kbd>[ ]</kbd> Previous / next page</div>
                        <div class="help-row"><kbd>&larr; &rarr;</kbd> Select item</div>
                        <div class="help-row"><kbd>Enter</kbd> Open item</div>
                        <div class="help-row"><kbd>Backspace</kbd> Parent folder</div>
                        <div class="help-row"><kbd>Tab</kbd> Switch panel</div>
                        <div class="help-row"><kbd>Esc</kbd> Clear filter / close</div>
                    </div>
                    <div class="help-section">
                        <h3>Library</h3>
                        <div class="help-row"><kbd>F</kbd> Focus filter</div>
                        <div class="help-row"><kbd>V</kbd> All photos below folder</div>
                        <div class="help-row"><kbd>B</kbd> Files of one import</div>
                        <div class="help-row"><kbd>S</kbd> Size of selected folder</div>
                        <div class="help-row"><kbd>*</kbd> Favorite selected item</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="/app.js"></script>
</body>
</html>'''


def get_styles_css() -> str:
    """Return CSS styles."""
    return '''/* Tokens - every colour in the UI comes from here.
   light-dark() picks a side from the root color-scheme, so the theme toggle
   is one attribute on <html> and nothing else has to know. */
:root {
    color-scheme: dark;

    --bg: #1a1b1e;
    --bg: light-dark(#f4f4f6, #1a1b1e);
    --surface: #202226;
    --surface: light-dark(#ffffff, #202226);
    --sunk: #27292e;
    --sunk: light-dark(#ebebee, #27292e);
    --line: #2e3137;
    --line: light-dark(#e1e1e6, #2e3137);
    --line-strong: #3b3f47;
    --line-strong: light-dark(#cdced5, #3b3f47);
    --line-faint: #232529;
    --line-faint: light-dark(#ededf0, #232529);

    --fg: #e9e9ec;
    --fg: light-dark(#1c1d21, #e9e9ec);
    --muted: #9b9fa8;
    --muted: light-dark(#696c76, #9b9fa8);
    --faint: #696d77;
    --faint: light-dark(#9b9da6, #696d77);

    --accent: #6fa1ee;
    --accent: light-dark(#2a63c4, #6fa1ee);
    --accent-soft: #243247;
    --accent-soft: light-dark(#e4edfb, #243247);
    --accent-fg: #0f1a2b;
    --accent-fg: light-dark(#ffffff, #0f1a2b);

    --ok: #7ed29a;
    --ok: light-dark(#1f7a3f, #7ed29a);
    --ok-soft: #1f3328;
    --ok-soft: light-dark(#e2f4e8, #1f3328);
    --warn: #e5b94c;
    --warn: light-dark(#8a5d08, #e5b94c);
    --warn-soft: #3a3221;
    --warn-soft: light-dark(#fbf0d9, #3a3221);
    --danger: #f08b85;
    --danger: light-dark(#c4332e, #f08b85);
    --danger-soft: #3a2527;
    --danger-soft: light-dark(#fbe6e5, #3a2527);
    --star: #f0b429;

    --mono: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
    --radius: 5px;
    --radius-lg: 8px;
    --control: 26px;
}

html[data-theme="light"] { color-scheme: light; }
html[data-theme="dark"] { color-scheme: dark; }

* {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    font-size: 13px;
    background: var(--bg);
    color: var(--fg);
    line-height: 1.4;
    -webkit-font-smoothing: antialiased;
}

.app {
    display: flex;
    flex-direction: column;
    height: 100vh;
    overflow: hidden;
}

/* ---------------------------------------------------------------- header */

.header {
    position: relative;
    flex-shrink: 0;
    background: var(--surface);
    border-bottom: 1px solid var(--line);
}

.header-row {
    display: flex;
    align-items: center;
    gap: 4px;
    height: 40px;
    padding: 0 10px 0 14px;
}

.header h1,
.header-row h1 {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 0 14px 0 0;
    font-size: 13px;
    font-weight: 600;
    letter-spacing: -.01em;
    white-space: nowrap;
    color: var(--fg);
}

/* The little photo mark in front of the name */
.header h1::before {
    content: "";
    width: 14px;
    height: 11px;
    border-radius: 2px;
    background: var(--accent);
    box-shadow: -4px -3px 0 -3px var(--accent);
}

.tabs {
    display: flex;
    align-items: stretch;
    height: 40px;
    gap: 2px;
}

.tab {
    position: relative;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    height: 40px;
    padding: 0 10px;
    border: none;
    background: transparent;
    color: var(--muted);
    font-family: inherit;
    font-size: 13px;
    cursor: pointer;
}

.tab:hover {
    color: var(--fg);
}

.tab.active {
    color: var(--fg);
    font-weight: 600;
}

.tab.active::after {
    content: "";
    position: absolute;
    left: 10px;
    right: 10px;
    bottom: -1px;
    height: 2px;
    border-radius: 1px;
    background: var(--accent);
}

.tab-count {
    font-family: var(--mono);
    font-size: 11px;
    color: var(--muted);
}

.header-tools {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 6px;
    position: relative;
}

.import-only.hidden {
    display: none;
}

/* Icon-only buttons in the header (theme, help) */
.icon-btn {
    width: 28px;
    height: var(--control);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 0;
    border: 1px solid transparent;
    background: transparent;
    color: var(--fg);
    border-radius: var(--radius);
    font-family: inherit;
    cursor: pointer;
}

.icon-btn:hover {
    background: var(--sunk);
}

/* Half-filled circle: the usual "switch theme" mark */
.theme-mark {
    width: 14px;
    height: 14px;
    border-radius: 50%;
    border: 1.5px solid var(--fg);
    background: linear-gradient(90deg, var(--fg) 50%, transparent 50%);
}

/* ------------------------------------------------------------------ views */

.view {
    display: none;
    flex: 1;
    min-height: 0;
    overflow: hidden;
}

.view.active {
    display: flex;
}

.main {
    overflow: hidden;
}

.panel-view {
    flex-direction: column;
    align-items: center;
    overflow-y: auto;
    padding: 20px 24px 32px;
    gap: 16px;
}

.content {
    flex: 1;
    display: flex;
    flex-direction: column;
    min-width: 0;
    min-height: 0;
    overflow: hidden;
}

/* ------------------------------------------------------------------- tree */

.sidebar {
    width: 220px;
    flex-shrink: 0;
    background: var(--surface);
    border-right: 1px solid var(--line);
    overflow-y: auto;
    padding: 8px 6px;
}

.sidebar[hidden] {
    display: none;
}

.tree {
    font-size: 12.5px;
}

.tree-item {
    padding: 0;
}

.tree-folder {
    display: flex;
    align-items: center;
    gap: 4px;
    height: 24px;
    padding: 0 6px 0 2px;
    border-radius: 4px;
    cursor: pointer;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.tree-folder:hover {
    background: var(--sunk);
}

.tree-folder.has-children::before {
    content: "\\203A";
    width: 14px;
    flex-shrink: 0;
    text-align: center;
    color: var(--faint);
    font-size: 13px;
    transition: transform 0.15s;
}

.tree-folder:not(.has-children)::before {
    content: "";
    width: 14px;
    flex-shrink: 0;
}

.tree-folder.open::before {
    transform: rotate(90deg);
}

.tree-folder.loading::before {
    content: "";
    width: 10px;
    height: 10px;
    margin: 0 2px;
    border: 2px solid var(--accent);
    border-top-color: transparent;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
}

@keyframes spin {
    to { transform: rotate(360deg); }
}

.tree-children {
    margin-left: 10px;
    display: none;
}

.tree-folder.open + .tree-children {
    display: block;
}

.tree-folder.active {
    background: var(--accent-soft);
    color: var(--accent);
    font-weight: 600;
}

.tree-folder.focused {
    outline: 1px solid var(--accent);
    outline-offset: -1px;
}

/* Which panel the arrow keys drive */
.sidebar.focused {
    box-shadow: inset 2px 0 0 var(--accent);
}

.content.focused {
    box-shadow: inset 2px 0 0 var(--accent);
}

.tree-empty {
    color: var(--faint);
    font-style: italic;
    padding: 4px 8px;
}

.loading-indicator {
    color: var(--muted);
    padding: 40px 0;
    text-align: center;
    font-size: 12.5px;
    grid-column: 1 / -1;
}

/* --------------------------------------------------------------- path bar */

.path-bar {
    flex-shrink: 0;
    min-height: 36px;
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 0 10px;
    background: var(--surface);
    border-bottom: 1px solid var(--line);
}

.breadcrumb {
    display: flex;
    align-items: center;
    gap: 2px;
    flex-shrink: 0;
    min-width: 0;
    overflow: hidden;
    font-size: 12.5px;
    white-space: nowrap;
    color: var(--muted);
}

.breadcrumb a {
    color: var(--muted);
    text-decoration: none;
    padding: 2px 4px;
    border-radius: 4px;
}

.breadcrumb a:hover {
    background: var(--sunk);
    text-decoration: none;
}

.breadcrumb a:last-child {
    color: var(--fg);
    font-weight: 600;
}

.breadcrumb .separator {
    color: var(--faint);
}

/* Chips in the path bar: one import, or the flattened view */
.chip {
    display: inline-flex;
    align-items: center;
    overflow: hidden;
    flex-shrink: 1;
    gap: 8px;
    height: 24px;
    padding: 0 4px 0 8px;
    border-radius: 12px;
    background: var(--accent-soft);
    color: var(--accent);
    font-size: 12px;
    white-space: nowrap;
    min-width: 0;
}

.chip[hidden] {
    display: none;
}

.chip b {
    font-weight: 600;
}

.chip .mono {
    font-family: var(--mono);
    font-size: 11px;
}

.chip .chip-paths {
    font-family: var(--mono);
    font-size: 11px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
    max-width: 260px;
}

.chip .warn {
    color: var(--warn);
}

.chip-close {
    width: 18px;
    height: 18px;
    flex-shrink: 0;
    border: none;
    border-radius: 50%;
    background: transparent;
    color: inherit;
    cursor: pointer;
    font-size: 14px;
    line-height: 1;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 0;
}

.chip-close:hover {
    background: var(--surface);
}

.file-count {
    margin-left: auto;
    padding-left: 8px;
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--muted);
    white-space: nowrap;
}

/* ---------------------------------------------------------------- toolbar */

.toolbar {
    flex-shrink: 0;
    min-height: 36px;
    padding: 5px 10px;
    background: var(--surface);
    border-bottom: 1px solid var(--line);
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 6px;
}

.toolbar-title {
    font-weight: 600;
    margin-right: 4px;
}

.btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    height: var(--control);
    padding: 0 8px;
    border: 1px solid var(--line-strong);
    background: var(--surface);
    color: var(--fg);
    border-radius: var(--radius);
    font-family: inherit;
    font-size: 12.5px;
    font-weight: 500;
    white-space: nowrap;
    cursor: pointer;
}

.btn:hover {
    background: var(--sunk);
}

.btn.active {
    background: var(--accent-soft);
    border-color: var(--accent);
    color: var(--accent);
}

.btn.primary {
    background: var(--accent);
    border-color: var(--accent);
    color: var(--accent-fg);
    font-weight: 600;
}

.btn.primary:hover {
    background: var(--accent);
    filter: brightness(1.08);
}

.btn.danger {
    border-color: var(--danger);
    color: var(--danger);
}

.btn.danger:hover {
    background: var(--danger-soft);
}

.btn.tiny {
    height: 24px;
    width: 24px;
    padding: 0;
    justify-content: center;
    border-color: transparent;
    background: transparent;
    color: var(--muted);
}

.btn.tiny:hover {
    background: var(--sunk);
}

.btn:disabled {
    opacity: 0.45;
    cursor: not-allowed;
}

.btn:disabled:hover {
    background: var(--surface);
    filter: none;
}

select.btn {
    padding-right: 4px;
}

/* Keyboard shortcut badges, shown while the help overlay is up */
.btn[data-shortcut],
.page-btn[data-shortcut],
.filter-wrapper,
.shortcut-wrap {
    position: relative;
}

.filter-wrapper,
.shortcut-wrap {
    display: inline-flex;
    align-items: center;
}

.btn[data-shortcut]::after,
.page-btn[data-shortcut]::after,
.filter-wrapper[data-shortcut]::after,
.shortcut-wrap[data-shortcut]::after {
    content: attr(data-shortcut);
    position: absolute;
    top: -7px;
    right: -7px;
    z-index: 1;
    font-family: var(--mono);
    font-size: 9.5px;
    font-weight: 600;
    line-height: 1;
    padding: 2px 3px;
    border: 1px solid var(--line-strong);
    border-radius: 3px;
    background: var(--surface);
    color: var(--muted);
    opacity: 0;
    transition: opacity 0.15s;
    pointer-events: none;
}

.show-shortcuts .btn[data-shortcut]::after,
.show-shortcuts .page-btn[data-shortcut]::after,
.show-shortcuts .filter-wrapper[data-shortcut]::after,
.show-shortcuts .shortcut-wrap[data-shortcut]::after {
    opacity: 1;
}

#btn-help {
    min-width: 28px;
    justify-content: center;
    font-weight: 600;
}

#btn-help.active::after {
    display: none;
}

.filter-input {
    height: var(--control);
    width: 170px;
    padding: 0 8px;
    border: 1px solid var(--line-strong);
    background: var(--surface);
    color: var(--fg);
    border-radius: var(--radius);
    font-family: inherit;
    font-size: 12.5px;
    outline: none;
}

.filter-input:focus {
    border-color: var(--accent);
    box-shadow: 0 0 0 2px var(--accent-soft);
}

.filter-input::placeholder {
    color: var(--faint);
}

#btn-clear-filter {
    width: 24px;
    padding: 0;
    justify-content: center;
}

.filter-wrapper:has(.filter-input:not(:placeholder-shown)) ~ #btn-clear-filter {
    display: inline-flex !important;
}

#batch-select {
    max-width: 260px;
}

/* Import is the one panel with two columns */
#view-import.active {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
    align-items: start;
    align-content: start;
    width: 100%;
    max-width: 928px;
    margin: 0 auto;
}

/* The batch the quick copy would run on */
.latest-batch {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 12px;
    margin-bottom: 12px;
    border: 1px solid var(--line);
    border-radius: 6px;
    background: var(--bg);
    font-size: 12.5px;
}

.latest-batch .batch-id {
    font-size: 12.5px;
}

.latest-batch .source {
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--muted);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.latest-batch .pending {
    margin-left: auto;
    font-family: var(--mono);
    font-size: 11.5px;
    white-space: nowrap;
}

.latest-batch:empty {
    display: none;
}

#btn-tree {
    width: 28px;
    height: var(--control);
    font-size: 13px;
    flex-shrink: 0;
}

/* ------------------------------------------------------------------- grid */

.file-grid {
    flex: 1;
    overflow-y: auto;
    padding: 10px 12px;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(124px, 1fr));
    /* Rows follow their content - 1fr would stretch a single row over the
       whole viewport and squash the tiles once there are many */
    grid-auto-rows: max-content;
    gap: 6px 8px;
    align-content: start;
}

.file-item {
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 5px;
    padding: 4px;
    border-radius: 6px;
    background: transparent;
    cursor: pointer;
    min-height: 0;
}

.file-item:hover {
    background: var(--sunk);
}

.file-item.selected {
    background: var(--accent-soft);
    outline: 2px solid var(--accent);
    outline-offset: -2px;
}

.file-thumb,
.file-icon,
.raw-icon,
.video-icon {
    width: 100%;
    aspect-ratio: 1;
    min-height: 0;
    border: 1px solid var(--line);
    border-radius: 4px;
    background: var(--sunk);
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
}

.file-thumb {
    object-fit: contain;
    object-position: center;
    display: block;
}

.file-icon {
    font-size: 30px;
    color: var(--accent);
}

.raw-icon {
    font-family: var(--mono);
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.08em;
    color: var(--muted);
}

.video-icon {
    font-size: 24px;
    color: var(--muted);
}

.file-name {
    padding: 0 2px;
    font-size: 12px;
    line-height: 1.3;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex-shrink: 0;
}

.file-sub {
    padding: 0 2px;
    font-family: var(--mono);
    font-size: 10.5px;
    line-height: 1.4;
    color: var(--muted);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex-shrink: 0;
}

/* Columns that only the list view shows */
.file-kind,
.file-size,
.file-date {
    display: none;
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--muted);
    white-space: nowrap;
    overflow: hidden;
}

.file-item.folder .file-icon {
    color: var(--accent);
}

.file-item.video {
    position: relative;
}

.file-item.video .file-thumb {
    position: relative;
}

/* The play triangle sits over the thumbnail, not over the name */
.file-item.video::after {
    content: "";
    position: absolute;
    top: 4px;
    left: 4px;
    right: 4px;
    aspect-ratio: 1;
    background:
        linear-gradient(transparent, transparent),
        radial-gradient(circle, rgba(0,0,0,.55) 18px, transparent 19px);
    background-position: center;
    background-repeat: no-repeat;
    pointer-events: none;
}

.file-item.video .video-play {
    position: absolute;
    top: 4px;
    left: 4px;
    right: 4px;
    aspect-ratio: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    pointer-events: none;
    color: #fff;
    font-size: 20px;
    text-shadow: 0 1px 6px rgba(0,0,0,.6);
}

.file-item.missing {
    opacity: 0.55;
    outline: 1px dashed var(--danger);
    outline-offset: -1px;
}

.empty,
.error {
    grid-column: 1 / -1;
    padding: 40px 0;
    text-align: center;
    font-size: 12.5px;
    color: var(--muted);
}

.error {
    color: var(--danger);
}

/* Favourite star over a tile */
.fav-toggle {
    position: absolute;
    top: 8px;
    right: 8px;
    width: 24px;
    height: 24px;
    border: none;
    border-radius: 50%;
    background: rgba(0,0,0,0.5);
    color: #ddd;
    font-size: 14px;
    line-height: 1;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0;
    opacity: 0;
    transition: opacity 0.12s;
}

.file-item:hover .fav-toggle,
.file-item.selected .fav-toggle,
.fav-toggle.on {
    opacity: 1;
}

.fav-toggle:hover {
    background: rgba(0,0,0,0.75);
    color: var(--star);
}

.fav-toggle.on {
    color: var(--star);
}

/* ------------------------------------------------------------- list view */

.file-grid.list-view {
    display: block;
    padding: 0;
}

.list-head,
.file-grid.list-view .file-item {
    display: grid;
    grid-template-columns: 36px minmax(0, 1fr) 80px 90px 150px 32px;
    align-items: center;
    gap: 0;
    padding: 0 12px 0 8px;
}

.file-grid.list-view .file-thumb,
.file-grid.list-view .file-icon,
.file-grid.list-view .raw-icon,
.file-grid.list-view .video-icon { grid-column: 1; }
.file-grid.list-view .file-name { grid-column: 2; }
.file-grid.list-view .file-kind { grid-column: 3; }
.file-grid.list-view .file-size { grid-column: 4; }
.file-grid.list-view .file-date { grid-column: 5; }
.file-grid.list-view .fav-toggle { grid-column: 6; }

.list-head {
    position: sticky;
    top: 0;
    z-index: 2;
    height: 28px;
    background: var(--bg);
    border-bottom: 1px solid var(--line);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
}

.list-head span:nth-child(4),
.list-head span:nth-child(5) {
    text-align: right;
    padding-right: 4px;
}

.file-grid:not(.list-view) .list-head {
    display: none;
}

.file-grid.list-view .file-item {
    height: 30px;
    border-radius: 0;
    border-bottom: 1px solid var(--line-faint);
    font-size: 12.5px;
}

.file-grid.list-view .file-thumb,
.file-grid.list-view .file-icon,
.file-grid.list-view .raw-icon,
.file-grid.list-view .video-icon {
    width: 24px;
    height: 24px;
    aspect-ratio: auto;
    border-radius: 3px;
    font-size: 12px;
}

.file-grid.list-view .raw-icon {
    font-size: 7px;
    letter-spacing: 0;
}

.file-grid.list-view .file-name {
    padding: 0 8px;
}

.file-grid.list-view .file-sub {
    display: none;
}

.file-grid.list-view .file-kind {
    display: block;
}

.file-grid.list-view .file-size,
.file-grid.list-view .file-date {
    display: block;
    text-align: right;
    padding-right: 4px;
}

.file-grid.list-view .file-item.video::after,
.file-grid.list-view .video-play {
    display: none;
}

.file-grid.list-view .fav-toggle {
    position: static;
    background: transparent;
    width: 22px;
    height: 22px;
    justify-self: end;
}

/* --------------------------------------------------------------- lightbox */

.lightbox {
    position: fixed;
    inset: 0;
    z-index: 1000;
    display: none;
    flex-direction: column;
    background: rgba(8,8,10,.96);
    background: light-dark(rgba(24,24,28,.97), rgba(8,8,10,.96));
    color: #f2f2f4;
    overflow: hidden;
}

.lightbox.active {
    display: flex;
}

.lightbox-top {
    flex-shrink: 0;
    height: 44px;
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 0 10px 0 16px;
}

.lightbox-name {
    font-weight: 600;
    font-size: 13px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.lightbox-index {
    font-family: var(--mono);
    font-size: 11.5px;
    color: rgba(242,242,244,.6);
    white-space: nowrap;
}

.lightbox-close,
.lightbox-fav {
    width: 32px;
    height: 32px;
    flex-shrink: 0;
    border: none;
    border-radius: 6px;
    background: transparent;
    color: #f2f2f4;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 0;
    line-height: 1;
}

.lightbox-close {
    font-size: 20px;
}

.lightbox-fav {
    margin-left: auto;
    font-size: 18px;
}

.lightbox-close:hover,
.lightbox-fav:hover {
    background: rgba(255,255,255,0.1);
}

.lightbox-fav.on {
    color: var(--star);
}

.lightbox-content {
    flex: 1;
    min-height: 0;
    position: relative;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 8px 56px;
    overflow: hidden;
}

.lightbox-nav {
    position: absolute;
    top: 0;
    bottom: 0;
    width: 56px;
    border: none;
    background: transparent;
    color: rgba(242,242,244,.55);
    font-size: 30px;
    cursor: pointer;
    padding: 0;
    z-index: 10;
}

.lightbox-nav:hover {
    color: #f2f2f4;
    background: rgba(255,255,255,0.04);
}

.lightbox-prev { left: 0; }
.lightbox-next { right: 0; }

#lightbox-img,
#lightbox-video {
    max-width: 100%;
    max-height: 100%;
    width: auto;
    height: auto;
    object-fit: contain;
    box-shadow: 0 10px 40px rgba(0,0,0,.5);
}

#lightbox-video {
    background: #000;
}

.lightbox-info {
    flex-shrink: 0;
    height: 36px;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 18px;
    padding: 0 16px;
    font-family: var(--mono);
    font-size: 11.5px;
    color: rgba(242,242,244,.7);
    white-space: nowrap;
    overflow: hidden;
}

/* ------------------------------------------------------------- status bar */

.status-bar {
    flex-shrink: 0;
    min-height: 30px;
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 2px 10px;
    background: var(--surface);
    border-top: 1px solid var(--line);
    font-size: 12px;
    color: var(--muted);
}

.status-left {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.status-right {
    margin-left: auto;
    display: inline-flex;
    align-items: center;
    gap: 8px;
}

.pagination {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    flex-wrap: wrap;
}

.pagination:empty {
    display: none;
}

.pagination .page-btn {
    min-width: 24px;
    height: 24px;
    padding: 0 6px;
    border: 1px solid var(--line-strong);
    background: var(--surface);
    color: var(--fg);
    border-radius: var(--radius);
    font-family: var(--mono);
    font-size: 11.5px;
    cursor: pointer;
}

.pagination .page-btn:hover:not(:disabled) {
    background: var(--sunk);
}

.pagination .page-btn.active {
    background: var(--accent);
    border-color: var(--accent);
    color: var(--accent-fg);
}

.pagination .page-btn:disabled {
    opacity: 0.45;
    cursor: not-allowed;
}

.pagination .page-info {
    color: var(--muted);
    font-size: 11.5px;
    padding: 0 6px;
}

.pagination .page-ellipsis {
    color: var(--faint);
    padding: 0 4px;
}

#per-page-select {
    height: 24px;
    padding: 0 4px;
    border: 1px solid var(--line-strong);
    background: var(--surface);
    color: var(--muted);
    border-radius: var(--radius);
    font-family: inherit;
    font-size: 11.5px;
    cursor: pointer;
    outline: none;
}

/* ------------------------------------------------------- cards and forms */

.card {
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: var(--radius-lg);
    padding: 16px 18px;
    max-width: 880px;
    width: 100%;
}

.card h2 {
    font-size: 15px;
    font-weight: 600;
    color: var(--fg);
    margin-bottom: 8px;
}

.card-head {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 12px;
    flex-wrap: wrap;
}

.card-head h2 {
    margin-bottom: 0;
    margin-right: auto;
}

.card-hint {
    color: var(--muted);
    font-size: 12.5px;
    margin-bottom: 12px;
}

.card-hint code {
    font-family: var(--mono);
    font-size: 11.5px;
    background: var(--sunk);
    padding: 1px 5px;
    border-radius: 3px;
}

.seg {
    display: inline-flex;
    height: 24px;
    border: 1px solid var(--line-strong);
    border-radius: var(--radius);
    overflow: hidden;
    margin-bottom: 12px;
}

.card-head .seg {
    margin-bottom: 0;
}

.seg-btn {
    padding: 0 10px;
    background: var(--surface);
    border: none;
    color: var(--muted);
    font-family: inherit;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
}

.seg-btn + .seg-btn {
    border-left: 1px solid var(--line-strong);
}

.seg-btn:hover {
    background: var(--sunk);
}

.seg-btn.active {
    background: var(--accent-soft);
    color: var(--accent);
}

.field {
    margin-bottom: 12px;
}

.field label {
    display: block;
    font-size: 12px;
    font-weight: 500;
    color: var(--muted);
    margin-bottom: 6px;
}

.field-row {
    display: flex;
    gap: 6px;
}

.field-row input[type="text"] {
    flex: 1;
    min-width: 0;
    height: 28px;
    padding: 0 8px;
    background: var(--surface);
    border: 1px solid var(--line-strong);
    border-radius: var(--radius);
    color: var(--fg);
    font-family: var(--mono);
    font-size: 12px;
    outline: none;
}

.field-row input[type="text"]:focus {
    border-color: var(--accent);
    box-shadow: 0 0 0 2px var(--accent-soft);
}

.field-row .btn {
    height: 28px;
}

.dest-hint {
    margin-top: 6px;
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--muted);
    word-break: break-all;
}

.dest-hint:empty {
    display: none;
}

.options {
    display: flex;
    flex-wrap: wrap;
    gap: 8px 18px;
    margin: 12px 0;
}

.check {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12.5px;
    color: var(--fg);
    cursor: pointer;
}

.check input[type="checkbox"] {
    margin: 0;
    accent-color: var(--accent);
}

.check input[type="number"] {
    width: 64px;
    height: 24px;
    padding: 0 6px;
    background: var(--surface);
    border: 1px solid var(--line-strong);
    border-radius: var(--radius);
    color: var(--fg);
    font-family: var(--mono);
    font-size: 12px;
    outline: none;
}

.actions {
    display: flex;
    align-items: center;
    gap: 10px;
    padding-top: 12px;
    border-top: 1px solid var(--line);
}

.actions .btn {
    height: 28px;
    padding: 0 12px;
}

.form-error {
    color: var(--danger);
    font-size: 12.5px;
}

.form-ok {
    color: var(--ok);
    font-size: 12.5px;
}

/* Mounted volumes quick pick (import source) */
.volumes {
    margin-top: 8px;
}

.volumes-head {
    display: flex;
    align-items: center;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--muted);
    margin-bottom: 6px;
}

.volumes-head span {
    margin-right: auto;
}

.volume-list {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-items: center;
}

.vol-btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    max-width: 100%;
    height: 24px;
    padding: 0 9px;
    background: var(--surface);
    border: 1px solid var(--line-strong);
    border-radius: 12px;
    color: var(--fg);
    font-family: inherit;
    font-size: 12px;
    cursor: pointer;
    white-space: nowrap;
}

.vol-btn:hover {
    background: var(--sunk);
    border-color: var(--accent);
}

.vol-btn .vol-icon {
    flex-shrink: 0;
    color: var(--muted);
    font-size: 11px;
}

.vol-btn .vol-name {
    overflow: hidden;
    text-overflow: ellipsis;
}

.volume-list .empty-row {
    color: var(--faint);
    font-size: 12px;
}

/* ----------------------------------------------------------- the job pill */

.job-pill {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    height: var(--control);
    padding: 0 10px;
    border: 1px solid var(--line-strong);
    background: var(--surface);
    color: var(--fg);
    border-radius: 13px;
    font-family: inherit;
    font-size: 12px;
    cursor: pointer;
    white-space: nowrap;
}

.job-pill[hidden] {
    display: none;
}

.job-pill:hover {
    background: var(--sunk);
}

.job-pill .job-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--accent);
    box-shadow: 0 0 0 3px var(--accent-soft);
}

.job-pill .job-pill-title {
    font-weight: 500;
}

.job-pill .job-pill-track {
    width: 64px;
    height: 4px;
    border-radius: 2px;
    background: var(--line);
    overflow: hidden;
}

.job-pill .job-pill-fill {
    display: block;
    height: 100%;
    width: 0;
    background: var(--accent);
    transition: width 0.2s linear;
}

.job-pill .job-pill-meta {
    font-family: var(--mono);
    font-size: 11px;
    color: var(--muted);
}

/* Thin progress line under the whole header */
.header-progress {
    position: absolute;
    left: 0;
    bottom: -1px;
    height: 2px;
    width: 0;
    background: var(--accent);
    pointer-events: none;
    transition: width 0.2s linear;
}

.header-progress[hidden] {
    display: none;
}

/* The popover the pill opens */
.job-bar {
    position: absolute;
    top: 34px;
    right: 0;
    z-index: 50;
    width: 360px;
    padding: 12px 14px;
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: 10px;
    box-shadow: 0 16px 40px rgba(0,0,0,.28);
    display: flex;
    flex-direction: column;
    gap: 8px;
}

.job-bar[hidden] {
    display: none;
}

.job-bar-head {
    display: flex;
    align-items: center;
    gap: 8px;
}

.job-title {
    font-size: 13px;
    font-weight: 600;
}

.job-badge,
.batch-status,
.conflict-badge {
    font-size: 10.5px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    padding: 3px 6px;
    border-radius: 4px;
    background: var(--accent-soft);
    color: var(--accent);
    white-space: nowrap;
}

.job-badge.completed,
.batch-status.completed,
.conflict-badge.identical {
    background: var(--ok-soft);
    color: var(--ok);
}

.job-badge.failed,
.batch-status.failed,
.conflict-badge.different {
    background: var(--danger-soft);
    color: var(--danger);
}

.job-badge.cancelled,
.batch-status.paused {
    background: var(--warn-soft);
    color: var(--warn);
}

.job-actions {
    margin-left: auto;
    display: flex;
    gap: 6px;
}

.job-actions .btn {
    height: 24px;
    font-size: 12px;
}

.progress {
    height: 4px;
    background: var(--line);
    border-radius: 2px;
    overflow: hidden;
}

.progress-fill {
    height: 100%;
    width: 0;
    background: var(--accent);
    transition: width 0.2s linear;
}

.job-meta {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--muted);
}

.job-meta span {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.job-result {
    font-size: 12px;
    color: var(--fg);
}

.job-result:empty {
    display: none;
}

/* --------------------------------------------------------------- batches */

.batch-list {
    display: flex;
    flex-direction: column;
    gap: 12px;
}

.batch {
    border: 1px solid var(--line);
    border-radius: var(--radius-lg);
    background: var(--surface);
    padding: 12px 14px;
    display: flex;
    flex-direction: column;
    gap: 10px;
}

.batch-head {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
    min-width: 0;
}

.batch-id {
    font-family: var(--mono);
    font-size: 13px;
    font-weight: 600;
    color: var(--fg);
}

.batch-when {
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--muted);
}

/* copied / skipped / conflicts / failed, as one bar */
.batch-mix {
    display: flex;
    height: 6px;
    border-radius: 3px;
    overflow: hidden;
    background: var(--sunk);
}

.batch-mix span {
    display: block;
}

.batch-mix .mix-copied { background: var(--ok); }
.batch-mix .mix-skipped { background: var(--line-strong); }
.batch-mix .mix-conflicts { background: var(--warn); }
.batch-mix .mix-failed { background: var(--danger); }

.batch-legend {
    display: inline-flex;
    align-items: center;
    gap: 12px;
    font-size: 11px;
    color: var(--muted);
}

.batch-legend span {
    display: inline-flex;
    align-items: center;
    gap: 4px;
}

.batch-legend i {
    width: 8px;
    height: 8px;
    border-radius: 2px;
    display: inline-block;
}

.batch-legend .mix-copied { background: var(--ok); }
.batch-legend .mix-skipped { background: var(--line-strong); }
.batch-legend .mix-conflicts { background: var(--warn); }
.batch-legend .mix-failed { background: var(--danger); }

.card-head .batch-legend {
    margin-left: auto;
}

.batch-paths {
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--muted);
    word-break: break-all;
}

.batch-head .batch-paths {
    margin-left: auto;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    word-break: normal;
    min-width: 0;
}

.batch-stats {
    display: flex;
    flex-wrap: wrap;
    gap: 4px 16px;
    font-size: 12px;
    color: var(--muted);
    white-space: nowrap;
}

.batch-stats b {
    font-family: var(--mono);
    font-weight: 600;
    color: var(--fg);
}

.batch-stats b.warn { color: var(--warn); }
.batch-stats b.bad { color: var(--danger); }

.batch-actions {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
    align-items: center;
}

.batch-actions .btn.warn {
    border-color: var(--warn);
    background: var(--warn-soft);
    color: var(--warn);
    font-weight: 600;
}

.batch-actions .btn.ghost {
    border-color: transparent;
    background: transparent;
    color: var(--accent);
}

.batch-actions .btn.ghost:hover {
    background: var(--sunk);
}

.batch-failed {
    border-top: 1px solid var(--line);
    padding-top: 8px;
    display: flex;
    flex-direction: column;
    gap: 4px;
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--danger);
    max-height: 200px;
    overflow-y: auto;
}

.batch-failed div {
    word-break: break-all;
}

/* ------------------------------------------------------- jobs and server */

.job-history {
    display: flex;
    flex-direction: column;
}

.job-row {
    display: grid;
    grid-template-columns: 92px minmax(0, 1fr) 150px 70px;
    align-items: center;
    gap: 12px;
    height: 32px;
    border-top: 1px solid var(--line-faint);
    font-size: 12.5px;
}

.job-row:first-child {
    border-top: none;
}

.job-row .grow {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.job-row .when,
.job-row .elapsed {
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--muted);
}

.job-row .elapsed {
    text-align: right;
}

.server-info {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 4px 16px;
    font-size: 12.5px;
    color: var(--muted);
}

.server-info div {
    font-family: var(--mono);
    font-size: 11.5px;
    color: var(--fg);
    word-break: break-all;
}

.server-info div.label {
    font-family: inherit;
    font-size: 12.5px;
    color: var(--muted);
}

/* --------------------------------------------------------------- storage */

.storage-path {
    font-family: var(--mono);
    font-size: 12px;
    color: var(--muted);
    word-break: break-all;
    margin-bottom: 12px;
}

.storage-summary {
    display: flex;
    flex-wrap: wrap;
    gap: 0 28px;
    padding: 12px 14px;
    margin-bottom: 14px;
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: var(--radius-lg);
}

.storage-summary span {
    display: flex;
    flex-direction: column;
    gap: 2px;
    font-size: 11px;
    color: var(--muted);
}

.storage-summary b {
    font-family: var(--mono);
    font-size: 16px;
    font-weight: 600;
    color: var(--fg);
}

.storage-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12.5px;
}

.storage-table th {
    text-align: right;
    padding: 0 8px;
    height: 30px;
    color: var(--muted);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    border-bottom: 1px solid var(--line);
}

.storage-table th:first-child,
.storage-table td:first-child {
    text-align: left;
}

.storage-table td {
    padding: 0 8px;
    height: 32px;
    border-bottom: 1px solid var(--line-faint);
}

.storage-table tr:hover td {
    background: var(--sunk);
}

.storage-table .folder-link {
    color: var(--accent);
    cursor: pointer;
    font-weight: 500;
}

.storage-table .folder-link:hover {
    text-decoration: underline;
}

.storage-table .num {
    text-align: right;
    font-family: var(--mono);
    font-size: 11.5px;
    font-variant-numeric: tabular-nums;
}

.storage-bar {
    height: 6px;
    min-width: 2px;
    background: var(--accent);
    border-radius: 3px;
}

.storage-table tfoot td {
    font-weight: 600;
    background: var(--bg);
    border-bottom: none;
}

/* ------------------------------------------------------------- conflicts */

.bulk-actions {
    display: flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
    padding: 8px 12px;
    margin-bottom: 12px;
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: var(--radius-lg);
    font-size: 12.5px;
    color: var(--muted);
}

.conflict-list {
    display: flex;
    flex-direction: column;
    gap: 12px;
}

.conflict {
    border: 1px solid var(--line);
    border-radius: var(--radius-lg);
    background: var(--surface);
    padding: 12px 14px;
    display: flex;
    flex-direction: column;
    gap: 10px;
}

.conflict-head {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
}

.conflict-name {
    font-family: var(--mono);
    font-size: 13px;
    font-weight: 600;
    word-break: break-all;
}

.conflict-sides {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: 10px;
}

.conflict-side {
    display: flex;
    flex-direction: column;
    gap: 6px;
}

.conflict-side h4 {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
}

.conflict-side .preview {
    width: 100%;
    height: 150px;
    object-fit: contain;
    background: var(--sunk);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    display: block;
}

.conflict-side .preview[hidden],
.conflict-side .no-preview[hidden] {
    display: none;
}

.conflict-side .no-preview {
    width: 100%;
    height: 150px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--sunk);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    color: var(--muted);
    font-size: 12px;
    text-align: center;
    padding: 8px;
}

.conflict-meta {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 2px 10px;
    font-size: 12px;
    color: var(--muted);
    word-break: break-all;
}

.conflict-meta b {
    font-family: var(--mono);
    font-size: 11.5px;
    font-weight: 400;
    color: var(--fg);
}

.conflict-meta b.warn {
    color: var(--warn);
}

.conflict-actions {
    margin-left: auto;
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
}

/* ------------------------------------------------------------- modals */

.modal {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.45);
    display: none;
    align-items: center;
    justify-content: center;
    z-index: 1100;
}

.modal.active {
    display: flex;
}

.modal-box {
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: 10px;
    box-shadow: 0 24px 64px rgba(0,0,0,.35);
    width: min(560px, 92vw);
    max-height: 80vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
}

.modal-head {
    display: flex;
    align-items: center;
    gap: 10px;
    height: 44px;
    padding: 0 10px 0 16px;
    border-bottom: 1px solid var(--line);
    flex-shrink: 0;
}

.modal-head h3 {
    font-size: 13px;
    font-weight: 600;
    margin-right: auto;
}

.modal-head .btn {
    width: 28px;
    height: 28px;
    padding: 0;
    justify-content: center;
    border-color: transparent;
    background: transparent;
    color: var(--muted);
    font-size: 18px;
}

.modal-head .btn:hover {
    background: var(--sunk);
}

.modal-path {
    padding: 8px 16px;
    font-family: var(--mono);
    font-size: 12px;
    color: var(--muted);
    word-break: break-all;
    flex-shrink: 0;
}

.modal-list {
    flex: 1;
    overflow-y: auto;
    border-top: 1px solid var(--line-faint);
    min-height: 200px;
}

.modal-list .dir-row {
    display: flex;
    align-items: center;
    gap: 10px;
    height: 30px;
    padding: 0 16px;
    cursor: pointer;
    font-size: 12.5px;
    border-bottom: 1px solid var(--line-faint);
}

.modal-list .dir-row:hover {
    background: var(--sunk);
}

/* A folder mark, so a row reads as a folder and not as text */
.modal-list .dir-row::before {
    content: "";
    width: 12px;
    height: 9px;
    flex-shrink: 0;
    border-radius: 1.5px;
    background: var(--accent);
    opacity: 0.8;
}

.modal-list .dir-row::after {
    content: "\203A";
    margin-left: auto;
    color: var(--faint);
}

.modal-list .empty-row {
    padding: 16px;
    color: var(--muted);
    font-size: 12.5px;
}

.modal-actions {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 10px 16px;
    border-top: 1px solid var(--line);
    flex-shrink: 0;
}

.modal-actions .grow,
.modal-actions .btn.primary {
    margin-left: auto;
}

.size-box {
    width: min(720px, 94vw);
}

.size-box .storage-summary {
    margin: 0;
    border: none;
    border-bottom: 1px solid var(--line);
    border-radius: 0;
}

.size-box .modal-list {
    border-top: none;
    min-height: 160px;
}

.size-box .storage-table td,
.size-box .storage-table th {
    padding: 0 16px;
}

/* ---------------------------------------------------------- help overlay */

.help-overlay {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.45);
    display: none;
    align-items: center;
    justify-content: center;
    z-index: 2000;
}

.help-overlay.active {
    display: flex;
}

.help-content {
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: 10px;
    box-shadow: 0 24px 64px rgba(0,0,0,.35);
    padding: 18px 22px 20px;
    width: min(760px, 94vw);
    max-height: 88vh;
    overflow-y: auto;
}

.help-content h2 {
    font-size: 14px;
    font-weight: 600;
    margin-bottom: 14px;
    color: var(--fg);
}

.help-columns {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 18px 28px;
}

.help-section h3 {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--muted);
    padding-bottom: 6px;
    margin-bottom: 6px;
    border-bottom: 1px solid var(--line);
}

.help-row {
    display: flex;
    align-items: center;
    gap: 10px;
    height: 26px;
    font-size: 12.5px;
}

.help-row kbd,
.help-hint kbd {
    min-width: 22px;
    text-align: center;
    font-family: var(--mono);
    font-size: 11px;
    font-weight: 600;
    color: var(--fg);
    border: 1px solid var(--line-strong);
    border-bottom-width: 2px;
    border-radius: 4px;
    padding: 1px 5px;
    background: var(--bg);
}

.help-content h2 {
    display: flex;
    align-items: center;
    gap: 10px;
}

.help-hint {
    margin-left: auto;
    font-size: 12px;
    font-weight: 400;
    color: var(--muted);
    white-space: nowrap;
}

/* -------------------------------------------------------------- scrollbar */

::-webkit-scrollbar {
    width: 10px;
    height: 10px;
}

::-webkit-scrollbar-track {
    background: transparent;
}

::-webkit-scrollbar-thumb {
    background: var(--line-strong);
    border-radius: 5px;
    border: 2px solid transparent;
    background-clip: padding-box;
}

::-webkit-scrollbar-thumb:hover {
    background: var(--muted);
    background-clip: padding-box;
}

/* ------------------------------------------------------------- responsive */

@media (max-width: 768px) {
    .sidebar {
        display: none;
    }

    .file-grid {
        grid-template-columns: repeat(auto-fill, minmax(104px, 1fr));
    }

    .file-grid.list-view .file-item,
    .list-head {
        grid-template-columns: 36px minmax(0, 1fr) 90px 32px;
    }

    .file-grid.list-view .file-size { grid-column: 3; }
    .file-grid.list-view .fav-toggle { grid-column: 4; }

    .file-grid.list-view .file-kind,
    .file-grid.list-view .file-date,
    .list-head span:nth-child(3),
    .list-head span:nth-child(5) {
        display: none;
    }
}'''


def get_app_js() -> str:
    """Return JavaScript."""
    return '''// State
let currentPath = '.';
let currentPage = 1;
let perPage = 50;
let totalPages = 1;
let totalFiles = 0;
let viewMode = 'grid';
let sortBy = 'name';
let sortOrder = 'asc';
let media = [];  // images and videos combined
let currentImageIndex = 0;
const loadedTreePaths = new Set(); // Track which tree nodes are loaded
let selectedIndex = -1; // Currently selected item in grid for keyboard navigation
let focusedPanel = 'content'; // 'tree' or 'content' - which panel has keyboard focus
let focusedTreeIndex = -1; // Currently focused tree item index

// Sorting / filtering state
let filterText = ''; // Current filter text
let allItems = []; // All items in current directory (for filtering)

// Import/management state
let currentView = 'browse';
let serverConfig = {root: '.', import_enabled: false};
let scanMedia = 'photo';
let copyMedia = 'photo';
let batchesMedia = 'photo';
let watchedJobId = null;      // job shown in the job bar
let dismissedJobId = null;    // job the user hid
let jobPollTimer = null;
let dirPickerInputId = null;
let dirPickerPath = null;
let favoriteItems = [];
let currentMediaPath = null;
let conflictContext = null;   // {media, batchId}
let conflictPage = 1;
let storagePath = '.';
let storageParent = null;
let recursiveMode = false;   // V: every photo below the current folder
let batchView = null;        // B: {media, id} when the grid shows one import
let batchNeedsRefresh = false; // ask the server to re-read the batch, not its cache
let batchIndex = [];         // imports offered in the picker, newest first
let gridMode = 'folder';     // what the grid holds right now: folder | recursive | batch
let treeShown = true;        // T: the folder tree on the left
let jobOpen = false;         // the job pill's popover
let sizePath = '.';          // folder shown in the size dialog
let sizeParent = null;

// DOM Elements
const treeEl = document.getElementById('tree');
const sidebarEl = document.getElementById('sidebar');
const fileGridEl = document.getElementById('file-grid');
const contentEl = document.querySelector('.content');
const breadcrumbEl = document.getElementById('breadcrumb');
const fileCountEl = document.getElementById('file-count');
const paginationEl = document.getElementById('pagination');
const perPageSelect = document.getElementById('per-page-select');
const sortSelect = document.getElementById('sort-select');
const sortOrderBtn = document.getElementById('btn-sort-order');
const lightboxEl = document.getElementById('lightbox');
const lightboxImgEl = document.getElementById('lightbox-img');
const lightboxInfoEl = document.getElementById('lightbox-info');
const filterInput = document.getElementById('filter-input');
const batchSelect = document.getElementById('batch-select');
const batchBarEl = document.getElementById('batch-bar');
const recursiveChipEl = document.getElementById('recursive-chip');
const statusLeftEl = document.getElementById('status-left');
const jobPillEl = document.getElementById('job-pill');
const headerProgressEl = document.getElementById('header-progress');
const clearFilterBtn = document.getElementById('btn-clear-filter');
const jobBarEl = document.getElementById('job-bar');
const batchListEl = document.getElementById('batch-list');
const jobHistoryEl = document.getElementById('job-history');
const favoritesGridEl = document.getElementById('favorites-grid');
const lightboxFavEl = document.getElementById('lightbox-fav');
const volumeListEl = document.getElementById('volume-list');
const dirModalEl = document.getElementById('dir-modal');
const sizeModalEl = document.getElementById('size-modal');
const dirListEl = document.getElementById('dir-list');
const dirPathEl = document.getElementById('dir-path');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadTreeNode('.'); // Load root only
    loadDirectory('.', 1);

    // Appearance
    applyTheme(storedTheme() || 'dark');
    try {
        treeShown = localStorage.getItem('photoBrowserTree') !== '0';
    } catch (err) { /* keep the default */ }
    applyTreeShown();
    document.getElementById('btn-theme').addEventListener('click', toggleTheme);
    document.getElementById('btn-tree').addEventListener('click', toggleTree);
    document.getElementById('btn-size').addEventListener('click', toggleSizeDialog);

    // The job pill opens the detail popover
    jobPillEl.addEventListener('click', () => {
        jobOpen = !jobOpen;
        jobBarEl.hidden = !jobOpen;
    });

    recursiveChipEl.addEventListener('click', (e) => {
        if (e.target.closest('[data-exit-recursive]')) toggleRecursive(false);
    });

    document.getElementById('btn-recursive').addEventListener('click', () => toggleRecursive());
    batchSelect.addEventListener('change', () => {
        const value = batchSelect.value;
        if (!value) {
            exitBatchView();
            return;
        }
        const [mediaType, id] = value.split(':');
        enterBatchView(mediaType, parseInt(id, 10));
    });
    batchBarEl.addEventListener('click', (e) => {
        if (e.target.closest('[data-exit-batch]')) exitBatchView();
    });
    document.getElementById('btn-grid').addEventListener('click', () => setViewMode('grid'));
    document.getElementById('btn-list').addEventListener('click', () => setViewMode('list'));
    document.getElementById('lightbox-close').addEventListener('click', closeLightbox);
    document.getElementById('lightbox-prev').addEventListener('click', prevImage);
    document.getElementById('lightbox-next').addEventListener('click', nextImage);
    lightboxFavEl.addEventListener('click', (e) => {
        e.stopPropagation();
        toggleFavorite(currentMediaPath);
    });

    // Size dialog
    document.getElementById('size-close').addEventListener('click', closeSizeDialog);
    document.getElementById('size-rescan').addEventListener('click', () => loadSizeDialog(true));
    document.getElementById('size-up').addEventListener('click', () => {
        if (sizeParent) openSizeDialog(sizeParent);
    });
    document.getElementById('size-open').addEventListener('click', () => {
        closeSizeDialog();
        setView('storage');
        loadStorage(sizePath);
    });
    document.getElementById('size-table').addEventListener('click', (e) => {
        const link = e.target.closest('[data-size-path]');
        if (link) openSizeDialog(link.dataset.sizePath);
    });
    sizeModalEl.addEventListener('click', (e) => {
        if (e.target === sizeModalEl) closeSizeDialog();
    });

    // Favorites view
    document.getElementById('btn-refresh-favorites').addEventListener('click', loadFavorites);
    document.getElementById('btn-prune-favorites').addEventListener('click', pruneFavorites);

    // Storage view
    document.getElementById('btn-storage-refresh').addEventListener('click', () => loadStorage(storagePath, true));
    document.getElementById('btn-storage-up').addEventListener('click', () => {
        if (storageParent) loadStorage(storageParent);
    });
    document.getElementById('btn-storage-browse').addEventListener('click', () => {
        setView('browse');
        navigateTo(storagePath);
    });
    document.getElementById('storage-table').addEventListener('click', (e) => {
        const link = e.target.closest('[data-storage-path]');
        if (link) loadStorage(link.dataset.storagePath);
    });

    // Conflicts view
    document.getElementById('btn-back-batches').addEventListener('click', () => setView('batches'));
    document.getElementById('conflict-list').addEventListener('click', (e) => {
        const btn = e.target.closest('button[data-action]');
        if (btn) resolveConflicts(btn.dataset.action, [parseInt(btn.dataset.id, 10)]);
    });
    document.querySelectorAll('[data-bulk]').forEach(btn => {
        btn.addEventListener('click', () => resolveConflicts(btn.dataset.bulk));
    });
    document.getElementById('conflict-pagination').addEventListener('click', (e) => {
        const btn = e.target.closest('.page-btn');
        if (btn && !btn.disabled) {
            conflictPage = parseInt(btn.dataset.cpage, 10);
            loadConflicts();
        }
    });

    // Per page selector
    perPageSelect.addEventListener('change', (e) => {
        perPage = parseInt(e.target.value);
        loadDirectory(currentPath, 1); // Reset to page 1
    });

    // Import / batches / tools UI
    initManage();

    // Sort selector
    sortSelect.addEventListener('change', (e) => {
        sortBy = e.target.value;
        loadDirectory(currentPath, 1);
    });

    // Sort order button
    sortOrderBtn.addEventListener('click', toggleSortOrder);

    // Help button click
    document.getElementById('btn-help').addEventListener('click', toggleShortcuts);

    // Help overlay click to close
    document.getElementById('help-overlay').addEventListener('click', (e) => {
        if (e.target.id === 'help-overlay') {
            toggleShortcuts();
        }
    });

    // Filter input
    filterInput.addEventListener('input', (e) => {
        filterText = e.target.value.toLowerCase();
        clearFilterBtn.style.display = filterText ? 'inline-block' : 'none';
        applyFilter();
    });

    filterInput.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            clearFilter();
            filterInput.blur();
        }
    });

    clearFilterBtn.addEventListener('click', clearFilter);

    // Keyboard navigation
    document.addEventListener('keydown', (e) => {
        // Handle filter input specially
        if (e.target === filterInput) return;

        if (dirModalEl.classList.contains('active')) {
            if (e.key === 'Escape') closeDirPicker();
            return;
        }

        if (sizeModalEl.classList.contains('active')) {
            if (e.key === 'Escape' || e.key === 's' || e.key === 'S') closeSizeDialog();
            return;
        }

        // Help overlay works from any view
        const helpOverlay = document.getElementById('help-overlay');
        if (helpOverlay.classList.contains('active')) {
            if (e.key === 'Escape' || e.key === '?' || (e.shiftKey && e.key === '/')) {
                e.preventDefault();
                toggleShortcuts();
            }
            return;
        }
        if ((e.key === '?' || (e.shiftKey && e.key === '/')) && !isTypingTarget(e.target)) {
            e.preventDefault();
            toggleShortcuts();
            return;
        }

        if (lightboxEl.classList.contains('active')) {
            if (e.key === 'Escape') closeLightbox();
            if (e.key === 'ArrowLeft') prevImage();
            if (e.key === 'ArrowRight') nextImage();
            if (e.key === '*') toggleFavorite(currentMediaPath);
            return;
        }

        if (currentView !== 'browse' && currentView !== 'favorites') return;
        if (isTypingTarget(e.target)) return;

        // Star favorites the selected item in either grid
        if (e.key === '*') {
            const grid = currentView === 'favorites' ? favoritesGridEl : fileGridEl;
            const selected = grid.querySelectorAll('.file-item')[selectedIndex];
            const star = selected && selected.querySelector('.fav-toggle');
            if (star) {
                e.preventDefault();
                toggleFavorite(star.dataset.fav);
            }
            return;
        }

        // The rest drives the browse grid only
        if (currentView === 'favorites') return;

        const key = e.key.toLowerCase();

        if (key === 'g') {
            e.preventDefault();
            setViewMode('grid');
            return;
        }
        if (key === 'l') {
            e.preventDefault();
            setViewMode('list');
            return;
        }
        if (key >= '1' && key <= '4') {
            e.preventDefault();
            const values = ['25', '50', '100', '200'];
            const idx = parseInt(key) - 1;
            perPageSelect.value = values[idx];
            perPage = parseInt(values[idx]);
            loadDirectory(currentPath, 1);
            return;
        }
        if (key === 'f') {
            e.preventDefault();
            filterInput.focus();
            filterInput.select();
            return;
        }
        if (key === '[' || (e.shiftKey && key === ',')) {
            e.preventDefault();
            if (currentPage > 1) loadDirectory(currentPath, currentPage - 1);
            return;
        }
        if (key === ']' || (e.shiftKey && key === '.')) {
            e.preventDefault();
            if (currentPage < totalPages) loadDirectory(currentPath, currentPage + 1);
            return;
        }
        if (e.key === 'Escape') {
            e.preventDefault();
            clearFilter();
            return;
        }
        // Sort shortcuts
        if (key === 'n') {
            e.preventDefault();
            setSort('name');
            return;
        }
        if (key === 'm') {
            e.preventDefault();
            setSort('modified');
            return;
        }
        if (key === 'c') {
            e.preventDefault();
            setSort('created');
            return;
        }
        if (key === 'a') {
            e.preventDefault();
            setSort('accessed');
            return;
        }
        if (key === 'z') {
            e.preventDefault();
            setSort('size');
            return;
        }
        if (key === 'o') {
            e.preventDefault();
            toggleSortOrder();
            return;
        }
        if (key === 'v') {
            e.preventDefault();
            toggleRecursive();
            return;
        }
        if (key === 'b') {
            e.preventDefault();
            if (batchView) {
                exitBatchView();
            } else if (serverConfig.import_enabled) {
                batchSelect.focus();
                if (batchSelect.showPicker) {
                    try { batchSelect.showPicker(); } catch (err) { /* not allowed here */ }
                }
            }
            return;
        }
        if (key === 's') {
            e.preventDefault();
            toggleSizeDialog();
            return;
        }
        if (key === 't') {
            e.preventDefault();
            toggleTree();
            return;
        }

        // Tab to switch between panels
        if (e.key === 'Tab') {
            e.preventDefault();
            switchFocusedPanel();
            return;
        }

        // Route to the panel that has focus
        if (focusedPanel === 'tree') {
            handleTreeKeyNavigation(e);
        } else {
            handleGridKeyNavigation(e);
        }
    });

    // Initialize panel focus
    setFocusedPanel('content');

    // Click outside image to close
    lightboxEl.addEventListener('click', (e) => {
        if (e.target === lightboxEl || e.target.classList.contains('lightbox-content')) {
            closeLightbox();
        }
    });
});

// ---------------------------------------------------------------------------
// Appearance: light / dark, and whether the folder tree is on screen
// ---------------------------------------------------------------------------

// Read before anything paints, so a light library never flashes dark
function storedTheme() {
    try {
        return localStorage.getItem('photoBrowserTheme');
    } catch (err) {
        return null;   // private window, blocked storage - the default is fine
    }
}

function applyTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    const btn = document.getElementById('btn-theme');
    if (btn) btn.title = theme === 'dark' ? 'Switch to light' : 'Switch to dark';
}

function toggleTheme() {
    const next = document.documentElement.getAttribute('data-theme') === 'light' ? 'dark' : 'light';
    applyTheme(next);
    try {
        localStorage.setItem('photoBrowserTheme', next);
    } catch (err) { /* nothing to persist to */ }
}

function applyTreeShown() {
    if (sidebarEl) sidebarEl.hidden = !treeShown;
    const btn = document.getElementById('btn-tree');
    if (btn) btn.classList.toggle('active', treeShown);
    if (!treeShown && focusedPanel === 'tree') setFocusedPanel('content');
}

function toggleTree() {
    treeShown = !treeShown;
    applyTreeShown();
    try {
        localStorage.setItem('photoBrowserTree', treeShown ? '1' : '0');
    } catch (err) { /* nothing to persist to */ }
}

// Load a single tree node (lazy loading)
async function loadTreeNode(path) {
    if (loadedTreePaths.has(path)) return;

    try {
        const res = await fetch(`/api/tree?path=${encodeURIComponent(path)}`);
        const data = await res.json();

        if (data.error) return;

        loadedTreePaths.add(path);

        if (path === '.') {
            // Root level - render directly into tree
            treeEl.innerHTML = renderTreeChildren(data.children);
        } else {
            // Find the parent folder element and append children
            const folderEl = document.querySelector(`.tree-folder[data-path="${CSS.escape(path)}"]`);
            if (folderEl) {
                let childrenEl = folderEl.nextElementSibling;
                if (!childrenEl || !childrenEl.classList.contains('tree-children')) {
                    childrenEl = document.createElement('div');
                    childrenEl.className = 'tree-children';
                    folderEl.parentNode.insertBefore(childrenEl, folderEl.nextSibling);
                }
                childrenEl.innerHTML = renderTreeChildren(data.children);
            }
        }
    } catch (err) {
        console.error('Failed to load tree node:', err);
    }
}

// Render tree children (not recursive - lazy loaded)
function renderTreeChildren(children) {
    if (!children || children.length === 0) return '<div class="tree-empty">No subdirectories</div>';

    let html = '';
    for (const child of children) {
        const hasChildrenClass = child.has_children ? 'has-children' : '';
        html += `<div class="tree-item">`;
        html += `<div class="tree-folder ${hasChildrenClass}" data-path="${escapeHtml(child.path)}">${escapeHtml(child.name)}</div>`;
        if (child.has_children) {
            html += `<div class="tree-children"></div>`;
        }
        html += `</div>`;
    }
    return html;
}

// Tree click handler
treeEl.addEventListener('click', async (e) => {
    const folder = e.target.closest('.tree-folder');
    if (!folder) return;

    const path = folder.dataset.path;

    // Load children if not loaded yet
    if (folder.classList.contains('has-children') && !loadedTreePaths.has(path)) {
        folder.classList.add('loading');
        await loadTreeNode(path);
        folder.classList.remove('loading');
    }

    // Toggle folder open state
    folder.classList.toggle('open');

    // Navigate to folder, landing where we left off
    navigateTo(path);

    // Update active state
    document.querySelectorAll('.tree-folder.active').forEach(el => el.classList.remove('active'));
    folder.classList.add('active');
});

// Load directory contents with pagination
async function loadDirectory(path, page = 1, restore = null) {
    rememberDirState();   // so coming back to where we are now lands in the same spot

    currentPath = path;
    currentPage = page;
    resetSelection(); // Reset keyboard selection when changing directory
    fileGridEl.innerHTML = '<div class="loading-indicator">Loading...</div>';
    paginationEl.innerHTML = '';

    try {
        const common = `page=${page}&per_page=${perPage}&sort=${sortBy}&order=${sortOrder}`;
        let url;
        if (batchView) {
            // One import, flattened - the folder we came from is kept for the way back
            url = `/api/batch-media?media=${batchView.media}&id=${batchView.id}&${common}`;
            if (batchNeedsRefresh) {
                url += '&refresh=1';
                batchNeedsRefresh = false;
            }
        } else {
            const endpoint = recursiveMode ? '/api/all' : '/api/list';
            url = `${endpoint}?path=${encodeURIComponent(path)}&${common}`;
        }

        const res = await fetch(url);
        const data = await res.json();

        if (data.error) {
            fileGridEl.innerHTML = `<div class="error">${escapeHtml(data.error)}</div>`;
            return;
        }

        // Update pagination state
        const pag = data.pagination;
        totalPages = pag.total_pages;
        totalFiles = pag.total_files;

        // Store all items for filtering
        allItems = data.items;
        gridMode = batchView ? 'batch' : (recursiveMode ? 'recursive' : 'folder');

        if (batchView) {
            batchView.info = data.batch;
            renderBatchBar(data.batch);
            renderBatchBreadcrumb(data.batch);
        } else {
            renderBatchBar(null);
            renderBreadcrumb(path);
        }
        applyFilter(); // This will render files with current filter
        renderPagination(pag);

        // Update file count
        updateFileCount(pag);

        if (restore) {
            // Coming back to a folder we have seen before
            if (restore.selectedIndex >= 0) highlightItem(restore.selectedIndex);
            fileGridEl.scrollTop = restore.scrollTop || 0;
        } else {
            fileGridEl.scrollTop = 0;
        }

    } catch (err) {
        console.error('Failed to load directory:', err);
        fileGridEl.innerHTML = `<div class="error">Failed to load directory</div>`;
    }
}

// Key events can land on the document itself, which has no closest()
function isTypingTarget(target) {
    return !!(target && typeof target.closest === 'function' && target.closest('input, select, textarea'));
}

// V: flatten everything below the current folder into one thumbnail grid
function toggleRecursive(force) {
    const wanted = force === undefined ? !recursiveMode : force;
    if (wanted === recursiveMode) return;

    exitBatchView(false);   // the two flat views are alternatives, not layers

    if (wanted) rememberDirState();   // keep the folder's own spot for the way back
    recursiveMode = wanted;
    document.getElementById('btn-recursive').classList.toggle('active', recursiveMode);
    contentEl.classList.toggle('recursive', recursiveMode);
    recursiveChipEl.hidden = !recursiveMode;

    // Leaving the flat view returns to where we were in the folder itself
    if (recursiveMode) {
        loadDirectory(currentPath, 1);
    } else {
        const saved = dirState.get(currentPath);
        loadDirectory(currentPath, saved ? saved.page : 1, saved || null);
    }
}

// Where we were in each folder we have visited: page, scroll and selection
const dirState = new Map();

function rememberDirState() {
    if (!currentPath || gridMode !== 'folder') return;
    dirState.set(currentPath, {
        page: currentPage,
        scrollTop: fileGridEl.scrollTop,
        selectedIndex: selectedIndex,
    });
}

// Navigate to a folder, landing where we left off if we have been there
function navigateTo(path) {
    exitBatchView(false);   // a folder was asked for, so stop showing one import
    if (recursiveMode) {
        recursiveMode = false;
        document.getElementById('btn-recursive').classList.remove('active');
        contentEl.classList.remove('recursive');
        recursiveChipEl.hidden = true;
    }
    const saved = dirState.get(path);
    loadDirectory(path, saved ? saved.page : 1, saved || null);
}

// Reload the current folder without losing the spot
function reloadCurrentDirectory() {
    rememberDirState();
    navigateTo(currentPath);
}

// Select an item without scrolling it into view - the caller restores scroll
function highlightItem(index) {
    const items = fileGridEl.querySelectorAll('.file-item');
    if (index < 0 || index >= items.length) return;

    items.forEach(item => item.classList.remove('selected'));
    selectedIndex = index;
    items[index].classList.add('selected');
}

// What the grid holds, in two places: a short count next to the crumbs and
// the fuller sentence in the status bar
function updateFileCount(pag) {
    const total = pag ? pag.total_files : allItems.filter(i => !i.is_dir).length;
    const filtered = filterText ? getFilteredItems().length : null;

    if (batchView) {
        setCounts(
            filterText ? `${filtered} of ${total.toLocaleString()}` : `${total.toLocaleString()} files`,
            filterText
                ? `Filter: ${filtered} of ${total.toLocaleString()} files in this import`
                : `Files from this import: ${total.toLocaleString()}`,
            pag);
        return;
    }

    if (recursiveMode) {
        const where = currentPath === '.' ? 'the library' : currentPath;
        setCounts(
            filterText ? `${filtered} of ${total.toLocaleString()}` : `${total.toLocaleString()} files`,
            filterText
                ? `Filter: ${filtered} of ${total.toLocaleString()} below ${where}`
                : `All photos below ${where}: ${total.toLocaleString()}`,
            pag);
        return;
    }

    const dirCount = pag ? pag.total_dirs : allItems.filter(i => i.is_dir).length;
    const folders = dirCount === 1 ? '1 folder' : `${dirCount.toLocaleString()} folders`;
    const files = total === 1 ? '1 file' : `${total.toLocaleString()} files`;

    if (filterText) {
        setCounts(`${filtered} of ${total.toLocaleString()}`, `Filter: ${filtered} matches`, pag);
        return;
    }

    setCounts(dirCount ? `${folders}, ${files}` : files, `${folders}, ${files}`, pag);
}

// The status bar also says which slice of the files is on screen
function setCounts(shortText, longText, pag) {
    fileCountEl.textContent = shortText;
    if (!statusLeftEl) return;

    if (!filterText && pag && pag.total_pages > 1) {
        const from = (pag.page - 1) * pag.per_page + 1;
        const to = Math.min(pag.page * pag.per_page, pag.total_files);
        statusLeftEl.textContent = `${longText} - showing ${from.toLocaleString()}-${to.toLocaleString()}`;
        return;
    }

    statusLeftEl.textContent = longText;
}

// Get filtered items
function getFilteredItems() {
    if (!filterText) return allItems;
    return allItems.filter(item => item.name.toLowerCase().includes(filterText));
}

// Apply current filter
function applyFilter() {
    const filtered = getFilteredItems();
    renderFiles(filtered);

    // Update media for lightbox - a file gone from disk has nothing to show
    media = filtered.filter(i => !i.missing && (i.is_image || i.is_video));

    // Update count display
    updateFileCount(null);
}

// Clear filter
function clearFilter() {
    filterText = '';
    filterInput.value = '';
    clearFilterBtn.style.display = 'none';
    applyFilter();
}

// ---------------------------------------------------------------------------
// Browsing one import: the files a batch put in the library, by date
// ---------------------------------------------------------------------------

async function loadBatchIndex() {
    if (!serverConfig.import_enabled) return;
    try {
        const data = await api('GET', '/api/batch-index');
        batchIndex = data.batches || [];
    } catch (err) {
        console.error('Failed to load imports:', err);
        batchIndex = [];
    }
    renderBatchOptions();
}

function batchOptionLabel(batch) {
    const when = formatBatchDate(batch.imported_at);
    const kind = batch.media === 'video' ? 'videos' : 'photos';
    const parts = (batch.source_directory || '').split('/').filter(Boolean);
    const source = parts.length ? parts[parts.length - 1] : batch.source_directory;
    return `#${batch.id} \u00b7 ${when} \u00b7 ${(batch.copied || 0).toLocaleString()} ${kind} \u00b7 ${source}`;
}

function formatBatchDate(value) {
    return (value || '').replace('T', ' ').slice(0, 16) || 'unknown date';
}

function renderBatchOptions() {
    const selected = batchView ? `${batchView.media}:${batchView.id}` : '';
    let html = '<option value="">Imports</option>';

    for (const batch of batchIndex) {
        const value = `${batch.media}:${batch.id}`;
        // A batch that landed somewhere else has nothing to show in this library
        const label = batchOptionLabel(batch) + (batch.in_root ? '' : ' (other library)');
        html += `<option value="${escapeHtml(value)}"${value === selected ? ' selected' : ''}`
             + `${batch.in_root ? '' : ' disabled'}>${escapeHtml(label)}</option>`;
    }

    batchSelect.innerHTML = html;
    batchSelect.classList.toggle('active', !!batchView);
}

function enterBatchView(mediaType, batchId) {
    if (recursiveMode) {
        recursiveMode = false;
        document.getElementById('btn-recursive').classList.remove('active');
        contentEl.classList.remove('recursive');
        recursiveChipEl.hidden = true;
    } else {
        rememberDirState();   // keep the folder's own spot for the way back
    }

    batchView = {media: mediaType, id: batchId};
    batchNeedsRefresh = true;
    renderBatchOptions();
    if (!batchIndex.some(b => b.media === mediaType && b.id === batchId)) {
        loadBatchIndex();   // picked from elsewhere - make sure the list knows it
    }

    clearFilter();
    loadDirectory(currentPath, 1);
}

function exitBatchView(reload = true) {
    if (!batchView) return;

    batchView = null;
    batchSelect.value = '';
    batchSelect.classList.remove('active');
    renderBatchBar(null);

    if (reload) {
        const saved = dirState.get(currentPath);
        loadDirectory(currentPath, saved ? saved.page : 1, saved || null);
    }
}

// The chip in the path bar: which import this is, and what it could not show
function renderBatchBar(info) {
    if (!info) {
        batchBarEl.hidden = true;
        batchBarEl.innerHTML = '';
        return;
    }

    const kind = info.media === 'video' ? 'videos' : 'photos';
    const paths = `${info.source_directory} \u2192 ${info.target_directory}`;

    let html = `<b>Import #${info.id}</b>`;
    html += `<span class="mono">${escapeHtml(formatBatchDate(info.imported_at))}</span>`;
    html += `<span class="mono">${(info.shown || 0).toLocaleString()} ${kind}</span>`;

    // Files the import put somewhere this grid cannot reach
    const gaps = [];
    const detail = [];
    if (info.missing) {
        gaps.push(`${info.missing.toLocaleString()} missing`);
        detail.push(`${info.missing.toLocaleString()} no longer on disk`);
    }
    if (info.outside) {
        gaps.push(`${info.outside.toLocaleString()} elsewhere`);
        detail.push(`${info.outside.toLocaleString()} outside the served folder`);
    }
    if (gaps.length) {
        html += `<span class="mono warn" title="${escapeHtml(detail.join(' - '))}">${escapeHtml(gaps.join(' - '))}</span>`;
    }

    html += `<span class="chip-paths" title="${escapeHtml(paths)}">${escapeHtml(paths)}</span>`;
    html += `<button class="chip-close" data-exit-batch="1" title="Show all folders">\u00d7</button>`;

    batchBarEl.innerHTML = html;
    batchBarEl.hidden = false;
}

function renderBatchBreadcrumb(info) {
    breadcrumbEl.innerHTML = `<a href="#" data-path=".">${escapeHtml(libraryName())}</a>`;
}

// The library's own folder name reads better than a generic "Home"
function libraryName() {
    const parts = (serverConfig.root || '').split('/').filter(Boolean);
    const name = parts.length ? parts[parts.length - 1] : '';
    return name && name !== '.' ? name : 'Home';
}

// Render breadcrumb navigation
function renderBreadcrumb(path) {
    const parts = path === '.' ? [] : path.split('/');
    let html = `<a href="#" data-path=".">${escapeHtml(libraryName())}</a>`;

    let currentPath = '';
    for (const part of parts) {
        currentPath += (currentPath ? '/' : '') + part;
        html += `<span class="separator">\u203A</span>`;
        html += `<a href="#" data-path="${escapeHtml(currentPath)}">${escapeHtml(part)}</a>`;
    }

    breadcrumbEl.innerHTML = html;
}

// Breadcrumb click handler
breadcrumbEl.addEventListener('click', (e) => {
    if (e.target.tagName === 'A') {
        e.preventDefault();
        navigateTo(e.target.dataset.path);
    }
});

// Render pagination controls
function renderPagination(pag) {
    if (pag.total_pages <= 1) {
        paginationEl.innerHTML = '';
        return;
    }

    let html = '';

    // Previous button
    html += `<button class="page-btn" ${pag.page <= 1 ? 'disabled' : ''} data-page="${pag.page - 1}" data-shortcut="[" title="Previous page ([)">\u2039</button>`;

    // Page numbers with ellipsis
    const maxVisible = 7;
    const pages = [];

    if (pag.total_pages <= maxVisible) {
        // Show all pages
        for (let i = 1; i <= pag.total_pages; i++) pages.push(i);
    } else {
        // Show first, last, and pages around current
        pages.push(1);

        let start = Math.max(2, pag.page - 2);
        let end = Math.min(pag.total_pages - 1, pag.page + 2);

        // Adjust if near start or end
        if (pag.page <= 3) {
            end = Math.min(5, pag.total_pages - 1);
        } else if (pag.page >= pag.total_pages - 2) {
            start = Math.max(2, pag.total_pages - 4);
        }

        if (start > 2) pages.push('...');
        for (let i = start; i <= end; i++) pages.push(i);
        if (end < pag.total_pages - 1) pages.push('...');

        pages.push(pag.total_pages);
    }

    for (const p of pages) {
        if (p === '...') {
            html += `<span class="page-ellipsis">...</span>`;
        } else {
            const active = p === pag.page ? 'active' : '';
            html += `<button class="page-btn ${active}" data-page="${p}">${p}</button>`;
        }
    }

    // Next button
    html += `<button class="page-btn" ${pag.page >= pag.total_pages ? 'disabled' : ''} data-page="${pag.page + 1}" data-shortcut="]" title="Next page (])">\u203A</button>`;

    paginationEl.innerHTML = html;
}

// Pagination click handler
paginationEl.addEventListener('click', (e) => {
    const btn = e.target.closest('.page-btn');
    if (!btn || btn.disabled) return;

    const page = parseInt(btn.dataset.page);
    if (page >= 1 && page <= totalPages) {
        loadDirectory(currentPath, page);
    }
});

// Render files
function renderFiles(items, targetEl) {
    const grid = targetEl || fileGridEl;
    let html = '';

    // The list view is a table, so it gets a header row the grid never shows
    if (grid === fileGridEl && items.length) {
        html += `<div class="list-head">`
            + `<span></span><span>Name</span><span>Kind</span>`
            + `<span>Size</span><span>Modified</span><span></span></div>`;
    }

    for (const item of items) {
        html += renderFileItem(item);
    }

    grid.innerHTML = html || '<div class="empty">Nothing here.</div>';
}

// Columns the list view shows and the grid hides
function fileColumns(item) {
    if (item.is_dir) {
        return `<span class="file-kind">Folder</span><span class="file-size"></span>`
             + `<span class="file-date">${escapeHtml(formatStamp(item.modified))}</span>`;
    }

    const kind = item.is_video ? 'Video'
        : item.is_raw ? ((item.extension || '').replace('.', '').toUpperCase() || 'RAW')
        : item.is_image ? ((item.extension || '').replace('.', '').toUpperCase() || 'Image')
        : 'File';

    return `<span class="file-kind">${escapeHtml(kind)}</span>`
         + `<span class="file-size">${item.missing ? '' : formatSize(item.size || 0)}</span>`
         + `<span class="file-date">${escapeHtml(formatStamp(item.modified))}</span>`;
}

// Seconds since the epoch, as the list view prints them
function formatStamp(seconds) {
    if (!seconds) return '';
    const date = new Date(seconds * 1000);
    if (isNaN(date.getTime())) return '';
    const pad = (n) => String(n).padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} `
         + `${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function renderFileItem(item) {
    const path = escapeHtml(item.path);
    const name = escapeHtml(item.name);
    const cols = fileColumns(item);

    if (item.is_dir) {
        return `
            <div class="file-item folder" data-path="${path}">
                <div class="file-icon">&#128193;</div>
                <div class="file-name">${name}</div>${cols}
            </div>
        `;
    }

    const star = `<button class="fav-toggle ${item.favorite ? 'on' : ''}" data-fav="${path}"
                          title="${item.favorite ? 'Remove from favorites' : 'Add to favorites'}"
                  >${item.favorite ? '\\u2605' : '\\u2606'}</button>`;
    const missing = item.missing ? ' missing' : '';

    // In the flat view a tile also says which folder it came from
    const sub = item.folder && item.folder !== currentPath
        ? `<div class="file-sub">${escapeHtml(item.folder)}</div>`
        : '';

    if (item.is_image) {
        const thumb = item.missing
            ? '<div class="file-icon">&#10071;</div>'
            : `<img class="file-thumb" src="/api/thumbnail/${encodeURIComponent(item.path)}" alt="${name}" loading="lazy">`;
        return `
            <div class="file-item image${missing}" data-path="${path}">
                ${thumb}${star}
                <div class="file-name">${name}</div>${sub}${cols}
            </div>
        `;
    }

    if (item.is_video) {
        return `
            <div class="file-item video${missing}" data-path="${path}">
                <div class="video-icon">&#9658;</div>${star}
                <div class="file-name">${name}</div>${sub}${cols}
            </div>
        `;
    }

    if (item.is_raw) {
        const kind = (item.extension || '').replace('.', '').toUpperCase();
        return `
            <div class="file-item raw${missing}" data-path="${path}">
                <div class="raw-icon">${escapeHtml(kind || 'RAW')}</div>${star}
                <div class="file-name">${name}</div>${sub}${cols}
            </div>
        `;
    }

    return `
        <div class="file-item${missing}" data-path="${path}">
            <div class="file-icon">&#128196;</div>${star}
            <div class="file-name">${name}</div>${cols}
        </div>
    `;
}

// File click handler
fileGridEl.addEventListener('click', (e) => {
    const star = e.target.closest('.fav-toggle');
    if (star) {
        e.stopPropagation();
        toggleFavorite(star.dataset.fav);
        return;
    }

    const item = e.target.closest('.file-item');
    if (!item || item.classList.contains('missing')) return;

    if (item.classList.contains('folder')) {
        navigateTo(item.dataset.path);
    } else if (item.classList.contains('image') || item.classList.contains('video')) {
        openLightbox(item.dataset.path);
    }
});

// View mode
function setViewMode(mode) {
    viewMode = mode;
    document.getElementById('btn-grid').classList.toggle('active', mode === 'grid');
    document.getElementById('btn-list').classList.toggle('active', mode === 'list');
    fileGridEl.classList.toggle('list-view', mode === 'list');
}

// Sort functions
function setSort(by) {
    sortBy = by;
    sortSelect.value = by;
    loadDirectory(currentPath, 1);
}

function toggleSortOrder() {
    sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
    sortOrderBtn.textContent = sortOrder === 'asc' ? '↑' : '↓';
    sortOrderBtn.title = sortOrder === 'asc' ? 'Ascending (O)' : 'Descending (O)';
    loadDirectory(currentPath, 1);
}

// Toggle keyboard shortcuts display
function toggleShortcuts() {
    const app = document.querySelector('.app');
    const helpBtn = document.getElementById('btn-help');
    const helpOverlay = document.getElementById('help-overlay');
    const isActive = helpOverlay.classList.toggle('active');
    app.classList.toggle('show-shortcuts', isActive);
    helpBtn.classList.toggle('active', isActive);
}

// Lightbox functions
const lightboxVideoEl = document.getElementById('lightbox-video');

function openLightbox(path) {
    currentImageIndex = media.findIndex(m => m.path === path);
    showMedia(path);
    lightboxEl.classList.add('active');
    document.body.style.overflow = 'hidden';
}

function closeLightbox() {
    lightboxEl.classList.remove('active');
    document.body.style.overflow = '';
    lightboxImgEl.src = '';
    lightboxImgEl.style.display = 'none';
    lightboxVideoEl.src = '';
    lightboxVideoEl.style.display = 'none';
    lightboxVideoEl.pause();
}

function showMedia(path) {
    const item = media.find(i => i.path === path);
    const isVideo = item && item.is_video;
    currentMediaPath = path;
    updateLightboxFavorite();

    // Stop any playing video
    lightboxVideoEl.pause();

    if (isVideo) {
        lightboxImgEl.style.display = 'none';
        lightboxVideoEl.style.display = 'block';
        lightboxVideoEl.src = `/photo/${encodeURIComponent(path)}`;
    } else {
        lightboxVideoEl.style.display = 'none';
        lightboxImgEl.style.display = 'block';
        lightboxImgEl.src = `/photo/${encodeURIComponent(path)}`;
    }

    const nameEl = document.getElementById('lightbox-name');
    const indexEl = document.getElementById('lightbox-index');
    if (nameEl) nameEl.textContent = item ? item.name : path.split('/').pop();
    if (indexEl) {
        indexEl.textContent = media.length
            ? `${currentImageIndex + 1} / ${media.length}`
            : '';
    }

    // Size, date and folder along the bottom
    const folder = path.split('/').slice(0, -1).join('/');
    const parts = [];
    if (item) parts.push(formatSize(item.size));
    if (item && item.modified) parts.push(formatStamp(item.modified));
    if (folder) parts.push(folder);
    lightboxInfoEl.textContent = parts.join('   \u00b7   ');
}

function prevImage() {
    if (media.length === 0) return;
    lightboxVideoEl.pause();
    currentImageIndex = (currentImageIndex - 1 + media.length) % media.length;
    showMedia(media[currentImageIndex].path);
}

function nextImage() {
    if (media.length === 0) return;
    lightboxVideoEl.pause();
    currentImageIndex = (currentImageIndex + 1) % media.length;
    showMedia(media[currentImageIndex].path);
}

// Grid keyboard navigation
function handleGridKeyNavigation(e) {
    const items = fileGridEl.querySelectorAll('.file-item');
    if (items.length === 0) return;

    // Don't handle if typing in an input
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT') return;

    const key = e.key;

    // Calculate grid columns for up/down navigation
    const getGridColumns = () => {
        if (viewMode === 'list') return 1;
        const gridStyle = window.getComputedStyle(fileGridEl);
        const columns = gridStyle.getPropertyValue('grid-template-columns').split(' ').length;
        return columns || 1;
    };

    switch (key) {
        case 'ArrowRight':
            e.preventDefault();
            selectItem(selectedIndex + 1, items);
            break;
        case 'ArrowLeft':
            e.preventDefault();
            selectItem(selectedIndex - 1, items);
            break;
        case 'ArrowDown':
            e.preventDefault();
            selectItem(selectedIndex + getGridColumns(), items);
            break;
        case 'ArrowUp':
            e.preventDefault();
            selectItem(selectedIndex - getGridColumns(), items);
            break;
        case 'Enter':
            e.preventDefault();
            activateSelectedItem(items);
            break;
        case 'Backspace':
            e.preventDefault();
            goToParentDirectory();
            break;
        case 'Home':
            e.preventDefault();
            selectItem(0, items);
            break;
        case 'End':
            e.preventDefault();
            selectItem(items.length - 1, items);
            break;
        case 'PageDown':
            e.preventDefault();
            if (currentPage < totalPages) {
                loadDirectory(currentPath, currentPage + 1);
            }
            break;
        case 'PageUp':
            e.preventDefault();
            if (currentPage > 1) {
                loadDirectory(currentPath, currentPage - 1);
            }
            break;
    }
}

function selectItem(index, items) {
    if (!items) items = fileGridEl.querySelectorAll('.file-item');
    if (items.length === 0) return;

    // Clamp index to valid range
    if (index < 0) index = 0;
    if (index >= items.length) index = items.length - 1;

    // Remove previous selection
    items.forEach(item => item.classList.remove('selected'));

    // Set new selection
    selectedIndex = index;
    const selectedItem = items[index];
    selectedItem.classList.add('selected');

    // Scroll into view if needed
    selectedItem.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
}

function activateSelectedItem(items) {
    if (!items) items = fileGridEl.querySelectorAll('.file-item');
    if (selectedIndex < 0 || selectedIndex >= items.length) return;

    const item = items[selectedIndex];
    if (item.classList.contains('missing')) return;

    if (item.classList.contains('folder')) {
        navigateTo(item.dataset.path);
    } else if (item.classList.contains('image') || item.classList.contains('video')) {
        openLightbox(item.dataset.path);
    }
}

function goToParentDirectory() {
    if (currentPath === '.') return;
    const parent = currentPath.split('/').slice(0, -1).join('/') || '.';
    navigateTo(parent);
}

// Reset selection when directory changes
function resetSelection() {
    selectedIndex = -1;
}

// Panel focus management
function setFocusedPanel(panel) {
    focusedPanel = panel;
    sidebarEl.classList.toggle('focused', panel === 'tree');
    contentEl.classList.toggle('focused', panel === 'content');

    // Clear focus indicators when switching panels
    if (panel === 'tree') {
        // Clear grid selection visual
        fileGridEl.querySelectorAll('.file-item.selected').forEach(el => el.classList.remove('selected'));
        // If no tree item focused, focus the first one
        if (focusedTreeIndex < 0) {
            selectTreeItem(0);
        } else {
            // Re-apply focus to current tree item
            selectTreeItem(focusedTreeIndex);
        }
    } else {
        // Clear tree focus visual
        treeEl.querySelectorAll('.tree-folder.focused').forEach(el => el.classList.remove('focused'));
        // If no grid item selected, select the first one
        const items = fileGridEl.querySelectorAll('.file-item');
        if (items.length > 0 && selectedIndex < 0) {
            selectItem(0, items);
        } else if (items.length > 0) {
            selectItem(selectedIndex, items);
        }
    }
}

function switchFocusedPanel() {
    // With the tree hidden (T) there is only one panel to drive
    if (!treeShown) {
        setFocusedPanel('content');
        return;
    }
    setFocusedPanel(focusedPanel === 'tree' ? 'content' : 'tree');
}

// Get all visible tree folders (flattened, respecting open/closed state)
function getVisibleTreeFolders() {
    const folders = [];

    function collectFolders(container) {
        const items = container.querySelectorAll(':scope > .tree-item');
        items.forEach(item => {
            const folder = item.querySelector(':scope > .tree-folder');
            if (folder) {
                folders.push(folder);
                // If folder is open, collect its children
                if (folder.classList.contains('open')) {
                    const children = item.querySelector(':scope > .tree-children');
                    if (children) {
                        collectFolders(children);
                    }
                }
            }
        });
    }

    collectFolders(treeEl);
    return folders;
}

// Tree keyboard navigation
function handleTreeKeyNavigation(e) {
    const folders = getVisibleTreeFolders();
    if (folders.length === 0) return;

    // Don't handle if typing in an input
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT') return;

    const key = e.key;
    const currentFolder = folders[focusedTreeIndex];

    switch (key) {
        case 'ArrowDown':
            e.preventDefault();
            selectTreeItem(focusedTreeIndex + 1, folders);
            break;
        case 'ArrowUp':
            e.preventDefault();
            selectTreeItem(focusedTreeIndex - 1, folders);
            break;
        case 'ArrowRight':
            e.preventDefault();
            if (currentFolder && currentFolder.classList.contains('has-children')) {
                if (!currentFolder.classList.contains('open')) {
                    // Expand the folder
                    expandTreeFolder(currentFolder);
                } else {
                    // Move to first child
                    selectTreeItem(focusedTreeIndex + 1, folders);
                }
            }
            break;
        case 'ArrowLeft':
            e.preventDefault();
            if (currentFolder) {
                if (currentFolder.classList.contains('open')) {
                    // Collapse the folder
                    currentFolder.classList.remove('open');
                } else {
                    // Move to parent folder
                    const parentPath = getParentPath(currentFolder.dataset.path);
                    if (parentPath) {
                        const parentIndex = folders.findIndex(f => f.dataset.path === parentPath);
                        if (parentIndex >= 0) {
                            selectTreeItem(parentIndex, folders);
                        }
                    }
                }
            }
            break;
        case 'Enter':
        case ' ':
            e.preventDefault();
            if (currentFolder) {
                // Toggle expand/collapse if folder has children
                if (currentFolder.classList.contains('has-children')) {
                    if (currentFolder.classList.contains('open')) {
                        currentFolder.classList.remove('open');
                    } else {
                        expandTreeFolder(currentFolder);
                    }
                }
                // Navigate to folder (load contents in right panel)
                navigateTo(currentFolder.dataset.path);
                // Update active state
                document.querySelectorAll('.tree-folder.active').forEach(el => el.classList.remove('active'));
                currentFolder.classList.add('active');
                // Keep focus on tree panel - don't switch to content
            }
            break;
        case 'Home':
            e.preventDefault();
            selectTreeItem(0, folders);
            break;
        case 'End':
            e.preventDefault();
            selectTreeItem(folders.length - 1, folders);
            break;
    }
}

async function expandTreeFolder(folder) {
    const path = folder.dataset.path;

    // Load children if not loaded yet
    if (folder.classList.contains('has-children') && !loadedTreePaths.has(path)) {
        folder.classList.add('loading');
        await loadTreeNode(path);
        folder.classList.remove('loading');
    }

    folder.classList.add('open');
}

function getParentPath(path) {
    if (!path || path === '.') return null;
    const parts = path.split('/');
    if (parts.length <= 1) return null;
    return parts.slice(0, -1).join('/') || null;
}

function selectTreeItem(index, folders) {
    if (!folders) folders = getVisibleTreeFolders();
    if (folders.length === 0) return;

    // Clamp index to valid range
    if (index < 0) index = 0;
    if (index >= folders.length) index = folders.length - 1;

    // Remove previous focus
    folders.forEach(f => f.classList.remove('focused'));

    // Set new focus
    focusedTreeIndex = index;
    const focusedFolder = folders[index];
    focusedFolder.classList.add('focused');

    // Scroll into view if needed
    focusedFolder.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
}

// Click handlers to set panel focus
sidebarEl.addEventListener('click', () => {
    setFocusedPanel('tree');
});

fileGridEl.addEventListener('click', () => {
    setFocusedPanel('content');
}, true);

// ===========================================================================
// Import tools - the CLI commands (scan / copy / retry / expand) in the UI
// ===========================================================================

let latestJobs = [];
let lastWatchedStatus = null;

async function initManage() {
    try {
        serverConfig = await api('GET', '/api/config');
    } catch (err) {
        console.error('Failed to load config:', err);
    }

    // Tabs (Browse and Favorites work even without the import tools)
    document.getElementById('tabs').addEventListener('click', (e) => {
        const tab = e.target.closest('.tab');
        if (tab) setView(tab.dataset.view);
    });

    updateFavoriteCount(serverConfig.favorites || 0);
    if (!batchView) renderBreadcrumb(currentPath);

    if (!serverConfig.import_enabled) {
        document.querySelectorAll('.import-only').forEach(el => el.classList.add('hidden'));
        return;
    }

    // Media selectors
    bindSegmented('scan-media', (media) => {
        scanMedia = media;
        // Checksums are slow for videos - the CLI defaults them off there
        document.getElementById('scan-checksum').checked = media === 'photo';
    });
    bindSegmented('quick-copy-media', (media) => { copyMedia = media; loadLatestBatch(); });
    bindSegmented('batches-media', (media) => { batchesMedia = media; loadBatches(); });

    // Actions
    document.getElementById('btn-scan').addEventListener('click', startScan);
    document.getElementById('btn-quick-copy').addEventListener('click', startQuickCopy);
    document.getElementById('btn-all-batches').addEventListener('click', () => setView('batches'));
    document.getElementById('btn-expand').addEventListener('click', startExpand);
    document.getElementById('btn-refresh-batches').addEventListener('click', loadBatches);
    document.getElementById('btn-refresh-jobs').addEventListener('click', () => renderJobHistory(latestJobs));
    document.getElementById('job-cancel').addEventListener('click', cancelWatchedJob);
    document.getElementById('job-dismiss').addEventListener('click', () => {
        dismissedJobId = watchedJobId;
        jobOpen = false;
        jobBarEl.hidden = true;
        jobPillEl.hidden = true;
        headerProgressEl.hidden = true;
    });

    const importHereBtn = document.getElementById('btn-import-here');
    if (importHereBtn) {
        importHereBtn.addEventListener('click', () => {
            document.getElementById('scan-source').value = currentAbsolutePath();
            setView('import');
        });
    }

    // Path helpers
    document.querySelectorAll('[data-pick]').forEach(btn => {
        btn.addEventListener('click', () => openDirPicker(btn.dataset.pick));
    });
    document.querySelectorAll('[data-fill-current]').forEach(btn => {
        btn.addEventListener('click', () => {
            document.getElementById(btn.dataset.fillCurrent).value = currentAbsolutePath();
            updateDestinationHint();
        });
    });
    document.querySelectorAll('[data-clear]').forEach(btn => {
        btn.addEventListener('click', () => {
            document.getElementById(btn.dataset.clear).value = '';
            updateDestinationHint();
        });
    });

    // Directory picker modal
    document.getElementById('dir-close').addEventListener('click', closeDirPicker);
    document.getElementById('dir-up').addEventListener('click', () => {
        const parent = dirModalEl.dataset.parent;
        if (parent) loadDirPicker(parent);
    });
    document.getElementById('dir-home').addEventListener('click', () => loadDirPicker(serverConfig.home));
    document.getElementById('dir-select').addEventListener('click', () => {
        if (dirPickerInputId && dirPickerPath) {
            document.getElementById(dirPickerInputId).value = dirPickerPath;
            updateDestinationHint();
        }
        closeDirPicker();
    });
    dirModalEl.addEventListener('click', (e) => {
        if (e.target === dirModalEl) closeDirPicker();
    });
    dirListEl.addEventListener('click', (e) => {
        const row = e.target.closest('.dir-row');
        if (row) loadDirPicker(row.dataset.path);
    });

    // Batch actions
    batchListEl.addEventListener('click', onBatchAction);

    // Mounted volumes quick pick for the scan source
    volumeListEl.addEventListener('click', onVolumeClick);
    document.getElementById('btn-refresh-volumes').addEventListener('click', loadVolumes);
    loadVolumes();

    // Imports offered in the Browse toolbar, newest first
    loadBatchIndex();

    // Defaults
    const targetInput = document.getElementById('scan-target');
    targetInput.value = serverConfig.root || '';
    targetInput.addEventListener('input', updateDestinationHint);
    updateDestinationHint();

    document.getElementById('scan-workers').placeholder = `auto (${serverConfig.default_workers})`;
    renderServerInfo();

    pollJobs();
}

function bindSegmented(id, onChange) {
    const container = document.getElementById(id);
    container.addEventListener('click', (e) => {
        const btn = e.target.closest('.seg-btn');
        if (!btn) return;
        container.querySelectorAll('.seg-btn').forEach(b => b.classList.toggle('active', b === btn));
        onChange(btn.dataset.media);
    });
}

function setView(view) {
    currentView = view;
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.view === view);
    });
    document.querySelectorAll('.view').forEach(el => {
        el.classList.toggle('active', el.id === `view-${view}`);
    });

    if (view === 'import') loadLatestBatch();
    if (view === 'batches') loadBatches();
    if (view === 'tools') renderJobHistory(latestJobs);
    if (view === 'favorites') loadFavorites();
    if (view === 'conflicts') loadConflicts();
    if (view === 'storage') loadStorage(storagePath);
}

async function api(method, path, body) {
    const options = {method, headers: {'Content-Type': 'application/json'}};
    if (body !== undefined) options.body = JSON.stringify(body);

    const res = await fetch(path, options);
    let data = null;
    try {
        data = await res.json();
    } catch (err) {
        data = null;
    }

    if (!res.ok) {
        throw new Error((data && data.error) || `Request failed (${res.status})`);
    }
    return data || {};
}

function currentAbsolutePath() {
    const root = serverConfig.root || '.';
    return currentPath === '.' ? root : `${root}/${currentPath}`;
}

// Spell out where the files will actually land
function updateDestinationHint() {
    const hintEl = document.getElementById('scan-dest');
    let base = document.getElementById('scan-target').value.trim();

    while (base.length > 1 && base.endsWith('/')) {
        base = base.slice(0, -1);
    }

    if (!base) {
        hintEl.textContent = '';
        return;
    }

    const library = base.split('/').pop() === 'organized_photos' ? base : `${base}/organized_photos`;
    hintEl.textContent = `Files go to ${library}/YYYY/MM/DD/`;
}

function showFormMessage(id, message, ok) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = message || '';
    el.className = ok ? 'form-ok' : 'form-error';
}

// ---------------------------------------------------------------------------
// Starting jobs
// ---------------------------------------------------------------------------

async function startScan() {
    const source = document.getElementById('scan-source').value.trim();
    const target = document.getElementById('scan-target').value.trim();
    const workersValue = document.getElementById('scan-workers').value.trim();

    if (!source || !target) {
        showFormMessage('scan-error', 'Source and target folders are required');
        return;
    }

    showFormMessage('scan-error', '');
    await runJob('scan-error', () => api('POST', '/api/scan', {
        media: scanMedia,
        source: source,
        target: target,
        checksum: document.getElementById('scan-checksum').checked,
        resume: document.getElementById('scan-resume').checked,
        workers: workersValue ? parseInt(workersValue, 10) : null,
    }));
}

async function startQuickCopy() {
    showFormMessage('copy-error', '');
    await runJob('copy-error', () => api('POST', '/api/copy', {
        media: copyMedia,
        dry_run: document.getElementById('copy-dry-run').checked,
        skip_no_date: document.getElementById('copy-skip-no-date').checked,
        use_file_date: document.getElementById('copy-use-file-date').checked,
    }));
}

async function startExpand() {
    const source = document.getElementById('expand-source').value.trim();
    if (!source) {
        showFormMessage('expand-error', 'Source folder is required');
        return;
    }

    showFormMessage('expand-error', '');
    await runJob('expand-error', () => api('POST', '/api/expand', {
        source: source,
        target: document.getElementById('expand-target').value.trim(),
        dry_run: document.getElementById('expand-dry-run').checked,
        move: document.getElementById('expand-move').checked,
    }));
}

async function runJob(errorElementId, request) {
    try {
        const job = await request();
        watchedJobId = job.id;
        dismissedJobId = null;
        lastWatchedStatus = job.status;
        renderJobBar(job);
        schedulePoll(400);
    } catch (err) {
        showFormMessage(errorElementId, err.message);
    }
}

async function cancelWatchedJob() {
    if (!watchedJobId) return;
    try {
        await api('POST', '/api/job/cancel', {id: watchedJobId});
    } catch (err) {
        console.error('Cancel failed:', err);
    }
    schedulePoll(300);
}

// ---------------------------------------------------------------------------
// Job polling & rendering
// ---------------------------------------------------------------------------

function schedulePoll(delay) {
    clearTimeout(jobPollTimer);
    jobPollTimer = setTimeout(pollJobs, delay);
}

async function pollJobs() {
    try {
        const data = await api('GET', '/api/jobs');
        latestJobs = data.jobs || [];

        const running = latestJobs.find(j => j.status === 'running');
        if (running && watchedJobId !== running.id) {
            watchedJobId = running.id;
            dismissedJobId = null;
        }

        const watched = latestJobs.find(j => j.id === watchedJobId);
        renderJobBar(watched);

        // A job just finished - refresh whatever the user is looking at
        if (watched && lastWatchedStatus === 'running' && watched.status !== 'running') {
            if (currentView === 'batches') loadBatches();
            if (currentView === 'conflicts') loadConflicts();
            refreshBrowseAfterJob();
        }
        lastWatchedStatus = watched ? watched.status : null;

        if (currentView === 'tools') renderJobHistory(latestJobs);

        schedulePoll(running ? 700 : 4000);
    } catch (err) {
        schedulePoll(5000);
    }
}

function refreshBrowseAfterJob() {
    loadBatchIndex();   // a copy may have created or grown an import

    // Newly copied files may have appeared in the served tree
    if (currentView === 'browse') {
        loadedTreePaths.clear();
        loadTreeNode('.');
        if (batchView) {
            batchNeedsRefresh = true;
            loadDirectory(currentPath, currentPage);
        } else {
            reloadCurrentDirectory();
        }
    }
}

function renderJobBar(job) {
    if (!job || job.id === dismissedJobId) {
        jobBarEl.hidden = true;
        jobPillEl.hidden = true;
        headerProgressEl.hidden = true;
        jobOpen = false;
        return;
    }

    const percent = job.status === 'running' ? job.percent : 100;

    // The pill is always there while a job is worth showing; the panel below
    // it only when asked for
    jobPillEl.hidden = false;
    document.getElementById('job-pill-title').textContent = job.title;
    document.getElementById('job-pill-fill').style.width = `${percent}%`;
    document.getElementById('job-pill-meta').textContent = job.status === 'running'
        ? `${percent}% - ${formatSeconds(job.elapsed_seconds)}`
        : job.status;

    headerProgressEl.hidden = job.status !== 'running';
    headerProgressEl.style.width = `${percent}%`;

    jobBarEl.hidden = !jobOpen;
    document.getElementById('job-title').textContent = job.title;

    const badge = document.getElementById('job-badge');
    badge.textContent = job.status;
    badge.className = `job-badge ${job.status}`;

    const running = job.status === 'running';
    document.getElementById('job-progress').style.width = `${running ? job.percent : 100}%`;
    document.getElementById('job-file').textContent = running
        ? (job.current_file || (job.total ? '' : 'Discovering files...'))
        : '';
    document.getElementById('job-counts').textContent = job.total
        ? `${job.current}/${job.total} (${job.percent}%) - ${formatSeconds(job.elapsed_seconds)}`
        : formatSeconds(job.elapsed_seconds);

    document.getElementById('job-cancel').style.display = running ? '' : 'none';
    document.getElementById('job-result').textContent = describeJobResult(job);

    // A job that just ended has something to say - open the panel once
    if (!running && lastWatchedStatus === 'running') {
        jobOpen = true;
        jobBarEl.hidden = false;
    }
}

function describeJobResult(job) {
    if (job.status === 'running') return '';
    if (job.status === 'failed') return `Error: ${job.error}`;
    if (job.status === 'cancelled') return 'Stopped - progress has been saved, run it again to resume.';

    const result = job.result || {};
    if (job.kind === 'scan') {
        const stats = result.stats || {};
        const dated = stats.with_exif !== undefined ? stats.with_exif : (stats.with_metadata || 0);
        return `Batch #${result.batch_id}: ${stats.total || 0} files, ${dated} with a date, `
            + `${formatSize(stats.total_size || 0)}, ${stats.pending || 0} pending copy.`;
    }
    if (job.kind === 'copy' || job.kind === 'retry' || job.kind === 'resolve') {
        const conflicts = result.conflicts
            ? ` ${result.conflicts} need a decision (Batches -> Review conflicts).`
            : '';
        return `Copied ${result.copied || 0}, skipped ${result.skipped || 0}, failed ${result.failed || 0} `
            + `in ${formatSeconds(result.duration_seconds || 0)}.${conflicts}`;
    }
    if (job.kind === 'expand') {
        return `${result.dirs_processed || 0} folders expanded, ${result.files_moved || 0} files, `
            + `${result.dirs_skipped || 0} skipped, ${result.error_count || 0} errors.`;
    }
    return 'Done.';
}

function renderJobHistory(jobs) {
    if (!jobs || jobs.length === 0) {
        jobHistoryEl.innerHTML = '<div class="card-hint">No jobs yet.</div>';
        return;
    }

    jobHistoryEl.innerHTML = jobs.map(job => `
        <div class="job-row">
            <span><span class="job-badge ${job.status}">${escapeHtml(job.status)}</span></span>
            <span class="grow">${escapeHtml(job.title)}</span>
            <span class="when">${escapeHtml((job.started_at || '').replace('T', ' '))}</span>
            <span class="elapsed">${formatSeconds(job.elapsed_seconds)}</span>
        </div>
    `).join('');
}

function renderServerInfo() {
    const thumbs = serverConfig.has_pil
        ? `available${serverConfig.has_heif ? ' \u00b7 HEIC supported' : ''}`
        : 'not installed';

    const rows = [
        ['Serving', serverConfig.root || ''],
        ['Photo database', serverConfig.db_path || ''],
        ['Video database', serverConfig.video_db_path || ''],
        ['Default workers', String(serverConfig.default_workers || '')],
        ['Thumbnails (Pillow)', thumbs],
    ];

    document.getElementById('server-info').innerHTML = rows.map(([label, value]) =>
        `<div class="label">${escapeHtml(label)}</div><div>${escapeHtml(value)}</div>`
    ).join('');
}

// ---------------------------------------------------------------------------
// Batches
// ---------------------------------------------------------------------------

// The Import tab says which batch "Copy latest batch" would actually run on
async function loadLatestBatch() {
    const el = document.getElementById('latest-batch');
    if (!el || !serverConfig.import_enabled) return;

    try {
        const data = await api('GET', `/api/batches?media=${copyMedia}&limit=1`);
        const batch = (data.batches || [])[0];
        if (!batch) {
            el.innerHTML = '<span class="source">No batch yet - scan a source first.</span>';
            return;
        }

        const stats = batch.stats || {};
        el.innerHTML = `
            <span class="batch-id">#${batch.id}</span>
            <span class="batch-status ${escapeHtml(batch.status)}">${escapeHtml(batch.status)}</span>
            <span class="source" title="${escapeHtml(batch.source_directory)}">${escapeHtml(batch.source_directory)}</span>
            <span class="pending">${(stats.pending || 0).toLocaleString()} pending</span>`;
    } catch (err) {
        el.innerHTML = '';   // the card still works without it
    }
}

async function loadBatches() {
    batchListEl.innerHTML = '<div class="loading-indicator">Loading...</div>';
    try {
        const data = await api('GET', `/api/batches?media=${batchesMedia}`);
        renderBatches(data.batches || []);
    } catch (err) {
        batchListEl.innerHTML = `<div class="form-error">${escapeHtml(err.message)}</div>`;
    }
}

function renderBatches(batches) {
    if (batches.length === 0) {
        batchListEl.innerHTML = '<div class="card-hint">No batches yet - run a scan first.</div>';
        return;
    }

    batchListEl.innerHTML = batches.map(batch => {
        const stats = batch.stats || {};
        const dated = stats.with_exif !== undefined ? stats.with_exif : (stats.with_metadata || 0);
        const datedLabel = batchesMedia === 'video' ? 'With metadata' : 'With EXIF';
        const paths = `${batch.source_directory} \u2192 ${batch.target_directory}`;

        // One bar for how the batch ended up: copied, skipped, conflicts, failed
        const total = stats.total || 0;
        const share = (n) => total ? `${((n || 0) / total) * 100}%` : '0';

        return `
        <div class="batch" data-id="${batch.id}">
            <div class="batch-head">
                <span class="batch-id">#${batch.id}</span>
                <span class="batch-status ${escapeHtml(batch.status)}">${escapeHtml(batch.status)}</span>
                <span class="batch-when">${escapeHtml((batch.started_at || '').replace('T', ' '))}</span>
                <span class="batch-paths" title="${escapeHtml(paths)}">${escapeHtml(paths)}</span>
            </div>
            <div class="batch-mix" title="copied / skipped / conflicts / failed">
                <span class="mix-copied" style="width:${share(stats.copied)}"></span>
                <span class="mix-skipped" style="width:${share(stats.skipped)}"></span>
                <span class="mix-conflicts" style="width:${share(stats.conflicts)}"></span>
                <span class="mix-failed" style="width:${share(stats.failed)}"></span>
            </div>
            <div class="batch-stats">
                <span>Total <b>${(stats.total || 0).toLocaleString()}</b></span>
                <span>Pending <b>${(stats.pending || 0).toLocaleString()}</b></span>
                <span>Copied <b>${(stats.copied || 0).toLocaleString()}</b></span>
                <span>Skipped <b>${(stats.skipped || 0).toLocaleString()}</b></span>
                <span>Failed <b class="${stats.failed ? 'bad' : ''}">${(stats.failed || 0).toLocaleString()}</b></span>
                <span>Conflicts <b class="${stats.conflicts ? 'warn' : ''}">${(stats.conflicts || 0).toLocaleString()}</b></span>
                <span>${datedLabel} <b>${(dated || 0).toLocaleString()}</b></span>
                <span>Size <b>${formatSize(stats.total_size || 0)}</b></span>
            </div>
            <div class="batch-actions">
                <button class="btn primary" data-action="copy" data-id="${batch.id}"
                    ${stats.pending ? '' : 'disabled'}>Copy ${stats.pending || 0} pending</button>
                <button class="btn" data-action="dry" data-id="${batch.id}"
                    ${stats.pending ? '' : 'disabled'}>Dry run</button>
                <button class="btn warn" data-action="conflicts" data-id="${batch.id}"
                    ${stats.conflicts ? '' : 'disabled'}>Review ${stats.conflicts || 0} conflicts</button>
                <button class="btn" data-action="retry" data-id="${batch.id}"
                    ${stats.failed ? '' : 'disabled'}>Retry ${stats.failed || 0} failed</button>
                <button class="btn" data-action="failed" data-id="${batch.id}"
                    ${stats.failed ? '' : 'disabled'}>Show failed</button>
                <button class="btn" data-action="files" data-id="${batch.id}"
                    ${stats.copied ? '' : 'disabled'}>Show ${stats.copied || 0} imported files</button>
                <button class="btn ghost" data-action="browse" data-id="${batch.id}"
                    data-path="${escapeHtml(batch.target_directory)}">Open target</button>
            </div>
            <div class="batch-failed" id="failed-${batch.id}" hidden></div>
        </div>`;
    }).join('');
}

async function onBatchAction(e) {
    const btn = e.target.closest('button[data-action]');
    if (!btn || btn.disabled) return;

    const batchId = parseInt(btn.dataset.id, 10);
    const action = btn.dataset.action;

    try {
        if (action === 'copy' || action === 'dry') {
            const job = await api('POST', '/api/copy', {
                media: batchesMedia,
                batch_id: batchId,
                dry_run: action === 'dry',
                skip_no_date: document.getElementById('copy-skip-no-date').checked,
                use_file_date: document.getElementById('copy-use-file-date').checked,
            });
            watchJob(job);
        } else if (action === 'retry') {
            const job = await api('POST', '/api/retry', {media: batchesMedia, batch_id: batchId});
            watchJob(job);
        } else if (action === 'conflicts') {
            openConflicts(batchesMedia, batchId);
        } else if (action === 'failed') {
            await toggleFailedFiles(batchId);
        } else if (action === 'files') {
            setView('browse');
            enterBatchView(batchesMedia, batchId);
        } else if (action === 'browse') {
            openTargetInBrowser(btn.dataset.path);
        }
    } catch (err) {
        const container = document.getElementById(`failed-${batchId}`);
        if (container) {
            container.hidden = false;
            container.innerHTML = `<div>${escapeHtml(err.message)}</div>`;
        }
    }
}

function watchJob(job) {
    watchedJobId = job.id;
    dismissedJobId = null;
    lastWatchedStatus = job.status;
    renderJobBar(job);
    schedulePoll(400);
}

async function toggleFailedFiles(batchId) {
    const container = document.getElementById(`failed-${batchId}`);
    if (!container.hidden) {
        container.hidden = true;
        return;
    }

    container.hidden = false;
    container.innerHTML = '<div>Loading...</div>';

    const data = await api('GET', `/api/batch?media=${batchesMedia}&id=${batchId}`);
    const files = data.failed_files || [];
    container.innerHTML = files.length
        ? files.map(f => `<div>${escapeHtml(f.filename)}: ${escapeHtml(f.error || 'unknown error')}</div>`).join('')
        : '<div>No failed files.</div>';
}

function openTargetInBrowser(targetPath) {
    const root = serverConfig.root || '';
    if (targetPath === root) {
        setView('browse');
        navigateTo('.');
        return;
    }
    if (targetPath.startsWith(root + '/')) {
        setView('browse');
        navigateTo(targetPath.slice(root.length + 1));
        return;
    }
    alert(`This target is outside the served folder:\\n${targetPath}\\n\\nRestart the server on that folder to browse it.`);
}

// ---------------------------------------------------------------------------
// Favorites
// ---------------------------------------------------------------------------

async function toggleFavorite(path, wanted) {
    if (!path) return;

    const body = wanted === undefined ? {path: path} : {path: path, favorite: wanted};
    try {
        const data = await api('POST', '/api/favorites', body);
        applyFavoriteState(data.path, data.favorite);
        updateFavoriteCount(data.total);
        if (currentView === 'favorites') loadFavorites();
    } catch (err) {
        console.error('Favorite failed:', err);
    }
}

function applyFavoriteState(path, favorite) {
    document.querySelectorAll(`.fav-toggle[data-fav="${CSS.escape(path)}"]`).forEach(btn => {
        btn.classList.toggle('on', favorite);
        btn.textContent = favorite ? '\\u2605' : '\\u2606';
        btn.title = favorite ? 'Remove from favorites' : 'Add to favorites';
    });

    const entry = media.find(m => m.path === path);
    if (entry) entry.favorite = favorite;

    if (lightboxEl.classList.contains('active')) updateLightboxFavorite();
}

function updateFavoriteCount(total) {
    document.getElementById('fav-count').textContent = total ? `(${total})` : '';
}

function updateLightboxFavorite() {
    const item = media.find(m => m.path === currentMediaPath);
    const on = !!(item && item.favorite);
    lightboxFavEl.classList.toggle('on', on);
    lightboxFavEl.textContent = on ? '\\u2605' : '\\u2606';
}

async function pruneFavorites() {
    try {
        const data = await api('POST', '/api/favorites/prune', {});
        updateFavoriteCount(data.total);
        loadFavorites();
    } catch (err) {
        console.error('Prune failed:', err);
    }
}

async function loadFavorites() {
    favoritesGridEl.innerHTML = '<div class="loading-indicator">Loading...</div>';

    try {
        const data = await api('GET', '/api/favorites');
        favoriteItems = data.favorites || [];
        updateFavoriteCount(data.total);

        const missing = favoriteItems.filter(f => f.missing).length;
        document.getElementById('favorites-count').textContent = missing
            ? `${data.total} favorites (${missing} missing)`
            : `${data.total} favorites`;

        if (favoriteItems.length === 0) {
            favoritesGridEl.innerHTML = '<div class="empty">No favorites yet - hover a photo and hit the star, or press F.</div>';
            return;
        }

        renderFiles(favoriteItems, favoritesGridEl);
    } catch (err) {
        favoritesGridEl.innerHTML = `<div class="form-error">${escapeHtml(err.message)}</div>`;
    }
}

// Favorites grid: star toggles, anything else opens the lightbox over the favorites
favoritesGridEl.addEventListener('click', (e) => {
    const star = e.target.closest('.fav-toggle');
    if (star) {
        e.stopPropagation();
        toggleFavorite(star.dataset.fav);
        return;
    }

    const item = e.target.closest('.file-item');
    if (!item || item.classList.contains('missing')) return;

    media = favoriteItems.filter(f => !f.missing && (f.is_image || f.is_video));
    openLightbox(item.dataset.path);
});

// ---------------------------------------------------------------------------
// Storage - size and counts per folder (per year inside the library)
// ---------------------------------------------------------------------------

async function loadStorage(path, refresh) {
    if (path !== undefined) storagePath = path;

    const tableEl = document.getElementById('storage-table');
    tableEl.innerHTML = '<div class="loading-indicator">Measuring folders, this can take a while on a big library...</div>';

    try {
        const query = `path=${encodeURIComponent(storagePath)}${refresh ? '&refresh=1' : ''}`;
        const data = await api('GET', `/api/stats?${query}`);
        storageParent = data.parent;
        renderStorage(data);
    } catch (err) {
        tableEl.innerHTML = `<div class="form-error">${escapeHtml(err.message)}</div>`;
    }
}

function renderStorage(data) {
    const root = serverConfig.root || '';
    document.getElementById('storage-path').textContent =
        data.path === '.' ? root : `${root}/${data.path}`;

    const totals = data.totals;
    document.getElementById('storage-summary').innerHTML = `
        <span>Total size<b>${formatGB(totals.bytes)}</b></span>
        <span>Photos<b>${totals.images.toLocaleString()}</b></span>
        <span>RAW<b>${(totals.raw || 0).toLocaleString()}</b></span>
        <span>Videos<b>${totals.videos.toLocaleString()}</b></span>
        <span>Other files<b>${totals.other.toLocaleString()}</b></span>
        <span>Folders<b>${data.rows.length}</b></span>
    `;

    document.getElementById('btn-storage-up').disabled = !data.parent;

    if (data.rows.length === 0 && totals.files === 0) {
        document.getElementById('storage-table').innerHTML = '<div class="card-hint">This folder is empty.</div>';
        return;
    }

    const biggest = Math.max(1, ...data.rows.map(r => r.bytes));
    const rows = data.rows.map(row => `
        <tr>
            <td><span class="folder-link" data-storage-path="${escapeHtml(row.path)}">${escapeHtml(row.name)}</span></td>
            <td class="num">${row.images.toLocaleString()}</td>
            <td class="num">${(row.raw || 0).toLocaleString()}</td>
            <td class="num">${row.videos.toLocaleString()}</td>
            <td class="num">${row.other.toLocaleString()}</td>
            <td class="num">${formatGB(row.bytes)}</td>
            <td><div class="storage-bar" style="width: ${Math.round(100 * row.bytes / biggest)}%"></div></td>
        </tr>
    `).join('');

    const loose = data.loose.files > 0 ? `
        <tr>
            <td><em>files in this folder</em></td>
            <td class="num">${data.loose.images.toLocaleString()}</td>
            <td class="num">${(data.loose.raw || 0).toLocaleString()}</td>
            <td class="num">${data.loose.videos.toLocaleString()}</td>
            <td class="num">${data.loose.other.toLocaleString()}</td>
            <td class="num">${formatGB(data.loose.bytes)}</td>
            <td></td>
        </tr>` : '';

    document.getElementById('storage-table').innerHTML = `
        <table class="storage-table">
            <thead>
                <tr>
                    <th>Folder</th><th>Photos</th><th>RAW</th><th>Videos</th><th>Other</th><th>Size</th><th style="width:25%"></th>
                </tr>
            </thead>
            <tbody>${rows}${loose}</tbody>
            <tfoot>
                <tr>
                    <td>Total</td>
                    <td class="num">${totals.images.toLocaleString()}</td>
                    <td class="num">${(totals.raw || 0).toLocaleString()}</td>
                    <td class="num">${totals.videos.toLocaleString()}</td>
                    <td class="num">${totals.other.toLocaleString()}</td>
                    <td class="num">${formatGB(totals.bytes)}</td>
                    <td></td>
                </tr>
            </tfoot>
        </table>
    `;
}

// ---------------------------------------------------------------------------
// S - size of whatever is selected in the hierarchy, without leaving Browse
// ---------------------------------------------------------------------------

// Whatever the user would call "selected": tree focus, then grid folder, then here
function selectedHierarchyPath() {
    if (focusedPanel === 'tree') {
        const treeFolder = treeEl.querySelector('.tree-folder.focused')
            || treeEl.querySelector('.tree-folder.active');
        if (treeFolder) return treeFolder.dataset.path;
    }

    const grid = currentView === 'favorites' ? favoritesGridEl : fileGridEl;
    const selected = grid.querySelectorAll('.file-item')[selectedIndex];
    if (selected && selected.classList.contains('folder')) return selected.dataset.path;

    return currentPath;
}

function toggleSizeDialog() {
    if (sizeModalEl.classList.contains('active')) {
        closeSizeDialog();
    } else {
        openSizeDialog(selectedHierarchyPath());
    }
}

function openSizeDialog(path, refresh) {
    sizePath = path || '.';
    sizeModalEl.classList.add('active');
    loadSizeDialog(refresh);
}

function closeSizeDialog() {
    sizeModalEl.classList.remove('active');
}

async function loadSizeDialog(refresh) {
    const name = sizePath === '.' ? (serverConfig.root || 'Library').split('/').pop() : sizePath.split('/').pop();
    document.getElementById('size-title').textContent = `Size of ${name}`;
    document.getElementById('size-path').textContent =
        sizePath === '.' ? (serverConfig.root || '') : `${serverConfig.root || ''}/${sizePath}`;
    document.getElementById('size-summary').innerHTML = '';
    document.getElementById('size-table').innerHTML =
        '<div class="loading-indicator">Measuring...</div>';

    try {
        const query = `path=${encodeURIComponent(sizePath)}${refresh ? '&refresh=1' : ''}`;
        const data = await api('GET', `/api/stats?${query}`);
        sizeParent = data.parent;
        renderSizeDialog(data);
    } catch (err) {
        document.getElementById('size-table').innerHTML =
            `<div class="form-error">${escapeHtml(err.message)}</div>`;
    }
}

function renderSizeDialog(data) {
    const totals = data.totals;
    document.getElementById('size-summary').innerHTML = `
        <span>Total size<b>${formatGB(totals.bytes)}</b></span>
        <span>Photos<b>${totals.images.toLocaleString()}</b></span>
        <span>RAW<b>${(totals.raw || 0).toLocaleString()}</b></span>
        <span>Videos<b>${totals.videos.toLocaleString()}</b></span>
        <span>Folders<b>${data.rows.length}</b></span>
    `;

    document.getElementById('size-up').disabled = !data.parent;

    if (data.rows.length === 0) {
        document.getElementById('size-table').innerHTML =
            '<div class="card-hint" style="padding:0.6rem">No subfolders - the totals above cover this folder.</div>';
        return;
    }

    const biggest = Math.max(1, ...data.rows.map(r => r.bytes));
    document.getElementById('size-table').innerHTML = `
        <table class="storage-table">
            <thead>
                <tr><th>Folder</th><th>Photos</th><th>RAW</th><th>Videos</th><th>Size</th><th style="width:20%"></th></tr>
            </thead>
            <tbody>
                ${data.rows.map(row => `
                    <tr>
                        <td><span class="folder-link" data-size-path="${escapeHtml(row.path)}">${escapeHtml(row.name)}</span></td>
                        <td class="num">${row.images.toLocaleString()}</td>
                        <td class="num">${(row.raw || 0).toLocaleString()}</td>
                        <td class="num">${row.videos.toLocaleString()}</td>
                        <td class="num">${formatGB(row.bytes)}</td>
                        <td><div class="storage-bar" style="width: ${Math.round(100 * row.bytes / biggest)}%"></div></td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

function formatGB(bytes) {
    const gb = (bytes || 0) / (1024 ** 3);
    if (gb >= 10) return `${gb.toFixed(1)} GB`;
    if (gb >= 0.1) return `${gb.toFixed(2)} GB`;
    return `${((bytes || 0) / (1024 ** 2)).toFixed(0)} MB`;
}

// ---------------------------------------------------------------------------
// Conflicts
// ---------------------------------------------------------------------------

function openConflicts(mediaType, batchId) {
    conflictContext = {media: mediaType, batchId: batchId};
    conflictPage = 1;
    setView('conflicts');
}

async function loadConflicts() {
    if (!conflictContext) return;

    const listEl = document.getElementById('conflict-list');
    listEl.innerHTML = '<div class="loading-indicator">Loading...</div>';

    try {
        const data = await api('GET',
            `/api/conflicts?media=${conflictContext.media}&batch_id=${conflictContext.batchId}&page=${conflictPage}`);

        document.getElementById('conflicts-subtitle').textContent =
            `batch #${conflictContext.batchId} - ${data.pagination.total} left`;

        if (data.conflicts.length === 0) {
            listEl.innerHTML = '<div class="card-hint">Nothing left to decide.</div>';
            document.getElementById('conflict-pagination').innerHTML = '';
            return;
        }

        listEl.innerHTML = data.conflicts.map(renderConflict).join('');
        renderConflictPagination(data.pagination);
    } catch (err) {
        listEl.innerHTML = `<div class="form-error">${escapeHtml(err.message)}</div>`;
    }
}

function renderConflict(conflict) {
    let badge = '<span class="conflict-badge">unknown</span>';
    if (conflict.identical === true) {
        badge = '<span class="conflict-badge identical">identical file</span>';
    } else if (conflict.identical === false) {
        badge = '<span class="conflict-badge different">different file</span>';
    }

    return `
        <div class="conflict" data-id="${conflict.id}">
            <div class="conflict-head">
                <span class="conflict-name">${escapeHtml(conflict.filename)}</span>
                ${badge}
                <div class="conflict-actions">
                    <button class="btn" data-action="skip" data-id="${conflict.id}">Keep existing</button>
                    <button class="btn" data-action="keep_both" data-id="${conflict.id}">Keep both</button>
                    <button class="btn danger" data-action="overwrite" data-id="${conflict.id}">Replace</button>
                </div>
            </div>
            <div class="conflict-sides">
                ${renderConflictSide('Imported (new)', conflict.incoming)}
                ${renderConflictSide('Already in library', conflict.existing)}
            </div>
        </div>
    `;
}

function renderConflictSide(title, side) {
    if (!side || side.missing) {
        return `
            <div class="conflict-side">
                <h4>${escapeHtml(title)}</h4>
                <div class="no-preview">File is missing</div>
                <div class="conflict-meta">
                    <span>Path</span><b>${escapeHtml(side && side.path ? side.path : 'unknown path')}</b>
                </div>
            </div>
        `;
    }

    const preview = side.is_image
        ? `<img class="preview" src="/api/preview?path=${encodeURIComponent(side.path)}&size=400" alt=""
                onerror="this.hidden=true; this.nextElementSibling.hidden=false;">
           <div class="no-preview" hidden>No preview for this format</div>`
        : '<div class="no-preview">No preview (video or unsupported format)</div>';

    const taken = side.taken_at
        ? `<span>Taken</span><b>${escapeHtml(side.taken_at.replace('T', ' '))}</b>`
        : '';

    return `
        <div class="conflict-side">
            <h4>${escapeHtml(title)}</h4>
            ${preview}
            <div class="conflict-meta">
                <span>Size</span><b>${formatSize(side.size)}</b>
                ${taken}
                <span>Modified</span><b>${escapeHtml(side.modified.replace('T', ' '))}</b>
                <span>Path</span><b>${escapeHtml(side.path)}</b>
            </div>
        </div>
    `;
}

function renderConflictPagination(pag) {
    const el = document.getElementById('conflict-pagination');
    if (pag.total_pages <= 1) {
        el.innerHTML = '';
        return;
    }

    el.innerHTML = `
        <button class="page-btn" ${pag.page <= 1 ? 'disabled' : ''} data-cpage="${pag.page - 1}">&laquo; Prev</button>
        <span class="page-info">Page ${pag.page} of ${pag.total_pages}</span>
        <button class="page-btn" ${pag.page >= pag.total_pages ? 'disabled' : ''} data-cpage="${pag.page + 1}">Next &raquo;</button>
    `;
}

async function resolveConflicts(action, fileIds) {
    if (action === 'overwrite') {
        const what = fileIds ? 'this file' : 'every remaining conflict';
        if (!confirm(`Replace ${what} in the library with the imported version? The current file is lost.`)) {
            return;
        }
    }

    try {
        const body = {
            media: conflictContext.media,
            batch_id: conflictContext.batchId,
            action: action,
        };
        if (fileIds) body.file_ids = fileIds;

        watchJob(await api('POST', '/api/conflicts/resolve', body));
    } catch (err) {
        document.getElementById('conflict-list').insertAdjacentHTML('afterbegin',
            `<div class="form-error">${escapeHtml(err.message)}</div>`);
    }
}

// ---------------------------------------------------------------------------
// Mounted volumes (import source shortcuts)
// ---------------------------------------------------------------------------

async function loadVolumes() {
    try {
        const data = await api('GET', '/api/volumes');
        renderVolumes(data.volumes || []);
    } catch (err) {
        console.error('Failed to list volumes:', err);
        volumeListEl.innerHTML = '<span class="empty-row">Could not list volumes</span>';
    }
}

function renderVolumes(volumes) {
    if (volumes.length === 0) {
        volumeListEl.innerHTML = '<span class="empty-row">No mounted volumes</span>';
        return;
    }

    const icons = {served: '\\u2605', volume: '\\u25A0', home: '\\u2302'};

    volumeListEl.innerHTML = volumes.map(vol => `
        <button class="vol-btn" data-path="${escapeHtml(vol.path)}"
                title="Use ${escapeHtml(vol.path)} as source">
            <span class="vol-icon">${icons[vol.kind] || icons.volume}</span>
            <span class="vol-name">${escapeHtml(vol.name)}</span>
        </button>
    `).join('');
}

// Clicking a volume fills in the scan source
function onVolumeClick(e) {
    const btn = e.target.closest('.vol-btn');
    if (!btn) return;

    document.getElementById('scan-source').value = btn.dataset.path;
    showFormMessage('scan-error', '');
}

// ---------------------------------------------------------------------------
// Directory picker
// ---------------------------------------------------------------------------

function openDirPicker(inputId) {
    dirPickerInputId = inputId;
    const start = document.getElementById(inputId).value.trim() || serverConfig.root;
    dirModalEl.classList.add('active');
    loadDirPicker(start);
}

function closeDirPicker() {
    dirModalEl.classList.remove('active');
    dirPickerInputId = null;
}

async function loadDirPicker(path) {
    dirListEl.innerHTML = '<div class="empty-row">Loading...</div>';
    try {
        const data = await api('GET', `/api/fs?path=${encodeURIComponent(path || '')}`);
        dirPickerPath = data.path;
        dirModalEl.dataset.parent = data.parent || '';
        dirPathEl.textContent = data.path;
        document.getElementById('dir-up').disabled = !data.parent;

        dirListEl.innerHTML = data.dirs.length
            ? data.dirs.map(d => `<div class="dir-row" data-path="${escapeHtml(d.path)}">${escapeHtml(d.name)}</div>`).join('')
            : '<div class="empty-row">No subfolders</div>';
    } catch (err) {
        dirListEl.innerHTML = `<div class="empty-row">${escapeHtml(err.message)}</div>`;
    }
}

function formatSeconds(total) {
    const seconds = Math.max(0, Math.floor(total || 0));
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const rest = seconds % 60;
    if (hours > 0) return `${hours}h ${minutes}m ${rest}s`;
    if (minutes > 0) return `${minutes}m ${rest}s`;
    return `${rest}s`;
}

// Utility functions
function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function formatSize(bytes) {
    const units = ['B', 'KB', 'MB', 'GB'];
    let i = 0;
    while (bytes >= 1024 && i < units.length - 1) {
        bytes /= 1024;
        i++;
    }
    return `${bytes.toFixed(1)} ${units[i]}`;
}'''

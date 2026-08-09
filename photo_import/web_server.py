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
            info = {
                "path": entry["path"],
                "name": full_path.name,
                "added_at": entry["added_at"],
                "is_dir": False,
                "is_image": extension in IMAGE_EXTENSIONS,
                "is_video": extension in VIDEO_EXTENSIONS,
                "favorite": True,
                "missing": not full_path.exists(),
            }
            if not info["missing"]:
                info["size"] = full_path.stat().st_size
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
                    info["favorite"] = relative_path in favorite_paths
                    if info["is_image"] or info["is_video"]:
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

# Above this, comparing two files byte by byte costs more than it helps
MAX_COMPARE_BYTES = 512 * 1024 * 1024


# Walking a big library is slow, so results are kept until explicitly refreshed
STATS_CACHE: dict = {}


def scan_tree(directory: Path) -> dict:
    """Total size and file counts under a directory, by kind."""
    totals = {"files": 0, "images": 0, "videos": 0, "other": 0, "bytes": 0}
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
                else:
                    totals["other"] += 1
            except OSError:
                continue

    return totals


def collect_directory_stats(target: Path, root: Path) -> dict:
    """Per-subfolder totals plus the files sitting directly in this folder."""
    rows = []
    loose = {"files": 0, "images": 0, "videos": 0, "other": 0, "bytes": 0}

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
                else:
                    loose["other"] += 1
        except OSError:
            continue

    totals = {
        key: sum(row[key] for row in rows) + loose[key]
        for key in ("files", "images", "videos", "other", "bytes")
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
            </div>
            <div class="breadcrumb" id="breadcrumb"></div>
        </header>

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

        <div class="main view active" id="view-browse">
            <nav class="sidebar" id="sidebar">
                <div class="tree" id="tree"></div>
            </nav>

            <main class="content">
                <div class="toolbar">
                    <button class="btn" id="btn-grid" title="Grid view (G)" data-shortcut="G">Grid</button>
                    <button class="btn" id="btn-list" title="List view (L)" data-shortcut="L">List</button>
                    <select class="btn" id="per-page-select" title="Items per page (1-4)">
                        <option value="25" data-shortcut="1">25</option>
                        <option value="50" selected data-shortcut="2">50</option>
                        <option value="100" data-shortcut="3">100</option>
                        <option value="200" data-shortcut="4">200</option>
                    </select>
                    <select class="btn" id="sort-select" title="Sort by (S)">
                        <option value="name" selected>Name (N)</option>
                        <option value="modified">Modified (M)</option>
                        <option value="created">Created (C)</option>
                        <option value="accessed">Accessed (A)</option>
                        <option value="size">Size (Z)</option>
                    </select>
                    <button class="btn" id="btn-sort-order" title="Sort order (O)" data-shortcut="O">↑</button>
                    <span class="filter-wrapper" data-shortcut="F">
                        <input type="text" class="filter-input" id="filter-input" placeholder="Filter..." title="Filter files (F)">
                    </span>
                    <button class="btn" id="btn-clear-filter" title="Clear filter (Esc)" style="display:none;">&times;</button>
                    <button class="btn import-only" id="btn-import-here" title="Import from this folder">Import this folder</button>
                    <button class="btn" id="btn-help" title="Show shortcuts (?)" data-shortcut="?">?</button>
                    <span class="file-count" id="file-count"></span>
                </div>

                <div class="file-grid" id="file-grid"></div>

                <div class="pagination" id="pagination"></div>
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

                <div class="options">
                    <label class="check"><input type="checkbox" id="copy-dry-run"> Dry run</label>
                    <label class="check"><input type="checkbox" id="copy-skip-no-date"> Skip files without EXIF/metadata date</label>
                    <label class="check"><input type="checkbox" id="copy-use-file-date" checked> Fall back to file date</label>
                </div>

                <div class="actions">
                    <button class="btn primary" id="btn-quick-copy">Copy latest batch</button>
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
            <button class="lightbox-close" id="lightbox-close">&times;</button>
            <button class="lightbox-fav" id="lightbox-fav" title="Favorite (f)">&#9734;</button>
            <button class="lightbox-nav lightbox-prev" id="lightbox-prev">&lt;</button>
            <button class="lightbox-nav lightbox-next" id="lightbox-next">&gt;</button>
            <div class="lightbox-content">
                <img id="lightbox-img" src="" alt="">
                <video id="lightbox-video" controls style="display:none;"></video>
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

        <!-- Help overlay -->
        <div class="help-overlay" id="help-overlay">
            <div class="help-content">
                <h2>Keyboard Shortcuts</h2>
                <div class="help-columns">
                    <div class="help-section">
                        <h3>View</h3>
                        <div class="help-row"><kbd>G</kbd> Grid view</div>
                        <div class="help-row"><kbd>L</kbd> List view</div>
                        <div class="help-row"><kbd>1-4</kbd> Items per page</div>
                    </div>
                    <div class="help-section">
                        <h3>Sort By</h3>
                        <div class="help-row"><kbd>N</kbd> Name</div>
                        <div class="help-row"><kbd>M</kbd> Modified date</div>
                        <div class="help-row"><kbd>C</kbd> Created date</div>
                        <div class="help-row"><kbd>A</kbd> Accessed date</div>
                        <div class="help-row"><kbd>Z</kbd> Size</div>
                        <div class="help-row"><kbd>O</kbd> Toggle order ↑↓</div>
                    </div>
                    <div class="help-section">
                        <h3>Navigation</h3>
                        <div class="help-row"><kbd>[</kbd> Previous page</div>
                        <div class="help-row"><kbd>]</kbd> Next page</div>
                        <div class="help-row"><kbd>Tab</kbd> Switch panel</div>
                        <div class="help-row"><kbd>↑↓←→</kbd> Navigate items</div>
                        <div class="help-row"><kbd>Enter</kbd> Open item</div>
                        <div class="help-row"><kbd>Backspace</kbd> Parent folder</div>
                    </div>
                    <div class="help-section">
                        <h3>Other</h3>
                        <div class="help-row"><kbd>F</kbd> Focus filter</div>
                        <div class="help-row"><kbd>*</kbd> Favorite selected item</div>
                        <div class="help-row"><kbd>Esc</kbd> Clear filter / Close</div>
                        <div class="help-row"><kbd>?</kbd> Toggle this help</div>
                    </div>
                </div>
                <p class="help-hint">Press <kbd>?</kbd> to close</p>
            </div>
        </div>
    </div>

    <script src="/app.js"></script>
</body>
</html>'''


def get_styles_css() -> str:
    """Return CSS styles."""
    return '''* {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    background: #1a1a2e;
    color: #eee;
    line-height: 1.5;
}

.app {
    display: flex;
    flex-direction: column;
    height: 100vh;
}

.header {
    background: #16213e;
    padding: 1rem;
    border-bottom: 1px solid #0f3460;
}

.header h1 {
    font-size: 1.5rem;
    margin-bottom: 0.5rem;
    color: #e94560;
}

.breadcrumb {
    font-size: 0.9rem;
    color: #888;
}

.breadcrumb a {
    color: #4db5ff;
    text-decoration: none;
}

.breadcrumb a:hover {
    text-decoration: underline;
}

.breadcrumb .separator {
    margin: 0 0.5rem;
    color: #555;
}

.header-row {
    display: flex;
    align-items: center;
    gap: 1.5rem;
    margin-bottom: 0.5rem;
}

.header-row h1 {
    margin-bottom: 0;
}

.tabs {
    display: flex;
    gap: 0.25rem;
}

.tab {
    padding: 0.4rem 0.9rem;
    border: 1px solid #0f3460;
    background: #1a1a2e;
    color: #ccc;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.85rem;
}

.tab:hover {
    background: #0f3460;
    color: #eee;
}

.tab.active {
    background: #e94560;
    border-color: #e94560;
    color: #fff;
}

.import-only.hidden {
    display: none;
}

/* Views */
.view {
    display: none;
    flex: 1;
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
    overflow-y: auto;
    padding: 1.5rem;
    gap: 1.5rem;
}

.sidebar {
    width: 280px;
    background: #16213e;
    border-right: 1px solid #0f3460;
    overflow-y: auto;
    padding: 1rem;
}

.tree {
    font-size: 0.9rem;
}

.tree-item {
    padding: 0.3rem 0;
}

.tree-folder {
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.2rem 0;
}

.tree-folder:hover {
    color: #e94560;
}

.tree-folder.has-children::before {
    content: ">";
    font-size: 0.7rem;
    transition: transform 0.2s;
    width: 10px;
}

.tree-folder:not(.has-children)::before {
    content: "";
    width: 10px;
}

.tree-folder.open::before {
    transform: rotate(90deg);
}

.tree-folder.loading::before {
    content: "";
    width: 10px;
    height: 10px;
    border: 2px solid #e94560;
    border-top-color: transparent;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
}

@keyframes spin {
    to { transform: rotate(360deg); }
}

.tree-children {
    margin-left: 1.2rem;
    display: none;
}

.tree-folder.open + .tree-children {
    display: block;
}

.tree-folder.active {
    color: #e94560;
    font-weight: bold;
}

.tree-folder.focused {
    background: #0f3460;
    border-radius: 4px;
    outline: 2px solid #4db5ff;
    outline-offset: 1px;
}

/* Panel focus indicators */
.sidebar.focused {
    box-shadow: inset 0 0 0 2px #4db5ff;
}

.content.focused {
    box-shadow: inset 0 0 0 2px #4db5ff;
}

.tree-empty {
    color: #666;
    font-style: italic;
    padding: 0.3rem 0;
}

.loading-indicator {
    color: #888;
    padding: 2rem;
    text-align: center;
}

.content {
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
}

/* Pagination */
.pagination {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 0.5rem;
    padding: 1rem;
    background: #16213e;
    border-top: 1px solid #0f3460;
    flex-wrap: wrap;
}

.pagination:empty {
    display: none;
}

.pagination .page-btn {
    padding: 0.5rem 1rem;
    border: 1px solid #0f3460;
    background: #1a1a2e;
    color: #eee;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.9rem;
    min-width: 40px;
}

.pagination .page-btn:hover:not(:disabled) {
    background: #0f3460;
}

.pagination .page-btn.active {
    background: #e94560;
    border-color: #e94560;
}

.pagination .page-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
}

.pagination .page-info {
    color: #888;
    font-size: 0.85rem;
    padding: 0 1rem;
}

.pagination .page-ellipsis {
    color: #666;
    padding: 0 0.5rem;
}

.toolbar {
    padding: 0.75rem 1rem;
    background: #16213e;
    border-bottom: 1px solid #0f3460;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.btn {
    padding: 0.4rem 0.8rem;
    border: 1px solid #0f3460;
    background: #1a1a2e;
    color: #eee;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.85rem;
}

.btn:hover {
    background: #0f3460;
}

.btn.active {
    background: #e94560;
    border-color: #e94560;
}

/* Keyboard shortcut badges */
.btn[data-shortcut],
.page-btn[data-shortcut] {
    position: relative;
}

.btn[data-shortcut]::after,
.page-btn[data-shortcut]::after {
    content: attr(data-shortcut);
    position: absolute;
    top: -8px;
    right: -8px;
    background: #e94560;
    color: #fff;
    font-size: 0.65rem;
    font-weight: bold;
    padding: 2px 5px;
    border-radius: 3px;
    opacity: 0;
    transition: opacity 0.2s;
    pointer-events: none;
}

.show-shortcuts .btn[data-shortcut]::after,
.show-shortcuts .page-btn[data-shortcut]::after {
    opacity: 1;
}

#btn-help {
    min-width: 32px;
    font-weight: bold;
}

#btn-help.active::after {
    display: none;
}

/* Filter input */
.filter-wrapper {
    position: relative;
    display: inline-block;
}

.filter-wrapper[data-shortcut]::after {
    content: attr(data-shortcut);
    position: absolute;
    top: -8px;
    right: -8px;
    background: #e94560;
    color: #fff;
    font-size: 0.65rem;
    font-weight: bold;
    padding: 2px 5px;
    border-radius: 3px;
    opacity: 0;
    transition: opacity 0.2s;
    pointer-events: none;
    z-index: 1;
}

.show-shortcuts .filter-wrapper[data-shortcut]::after {
    opacity: 1;
}

.filter-input {
    padding: 0.4rem 0.8rem;
    border: 1px solid #0f3460;
    background: #1a1a2e;
    color: #eee;
    border-radius: 4px;
    font-size: 0.85rem;
    width: 150px;
    outline: none;
}

.filter-input:focus {
    border-color: #4db5ff;
    box-shadow: 0 0 0 2px rgba(77, 181, 255, 0.2);
}

.filter-input::placeholder {
    color: #666;
}

#btn-clear-filter {
    padding: 0.4rem 0.6rem;
    margin-left: 4px;
    border-radius: 4px;
}

.filter-wrapper:has(.filter-input:not(:placeholder-shown)) ~ #btn-clear-filter {
    display: inline-block !important;
}

.file-count {
    margin-left: auto;
    color: #888;
    font-size: 0.85rem;
}

.file-grid {
    flex: 1;
    overflow: hidden;
    padding: 1rem;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(120px, 1fr));
    grid-auto-rows: 1fr;
    gap: 0.5rem;
    align-content: start;
    align-items: stretch;
}

.file-grid.list-view {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
}

.file-item {
    background: #16213e;
    border-radius: 8px;
    overflow: hidden;
    cursor: pointer;
    transition: transform 0.2s, box-shadow 0.2s;
    display: flex;
    flex-direction: column;
    min-height: 0;
}

.file-item:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
}

.file-item.selected {
    outline: 3px solid #e94560;
    outline-offset: -3px;
    box-shadow: 0 0 12px rgba(233, 69, 96, 0.4);
}

.file-item.folder {
    background: #0f3460;
}

.file-thumb {
    width: 100%;
    flex: 1;
    min-height: 60px;
    object-fit: contain;
    object-position: center;
    background: #0f3460;
    display: block;
}

.file-icon {
    width: 100%;
    flex: 1;
    min-height: 60px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 3rem;
    background: #0f3460;
    color: #4db5ff;
}

.file-name {
    padding: 0.3rem 0.5rem;
    font-size: 0.75rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex-shrink: 0;
}

/* List view styles */
.file-grid.list-view .file-item {
    display: flex;
    flex-direction: row;
    align-items: center;
    border-radius: 4px;
}

.file-grid.list-view .file-thumb,
.file-grid.list-view .file-icon,
.file-grid.list-view .video-icon {
    width: 48px;
    height: 48px;
    min-height: 48px;
    max-height: 48px;
    font-size: 1.4rem;
    flex-shrink: 0;
}

.file-grid.list-view .file-name {
    flex: 1;
}

/* Lightbox */
.lightbox {
    position: fixed;
    top: 0;
    left: 0;
    width: 100vw;
    height: 100vh;
    background: rgba(0,0,0,0.95);
    display: none;
    flex-direction: column;
    z-index: 1000;
    overflow: hidden;
}

.lightbox.active {
    display: flex;
}

.lightbox-close {
    position: absolute;
    top: 1rem;
    right: 1rem;
    background: rgba(0,0,0,0.5);
    border: none;
    color: white;
    font-size: 2rem;
    cursor: pointer;
    z-index: 10;
    width: 44px;
    height: 44px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
}

.lightbox-close:hover {
    background: rgba(255,255,255,0.2);
}

.lightbox-nav {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    background: rgba(0,0,0,0.5);
    border: none;
    color: white;
    font-size: 2rem;
    cursor: pointer;
    padding: 1rem 1.5rem;
    z-index: 10;
}

.lightbox-nav:hover {
    background: rgba(255,255,255,0.2);
}

.lightbox-prev {
    left: 0;
    border-radius: 0 8px 8px 0;
}

.lightbox-next {
    right: 0;
    border-radius: 8px 0 0 8px;
}

.lightbox-content {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 50px;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 1rem 4rem;
    overflow: hidden;
}

#lightbox-img,
#lightbox-video {
    max-width: 100%;
    max-height: 100%;
    width: auto;
    height: auto;
    object-fit: contain;
}

#lightbox-video {
    background: #000;
}

.file-item.video .file-thumb {
    position: relative;
}

.file-item.video::after {
    content: "";
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: 0;
    height: 0;
    border-left: 20px solid rgba(255,255,255,0.9);
    border-top: 12px solid transparent;
    border-bottom: 12px solid transparent;
    pointer-events: none;
}

.file-item.video {
    position: relative;
}

.video-icon {
    width: 100%;
    flex: 1;
    min-height: 60px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 3rem;
    background: linear-gradient(135deg, #1a1a2e 0%, #0f3460 100%);
    color: #e94560;
}

.lightbox-info {
    position: absolute;
    bottom: 0;
    left: 0;
    right: 0;
    height: 50px;
    padding: 0.8rem 1rem;
    text-align: center;
    background: rgba(0,0,0,0.8);
    font-size: 0.9rem;
    display: flex;
    align-items: center;
    justify-content: center;
}

/* Cards & forms (import / batches / tools) */
.card {
    background: #16213e;
    border: 1px solid #0f3460;
    border-radius: 8px;
    padding: 1.25rem;
    max-width: 900px;
    width: 100%;
}

.card h2 {
    font-size: 1.1rem;
    color: #e94560;
    margin-bottom: 0.5rem;
}

.card-head {
    display: flex;
    align-items: center;
    gap: 1rem;
    margin-bottom: 1rem;
    flex-wrap: wrap;
}

.card-head h2 {
    margin-bottom: 0;
    margin-right: auto;
}

.card-hint {
    color: #8a93a8;
    font-size: 0.85rem;
    margin-bottom: 1rem;
}

.card-hint code {
    background: #0f3460;
    padding: 0.1rem 0.35rem;
    border-radius: 3px;
}

.seg {
    display: inline-flex;
    border: 1px solid #0f3460;
    border-radius: 4px;
    overflow: hidden;
    margin-bottom: 1rem;
}

.seg-btn {
    padding: 0.35rem 0.9rem;
    background: #1a1a2e;
    border: none;
    color: #ccc;
    cursor: pointer;
    font-size: 0.85rem;
}

.seg-btn.active {
    background: #0f3460;
    color: #fff;
}

.field {
    margin-bottom: 0.9rem;
}

.field label {
    display: block;
    font-size: 0.8rem;
    color: #8a93a8;
    margin-bottom: 0.3rem;
}

.field-row {
    display: flex;
    gap: 0.5rem;
}

.field-row input[type="text"] {
    flex: 1;
    min-width: 0;
    padding: 0.45rem 0.6rem;
    background: #1a1a2e;
    border: 1px solid #0f3460;
    border-radius: 4px;
    color: #eee;
    font-size: 0.85rem;
    font-family: inherit;
}

.field-row input[type="text"]:focus {
    outline: none;
    border-color: #4db5ff;
}

/* Favorites */
.tab-count {
    opacity: 0.75;
}

.toolbar-title {
    font-weight: 600;
    margin-right: 0.5rem;
}

.fav-toggle {
    position: absolute;
    top: 0.35rem;
    right: 0.35rem;
    width: 28px;
    height: 28px;
    border: none;
    border-radius: 50%;
    background: rgba(0,0,0,0.55);
    color: #ddd;
    font-size: 0.95rem;
    line-height: 1;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    opacity: 0;
    transition: opacity 0.15s;
}

.file-item {
    position: relative;
}

.file-item:hover .fav-toggle,
.file-item.selected .fav-toggle,
.fav-toggle.on {
    opacity: 1;
}

.fav-toggle:hover {
    background: rgba(0,0,0,0.8);
    color: #ffd45e;
}

.fav-toggle.on {
    color: #ffd45e;
}

.file-item.missing {
    opacity: 0.55;
    outline: 1px dashed #e94560;
}

.lightbox-fav {
    position: absolute;
    top: 1rem;
    right: 4.5rem;
    background: rgba(0,0,0,0.5);
    border: none;
    color: white;
    font-size: 1.6rem;
    cursor: pointer;
    z-index: 10;
    width: 44px;
    height: 44px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
}

.lightbox-fav:hover {
    background: rgba(255,255,255,0.2);
}

.lightbox-fav.on {
    color: #ffd45e;
}

/* Storage */
.storage-path {
    font-size: 0.85rem;
    color: #9ecfff;
    margin-bottom: 0.75rem;
    word-break: break-all;
}

.storage-summary {
    display: flex;
    flex-wrap: wrap;
    gap: 1.25rem;
    padding: 0.75rem 0.9rem;
    margin-bottom: 1rem;
    background: #1a1a2e;
    border: 1px solid #0f3460;
    border-radius: 6px;
    font-size: 0.85rem;
}

.storage-summary b {
    color: #eee;
    font-size: 1.05rem;
}

.storage-summary span {
    color: #8a93a8;
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
}

.storage-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
}

.storage-table th {
    text-align: right;
    padding: 0.4rem 0.6rem;
    color: #8a93a8;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border-bottom: 1px solid #0f3460;
}

.storage-table th:first-child,
.storage-table td:first-child {
    text-align: left;
}

.storage-table td {
    padding: 0.4rem 0.6rem;
    border-bottom: 1px solid #16213e;
}

.storage-table tr:hover td {
    background: #16213e;
}

.storage-table .folder-link {
    color: #4db5ff;
    cursor: pointer;
    font-weight: 600;
}

.storage-table .folder-link:hover {
    text-decoration: underline;
}

.storage-table .num {
    text-align: right;
    font-variant-numeric: tabular-nums;
}

.storage-bar {
    height: 6px;
    min-width: 2px;
    background: #e94560;
    border-radius: 3px;
}

.storage-table tfoot td {
    font-weight: 700;
    border-top: 2px solid #0f3460;
    border-bottom: none;
}

/* Conflicts */
.bulk-actions {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
    padding: 0.6rem 0.8rem;
    margin-bottom: 1rem;
    background: #1a1a2e;
    border: 1px solid #0f3460;
    border-radius: 6px;
    font-size: 0.85rem;
}

.conflict-list {
    display: flex;
    flex-direction: column;
    gap: 0.9rem;
}

.conflict {
    border: 1px solid #0f3460;
    border-radius: 6px;
    background: #1a1a2e;
    padding: 0.9rem;
}

.conflict-head {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
    margin-bottom: 0.7rem;
}

.conflict-name {
    font-weight: 600;
    word-break: break-all;
}

.conflict-badge {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 0.15rem 0.5rem;
    border-radius: 10px;
    background: #3a3a1a;
    color: #e8d16f;
}

.conflict-badge.identical {
    background: #1c4532;
    color: #6fdc8c;
}

.conflict-badge.different {
    background: #4a1420;
    color: #ff8ba0;
}

.conflict-sides {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 0.9rem;
}

.conflict-side {
    border: 1px solid #0f3460;
    border-radius: 6px;
    padding: 0.6rem;
    background: #16213e;
}

.conflict-side h4 {
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #8a93a8;
    margin-bottom: 0.5rem;
}

.conflict-side .preview {
    width: 100%;
    height: 180px;
    object-fit: contain;
    background: #0f3460;
    border-radius: 4px;
    display: block;
}

.conflict-side .preview[hidden],
.conflict-side .no-preview[hidden] {
    display: none;
}

.conflict-side .no-preview {
    width: 100%;
    height: 180px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #0f3460;
    border-radius: 4px;
    color: #8a93a8;
    font-size: 0.8rem;
    text-align: center;
    padding: 0.5rem;
}

.conflict-meta {
    margin-top: 0.5rem;
    font-size: 0.78rem;
    color: #8a93a8;
    word-break: break-all;
}

.conflict-meta b {
    color: #eee;
}

.conflict-actions {
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
    margin-top: 0.8rem;
}

.dest-hint {
    margin-top: 0.35rem;
    font-size: 0.78rem;
    color: #9ecfff;
    word-break: break-all;
}

.dest-hint:empty {
    display: none;
}

/* Mounted volumes quick pick (import source) */
.volumes {
    margin-top: 0.6rem;
}

.volumes-head {
    display: flex;
    align-items: center;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #8a93a8;
    margin-bottom: 0.35rem;
}

.volumes-head span {
    margin-right: auto;
}

.btn.tiny {
    padding: 0.05rem 0.4rem;
    font-size: 0.8rem;
    line-height: 1.3;
}

.volume-list {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
}

.vol-btn {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    max-width: 100%;
    padding: 0.25rem 0.6rem;
    background: #1a1a2e;
    border: 1px solid #0f3460;
    border-radius: 12px;
    color: #ccc;
    font-size: 0.8rem;
    font-family: inherit;
    cursor: pointer;
    white-space: nowrap;
}

.vol-btn:hover {
    background: #0f3460;
    color: #eee;
    border-color: #4db5ff;
}

.vol-btn .vol-icon {
    flex-shrink: 0;
    color: #8a93a8;
}

.vol-btn .vol-name {
    overflow: hidden;
    text-overflow: ellipsis;
}

.volume-list .empty-row {
    color: #666;
    font-size: 0.8rem;
}

.options {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    margin: 0.75rem 0 1rem;
}

.check {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: 0.85rem;
    color: #ccc;
    cursor: pointer;
}

.check input[type="number"] {
    width: 100px;
    padding: 0.25rem 0.4rem;
    background: #1a1a2e;
    border: 1px solid #0f3460;
    border-radius: 4px;
    color: #eee;
    font-family: inherit;
}

.actions {
    display: flex;
    align-items: center;
    gap: 1rem;
}

.btn.primary {
    background: #e94560;
    border-color: #e94560;
    color: #fff;
    font-weight: 600;
}

.btn.primary:hover {
    background: #d13a53;
}

.btn.danger {
    border-color: #e94560;
    color: #ff8ba0;
}

.btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
}

.form-error {
    color: #ff8ba0;
    font-size: 0.85rem;
}

.form-ok {
    color: #6fdc8c;
    font-size: 0.85rem;
}

/* Job bar */
.job-bar {
    background: #16213e;
    border-bottom: 1px solid #0f3460;
    padding: 0.75rem 1rem;
}

.job-bar[hidden] {
    display: none;
}

.job-bar-head {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 0.5rem;
}

.job-title {
    font-size: 0.9rem;
    font-weight: 600;
}

.job-badge {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 0.15rem 0.5rem;
    border-radius: 10px;
    background: #0f3460;
    color: #9ecfff;
}

.job-badge.completed { background: #1c4532; color: #6fdc8c; }
.job-badge.failed { background: #4a1420; color: #ff8ba0; }
.job-badge.cancelled { background: #3a3a1a; color: #e8d16f; }

.job-actions {
    margin-left: auto;
    display: flex;
    gap: 0.5rem;
}

.progress {
    height: 8px;
    background: #0f3460;
    border-radius: 4px;
    overflow: hidden;
}

.progress-fill {
    height: 100%;
    width: 0;
    background: #e94560;
    transition: width 0.2s linear;
}

.job-meta {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    font-size: 0.78rem;
    color: #8a93a8;
    margin-top: 0.35rem;
}

.job-meta span {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.job-result {
    font-size: 0.82rem;
    color: #ccc;
    margin-top: 0.4rem;
}

.job-result:empty {
    display: none;
}

/* Batches */
.batch-list {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
}

.batch {
    border: 1px solid #0f3460;
    border-radius: 6px;
    padding: 0.9rem;
    background: #1a1a2e;
}

.batch-head {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
    margin-bottom: 0.5rem;
}

.batch-id {
    font-weight: 600;
    color: #e94560;
}

.batch-status {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 0.15rem 0.5rem;
    border-radius: 10px;
    background: #0f3460;
    color: #9ecfff;
}

.batch-status.completed { background: #1c4532; color: #6fdc8c; }
.batch-status.failed { background: #4a1420; color: #ff8ba0; }
.batch-status.paused { background: #3a3a1a; color: #e8d16f; }

.batch-paths {
    font-size: 0.78rem;
    color: #8a93a8;
    margin-bottom: 0.6rem;
    word-break: break-all;
}

.batch-stats {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    font-size: 0.8rem;
    margin-bottom: 0.7rem;
}

.batch-stats b {
    color: #eee;
}

.batch-actions {
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
}

.batch-failed {
    margin-top: 0.7rem;
    border-top: 1px solid #0f3460;
    padding-top: 0.6rem;
    font-size: 0.78rem;
    color: #ff8ba0;
    max-height: 220px;
    overflow-y: auto;
}

.batch-failed div {
    padding: 0.15rem 0;
    word-break: break-all;
}

/* Job history & server info */
.job-history {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    font-size: 0.82rem;
}

.job-row {
    display: flex;
    gap: 0.75rem;
    align-items: center;
    padding: 0.4rem 0.6rem;
    background: #1a1a2e;
    border: 1px solid #0f3460;
    border-radius: 4px;
}

.job-row .grow {
    flex: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.server-info {
    font-size: 0.82rem;
    color: #8a93a8;
    word-break: break-all;
}

.server-info div {
    padding: 0.15rem 0;
}

/* Directory picker modal */
.modal {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.7);
    display: none;
    align-items: center;
    justify-content: center;
    z-index: 1100;
}

.modal.active {
    display: flex;
}

.modal-box {
    background: #16213e;
    border: 1px solid #0f3460;
    border-radius: 8px;
    width: min(600px, 92vw);
    max-height: 80vh;
    display: flex;
    flex-direction: column;
    padding: 1rem;
    gap: 0.75rem;
}

.modal-head {
    display: flex;
    align-items: center;
}

.modal-head h3 {
    font-size: 1rem;
    margin-right: auto;
}

.modal-path {
    font-size: 0.8rem;
    color: #9ecfff;
    word-break: break-all;
}

.modal-list {
    flex: 1;
    overflow-y: auto;
    border: 1px solid #0f3460;
    border-radius: 4px;
    min-height: 200px;
}

.modal-list .dir-row {
    padding: 0.4rem 0.6rem;
    cursor: pointer;
    font-size: 0.85rem;
    border-bottom: 1px solid #0f3460;
}

.modal-list .dir-row:hover {
    background: #0f3460;
}

.modal-list .empty-row {
    padding: 0.6rem;
    color: #666;
    font-size: 0.85rem;
}

.modal-actions {
    display: flex;
    gap: 0.5rem;
    justify-content: flex-end;
}

/* Scrollbar */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}

::-webkit-scrollbar-track {
    background: #1a1a2e;
}

::-webkit-scrollbar-thumb {
    background: #0f3460;
    border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
    background: #e94560;
}

/* Help overlay */
.help-overlay {
    position: fixed;
    top: 0;
    left: 0;
    width: 100vw;
    height: 100vh;
    background: rgba(0,0,0,0.9);
    display: none;
    align-items: center;
    justify-content: center;
    z-index: 2000;
}

.help-overlay.active {
    display: flex;
}

.help-content {
    background: #16213e;
    border-radius: 12px;
    padding: 2rem;
    max-width: 700px;
    max-height: 90vh;
    overflow-y: auto;
}

.help-content h2 {
    color: #e94560;
    margin-bottom: 1.5rem;
    text-align: center;
    font-size: 1.5rem;
}

.help-columns {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 1.5rem;
}

.help-section h3 {
    color: #4db5ff;
    font-size: 0.9rem;
    margin-bottom: 0.5rem;
    border-bottom: 1px solid #0f3460;
    padding-bottom: 0.3rem;
}

.help-row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.25rem 0;
    font-size: 0.85rem;
}

.help-row kbd {
    background: #0f3460;
    color: #e94560;
    padding: 0.2rem 0.5rem;
    border-radius: 4px;
    font-family: monospace;
    font-size: 0.8rem;
    min-width: 28px;
    text-align: center;
    font-weight: bold;
}

.help-hint {
    text-align: center;
    color: #666;
    margin-top: 1.5rem;
    font-size: 0.85rem;
}

.help-hint kbd {
    background: #0f3460;
    color: #e94560;
    padding: 0.15rem 0.4rem;
    border-radius: 3px;
    font-family: monospace;
}

/* Responsive */
@media (max-width: 768px) {
    .sidebar {
        display: none;
    }

    .file-grid {
        grid-template-columns: repeat(auto-fill, minmax(120px, 1fr));
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
const clearFilterBtn = document.getElementById('btn-clear-filter');
const jobBarEl = document.getElementById('job-bar');
const batchListEl = document.getElementById('batch-list');
const jobHistoryEl = document.getElementById('job-history');
const favoritesGridEl = document.getElementById('favorites-grid');
const lightboxFavEl = document.getElementById('lightbox-fav');
const volumeListEl = document.getElementById('volume-list');
const dirModalEl = document.getElementById('dir-modal');
const dirListEl = document.getElementById('dir-list');
const dirPathEl = document.getElementById('dir-path');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadTreeNode('.'); // Load root only
    loadDirectory('.', 1);

    document.getElementById('btn-grid').addEventListener('click', () => setViewMode('grid'));
    document.getElementById('btn-list').addEventListener('click', () => setViewMode('list'));
    document.getElementById('lightbox-close').addEventListener('click', closeLightbox);
    document.getElementById('lightbox-prev').addEventListener('click', prevImage);
    document.getElementById('lightbox-next').addEventListener('click', nextImage);
    lightboxFavEl.addEventListener('click', (e) => {
        e.stopPropagation();
        toggleFavorite(currentMediaPath);
    });

    // Favorites view
    document.getElementById('btn-refresh-favorites').addEventListener('click', loadFavorites);
    document.getElementById('btn-prune-favorites').addEventListener('click', pruneFavorites);

    // Storage view
    document.getElementById('btn-storage-refresh').addEventListener('click', () => loadStorage(storagePath, true));
    document.getElementById('btn-storage-up').addEventListener('click', () => {
        if (storageParent) loadStorage(storageParent);
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

        // Help overlay works from any view
        const helpOverlay = document.getElementById('help-overlay');
        if (helpOverlay.classList.contains('active')) {
            if (e.key === 'Escape' || e.key === '?' || (e.shiftKey && e.key === '/')) {
                e.preventDefault();
                toggleShortcuts();
            }
            return;
        }
        if ((e.key === '?' || (e.shiftKey && e.key === '/')) && !e.target.closest('input, select, textarea')) {
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
        if (e.target.closest('input, select, textarea')) return;

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

    // Navigate to folder (reset to page 1)
    loadDirectory(path, 1);

    // Update active state
    document.querySelectorAll('.tree-folder.active').forEach(el => el.classList.remove('active'));
    folder.classList.add('active');
});

// Load directory contents with pagination
async function loadDirectory(path, page = 1) {
    currentPath = path;
    currentPage = page;
    resetSelection(); // Reset keyboard selection when changing directory
    fileGridEl.innerHTML = '<div class="loading-indicator">Loading...</div>';
    paginationEl.innerHTML = '';

    try {
        const res = await fetch(`/api/list?path=${encodeURIComponent(path)}&page=${page}&per_page=${perPage}&sort=${sortBy}&order=${sortOrder}`);
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

        renderBreadcrumb(path);
        applyFilter(); // This will render files with current filter
        renderPagination(pag);

        // Update file count
        updateFileCount(pag);

        // Scroll to top
        fileGridEl.scrollTop = 0;

    } catch (err) {
        console.error('Failed to load directory:', err);
        fileGridEl.innerHTML = `<div class="error">Failed to load directory</div>`;
    }
}

// Update file count display
function updateFileCount(pag) {
    const dirCount = pag ? pag.total_dirs : allItems.filter(i => i.is_dir).length;
    const fileCount = pag ? pag.total_files : allItems.filter(i => !i.is_dir).length;
    const showingStart = pag ? (pag.page - 1) * pag.per_page + 1 : 1;
    const showingEnd = pag ? Math.min(pag.page * pag.per_page, pag.total_files) : fileCount;

    if (filterText) {
        const filtered = getFilteredItems();
        fileCountEl.textContent = `Filter: ${filtered.length} matches`;
    } else if (pag && pag.total_files > pag.per_page) {
        fileCountEl.textContent = `${dirCount} folders | Showing ${showingStart}-${showingEnd} of ${pag.total_files} files`;
    } else {
        fileCountEl.textContent = `${dirCount} folders, ${fileCount} files`;
    }
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

    // Update media for lightbox
    media = filtered.filter(i => i.is_image || i.is_video);

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

// Render breadcrumb navigation
function renderBreadcrumb(path) {
    const parts = path === '.' ? [] : path.split('/');
    let html = `<a href="#" data-path=".">Home</a>`;

    let currentPath = '';
    for (const part of parts) {
        currentPath += (currentPath ? '/' : '') + part;
        html += `<span class="separator">/</span>`;
        html += `<a href="#" data-path="${escapeHtml(currentPath)}">${escapeHtml(part)}</a>`;
    }

    breadcrumbEl.innerHTML = html;
}

// Breadcrumb click handler
breadcrumbEl.addEventListener('click', (e) => {
    if (e.target.tagName === 'A') {
        e.preventDefault();
        loadDirectory(e.target.dataset.path, 1);
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
    html += `<button class="page-btn" ${pag.page <= 1 ? 'disabled' : ''} data-page="${pag.page - 1}" data-shortcut="[" title="Previous page ([)">&laquo; Prev</button>`;

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
    html += `<button class="page-btn" ${pag.page >= pag.total_pages ? 'disabled' : ''} data-page="${pag.page + 1}" data-shortcut="]" title="Next page (])">Next &raquo;</button>`;

    // Page info
    html += `<span class="page-info">Page ${pag.page} of ${pag.total_pages}</span>`;

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

    for (const item of items) {
        html += renderFileItem(item);
    }

    grid.innerHTML = html || '<div class="empty">No files in this directory</div>';
}

function renderFileItem(item) {
    const path = escapeHtml(item.path);
    const name = escapeHtml(item.name);

    if (item.is_dir) {
        return `
            <div class="file-item folder" data-path="${path}">
                <div class="file-icon">&#128193;</div>
                <div class="file-name">${name}</div>
            </div>
        `;
    }

    const star = `<button class="fav-toggle ${item.favorite ? 'on' : ''}" data-fav="${path}"
                          title="${item.favorite ? 'Remove from favorites' : 'Add to favorites'}"
                  >${item.favorite ? '\\u2605' : '\\u2606'}</button>`;
    const missing = item.missing ? ' missing' : '';

    if (item.is_image) {
        const thumb = item.missing
            ? '<div class="file-icon">&#10071;</div>'
            : `<img class="file-thumb" src="/api/thumbnail/${encodeURIComponent(item.path)}" alt="${name}" loading="lazy">`;
        return `
            <div class="file-item image${missing}" data-path="${path}">
                ${thumb}${star}
                <div class="file-name">${name}</div>
            </div>
        `;
    }

    if (item.is_video) {
        return `
            <div class="file-item video${missing}" data-path="${path}">
                <div class="video-icon">&#9658;</div>${star}
                <div class="file-name">${name}</div>
            </div>
        `;
    }

    return `
        <div class="file-item${missing}" data-path="${path}">
            <div class="file-icon">&#128196;</div>${star}
            <div class="file-name">${name}</div>
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
        loadDirectory(item.dataset.path, 1);
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

    lightboxInfoEl.textContent = item ? `${item.name} (${formatSize(item.size)})` : path;
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
    if (item.classList.contains('folder')) {
        loadDirectory(item.dataset.path, 1);
    } else if (item.classList.contains('image') || item.classList.contains('video')) {
        openLightbox(item.dataset.path);
    }
}

function goToParentDirectory() {
    if (currentPath === '.') return;
    const parent = currentPath.split('/').slice(0, -1).join('/') || '.';
    loadDirectory(parent, 1);
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
                loadDirectory(currentFolder.dataset.path, 1);
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
    bindSegmented('quick-copy-media', (media) => { copyMedia = media; });
    bindSegmented('batches-media', (media) => { batchesMedia = media; loadBatches(); });

    // Actions
    document.getElementById('btn-scan').addEventListener('click', startScan);
    document.getElementById('btn-quick-copy').addEventListener('click', startQuickCopy);
    document.getElementById('btn-expand').addEventListener('click', startExpand);
    document.getElementById('btn-refresh-batches').addEventListener('click', loadBatches);
    document.getElementById('btn-refresh-jobs').addEventListener('click', () => renderJobHistory(latestJobs));
    document.getElementById('job-cancel').addEventListener('click', cancelWatchedJob);
    document.getElementById('job-dismiss').addEventListener('click', () => {
        dismissedJobId = watchedJobId;
        jobBarEl.hidden = true;
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
    // Newly copied files may have appeared in the served tree
    if (currentView === 'browse') {
        loadedTreePaths.clear();
        loadTreeNode('.');
        loadDirectory(currentPath, currentPage);
    }
}

function renderJobBar(job) {
    if (!job || job.id === dismissedJobId) {
        jobBarEl.hidden = true;
        return;
    }

    jobBarEl.hidden = false;
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
            <span class="job-badge ${job.status}">${escapeHtml(job.status)}</span>
            <span class="grow">${escapeHtml(job.title)}</span>
            <span>${escapeHtml(job.started_at.replace('T', ' '))}</span>
            <span>${formatSeconds(job.elapsed_seconds)}</span>
        </div>
    `).join('');
}

function renderServerInfo() {
    document.getElementById('server-info').innerHTML = `
        <div>Serving: ${escapeHtml(serverConfig.root || '')}</div>
        <div>Photo database: ${escapeHtml(serverConfig.db_path || '')}</div>
        <div>Video database: ${escapeHtml(serverConfig.video_db_path || '')}</div>
        <div>Default workers: ${serverConfig.default_workers}</div>
        <div>Thumbnails (Pillow): ${serverConfig.has_pil ? 'available' : 'not installed'}</div>
    `;
}

// ---------------------------------------------------------------------------
// Batches
// ---------------------------------------------------------------------------

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

        return `
        <div class="batch" data-id="${batch.id}">
            <div class="batch-head">
                <span class="batch-id">Batch #${batch.id}</span>
                <span class="batch-status ${escapeHtml(batch.status)}">${escapeHtml(batch.status)}</span>
                <span class="card-hint" style="margin:0">${escapeHtml((batch.started_at || '').replace('T', ' '))}</span>
            </div>
            <div class="batch-paths">
                ${escapeHtml(batch.source_directory)} &rarr; ${escapeHtml(batch.target_directory)}
            </div>
            <div class="batch-stats">
                <span>Total <b>${stats.total || 0}</b></span>
                <span>Pending <b>${stats.pending || 0}</b></span>
                <span>Copied <b>${stats.copied || 0}</b></span>
                <span>Skipped <b>${stats.skipped || 0}</b></span>
                <span>Failed <b>${stats.failed || 0}</b></span>
                <span>Conflicts <b>${stats.conflicts || 0}</b></span>
                <span>${datedLabel} <b>${dated}</b></span>
                <span>Size <b>${formatSize(stats.total_size || 0)}</b></span>
            </div>
            <div class="batch-actions">
                <button class="btn primary" data-action="copy" data-id="${batch.id}"
                    ${stats.pending ? '' : 'disabled'}>Copy ${stats.pending || 0} pending</button>
                <button class="btn" data-action="dry" data-id="${batch.id}"
                    ${stats.pending ? '' : 'disabled'}>Dry run</button>
                <button class="btn" data-action="conflicts" data-id="${batch.id}"
                    ${stats.conflicts ? '' : 'disabled'}>Review ${stats.conflicts || 0} conflicts</button>
                <button class="btn" data-action="retry" data-id="${batch.id}"
                    ${stats.failed ? '' : 'disabled'}>Retry ${stats.failed || 0} failed</button>
                <button class="btn" data-action="failed" data-id="${batch.id}"
                    ${stats.failed ? '' : 'disabled'}>Show failed</button>
                <button class="btn" data-action="browse" data-id="${batch.id}"
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
        loadDirectory('.', 1);
        return;
    }
    if (targetPath.startsWith(root + '/')) {
        setView('browse');
        loadDirectory(targetPath.slice(root.length + 1), 1);
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
            <td class="num">${data.loose.videos.toLocaleString()}</td>
            <td class="num">${data.loose.other.toLocaleString()}</td>
            <td class="num">${formatGB(data.loose.bytes)}</td>
            <td></td>
        </tr>` : '';

    document.getElementById('storage-table').innerHTML = `
        <table class="storage-table">
            <thead>
                <tr>
                    <th>Folder</th><th>Photos</th><th>Videos</th><th>Other</th><th>Size</th><th style="width:25%"></th>
                </tr>
            </thead>
            <tbody>${rows}${loose}</tbody>
            <tfoot>
                <tr>
                    <td>Total</td>
                    <td class="num">${totals.images.toLocaleString()}</td>
                    <td class="num">${totals.videos.toLocaleString()}</td>
                    <td class="num">${totals.other.toLocaleString()}</td>
                    <td class="num">${formatGB(totals.bytes)}</td>
                    <td></td>
                </tr>
            </tfoot>
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
            </div>
            <div class="conflict-sides">
                ${renderConflictSide('Imported (new)', conflict.incoming)}
                ${renderConflictSide('Already in library', conflict.existing)}
            </div>
            <div class="conflict-actions">
                <button class="btn" data-action="skip" data-id="${conflict.id}">Keep existing</button>
                <button class="btn" data-action="keep_both" data-id="${conflict.id}">Keep both</button>
                <button class="btn danger" data-action="overwrite" data-id="${conflict.id}">Replace with imported</button>
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
                <div class="conflict-meta">${escapeHtml(side && side.path ? side.path : 'unknown path')}</div>
            </div>
        `;
    }

    const preview = side.is_image
        ? `<img class="preview" src="/api/preview?path=${encodeURIComponent(side.path)}&size=400" alt=""
                onerror="this.hidden=true; this.nextElementSibling.hidden=false;">
           <div class="no-preview" hidden>No preview for this format</div>`
        : '<div class="no-preview">No preview (video or unsupported format)</div>';

    const taken = side.taken_at ? `<div>Taken: <b>${escapeHtml(side.taken_at.replace('T', ' '))}</b></div>` : '';

    return `
        <div class="conflict-side">
            <h4>${escapeHtml(title)}</h4>
            ${preview}
            <div class="conflict-meta">
                <div>Size: <b>${formatSize(side.size)}</b></div>
                ${taken}
                <div>Modified: ${escapeHtml(side.modified.replace('T', ' '))}</div>
                <div>${escapeHtml(side.path)}</div>
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

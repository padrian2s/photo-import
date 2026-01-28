"""
Web server for browsing photos in hierarchical directory structure.
"""

import json
import mimetypes
import os
import urllib.parse
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Optional
from io import BytesIO

from .expander import get_directory_tree, list_images_in_directory

# Try to import PIL for thumbnail generation
try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False


class PhotoBrowserHandler(SimpleHTTPRequestHandler):
    """HTTP request handler for photo browser."""

    root_directory: str = "."
    thumbnail_size: tuple = (200, 200)

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

    def send_json(self, data: dict, status: int = 200):
        """Send JSON response."""
        content = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(content))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(content)

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
        try:
            for item in target.iterdir():
                if item.name.startswith('.'):
                    continue

                try:
                    stat = item.stat()
                except (PermissionError, OSError):
                    continue

                info = {
                    "name": item.name,
                    "path": str(item.relative_to(root)),
                    "is_dir": item.is_dir(),
                    "modified": stat.st_mtime,
                    "accessed": stat.st_atime,
                    "created": getattr(stat, 'st_birthtime', stat.st_ctime),
                }

                if item.is_file():
                    ext = item.suffix.lower()
                    info["extension"] = ext
                    info["size"] = stat.st_size
                    info["is_image"] = ext in {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.heic', '.heif', '.bmp', '.tiff', '.tif'}
                    info["is_video"] = ext in {'.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v', '.wmv', '.flv', '.3gp', '.mts', '.m2ts'}
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
            with Image.open(full_path) as img:
                # Handle EXIF orientation
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

                # Convert to RGB if necessary
                if img.mode in ('RGBA', 'P'):
                    img = img.convert('RGB')

                img.thumbnail(self.thumbnail_size, Image.Resampling.LANCZOS)

                buffer = BytesIO()
                img.save(buffer, format='JPEG', quality=85)
                content = buffer.getvalue()

                self.send_response(200)
                self.send_header('Content-Type', 'image/jpeg')
                self.send_header('Content-Length', len(content))
                self.send_header('Cache-Control', 'max-age=3600')
                self.end_headers()
                self.wfile.write(content)

        except Exception as e:
            self.send_error(500, f"Error generating thumbnail: {e}")

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


class QuietHTTPServer(HTTPServer):
    """HTTP server that silently handles client disconnection errors."""

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
):
    """
    Run the photo browser web server.

    Args:
        directory: Root directory to serve
        port: Port number
        host: Host to bind to
        open_browser: Whether to open browser automatically
    """
    # Set class variable for the handler
    PhotoBrowserHandler.root_directory = str(Path(directory).resolve())

    server = QuietHTTPServer((host, port), PhotoBrowserHandler)
    url = f"http://{host}:{port}"

    print(f"\n{'='*50}")
    print(f"Photo Browser Server")
    print(f"{'='*50}")
    print(f"Serving: {PhotoBrowserHandler.root_directory}")
    print(f"URL: {url}")
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
            <h1>Photo Browser</h1>
            <div class="breadcrumb" id="breadcrumb"></div>
        </header>

        <div class="main">
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
                    <button class="btn" id="btn-help" title="Show shortcuts (?)" data-shortcut="?">?</button>
                    <span class="file-count" id="file-count"></span>
                </div>

                <div class="file-grid" id="file-grid"></div>

                <div class="pagination" id="pagination"></div>
            </main>
        </div>

        <!-- Lightbox overlay -->
        <div class="lightbox" id="lightbox">
            <button class="lightbox-close" id="lightbox-close">&times;</button>
            <button class="lightbox-nav lightbox-prev" id="lightbox-prev">&lt;</button>
            <button class="lightbox-nav lightbox-next" id="lightbox-next">&gt;</button>
            <div class="lightbox-content">
                <img id="lightbox-img" src="" alt="">
                <video id="lightbox-video" controls style="display:none;"></video>
            </div>
            <div class="lightbox-info" id="lightbox-info"></div>
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

.main {
    display: flex;
    flex: 1;
    overflow: hidden;
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
let filterText = ''; // Current filter text
let allItems = []; // All items in current directory (for filtering)

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

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadTreeNode('.'); // Load root only
    loadDirectory('.', 1);

    document.getElementById('btn-grid').addEventListener('click', () => setViewMode('grid'));
    document.getElementById('btn-list').addEventListener('click', () => setViewMode('list'));
    document.getElementById('lightbox-close').addEventListener('click', closeLightbox);
    document.getElementById('lightbox-prev').addEventListener('click', prevImage);
    document.getElementById('lightbox-next').addEventListener('click', nextImage);

    // Per page selector
    perPageSelect.addEventListener('change', (e) => {
        perPage = parseInt(e.target.value);
        loadDirectory(currentPath, 1); // Reset to page 1
    });

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

        if (lightboxEl.classList.contains('active')) {
            if (e.key === 'Escape') closeLightbox();
            if (e.key === 'ArrowLeft') prevImage();
            if (e.key === 'ArrowRight') nextImage();
        } else {
            // Global shortcuts (work in any panel)
            const key = e.key.toLowerCase();
            const helpOverlay = document.getElementById('help-overlay');

            // Close help overlay on Escape or ?
            if (helpOverlay.classList.contains('active')) {
                if (e.key === 'Escape' || key === '?' || (e.shiftKey && key === '/')) {
                    e.preventDefault();
                    toggleShortcuts();
                    return;
                }
                return; // Ignore other keys when help is open
            }

            if (key === '?' || (e.shiftKey && key === '/')) {
                e.preventDefault();
                toggleShortcuts();
                return;
            }
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
            if (e.key === 'Tab' && !e.target.closest('input, select, textarea')) {
                e.preventDefault();
                switchFocusedPanel();
                return;
            }
            // Route to appropriate panel handler
            if (focusedPanel === 'tree') {
                handleTreeKeyNavigation(e);
            } else {
                handleGridKeyNavigation(e);
            }
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
function renderFiles(items) {
    let html = '';

    for (const item of items) {
        if (item.is_dir) {
            html += `
                <div class="file-item folder" data-path="${escapeHtml(item.path)}">
                    <div class="file-icon">&#128193;</div>
                    <div class="file-name">${escapeHtml(item.name)}</div>
                </div>
            `;
        } else if (item.is_image) {
            html += `
                <div class="file-item image" data-path="${escapeHtml(item.path)}">
                    <img class="file-thumb" src="/api/thumbnail/${encodeURIComponent(item.path)}"
                         alt="${escapeHtml(item.name)}" loading="lazy">
                    <div class="file-name">${escapeHtml(item.name)}</div>
                </div>
            `;
        } else if (item.is_video) {
            html += `
                <div class="file-item video" data-path="${escapeHtml(item.path)}">
                    <div class="video-icon">&#9658;</div>
                    <div class="file-name">${escapeHtml(item.name)}</div>
                </div>
            `;
        } else {
            html += `
                <div class="file-item" data-path="${escapeHtml(item.path)}">
                    <div class="file-icon">&#128196;</div>
                    <div class="file-name">${escapeHtml(item.name)}</div>
                </div>
            `;
        }
    }

    fileGridEl.innerHTML = html || '<div class="empty">No files in this directory</div>';
}

// File click handler
fileGridEl.addEventListener('click', (e) => {
    const item = e.target.closest('.file-item');
    if (!item) return;

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

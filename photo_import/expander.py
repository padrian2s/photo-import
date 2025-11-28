"""
Directory expander - converts flat date-named directories to hierarchical structure.

Transforms directories like:
    2012_05_20/ -> 2012/05/20/
    2012-05-20/ -> 2012/05/20/
    20120520/   -> 2012/05/20/
"""

import os
import re
import shutil
import logging
from pathlib import Path
from typing import Optional, Callable, List, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Patterns for date-named directories
DATE_PATTERNS = [
    # 2012_05_20 or 2012-05-20
    (re.compile(r'^(\d{4})[-_](\d{2})[-_](\d{2})$'), '{0}/{1}/{2}'),
    # 20120520
    (re.compile(r'^(\d{4})(\d{2})(\d{2})$'), '{0}/{1}/{2}'),
    # 2012_05 or 2012-05
    (re.compile(r'^(\d{4})[-_](\d{2})$'), '{0}/{1}'),
    # 201205
    (re.compile(r'^(\d{4})(\d{2})$'), '{0}/{1}'),
]


@dataclass
class ExpandResult:
    """Result of directory expansion."""
    source_dir: str
    target_dir: str
    dirs_processed: int
    dirs_skipped: int
    files_moved: int
    errors: List[Tuple[str, str]]  # (path, error message)


def parse_date_directory(dirname: str) -> Optional[str]:
    """
    Parse a date-based directory name and return the hierarchical path.

    Args:
        dirname: Directory name like '2012_05_20'

    Returns:
        Hierarchical path like '2012/05/20' or None if not a date directory
    """
    for pattern, template in DATE_PATTERNS:
        match = pattern.match(dirname)
        if match:
            groups = match.groups()
            # Validate date components
            year = int(groups[0])
            month = int(groups[1])
            if year < 1900 or year > 2100:
                continue
            if month < 1 or month > 12:
                continue
            if len(groups) > 2:
                day = int(groups[2])
                if day < 1 or day > 31:
                    continue
            return template.format(*groups)
    return None


def expand_directories(
    source_dir: str,
    target_dir: Optional[str] = None,
    dry_run: bool = False,
    move_files: bool = False,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> ExpandResult:
    """
    Expand flat date directories into hierarchical structure.

    Args:
        source_dir: Source directory containing date-named subdirectories
        target_dir: Target directory for expanded structure (default: same as source)
        dry_run: If True, only simulate the expansion
        move_files: If True, move files instead of copying (only if target != source)
        progress_callback: Callback(processed, total, current_dir)

    Returns:
        ExpandResult with statistics
    """
    source_path = Path(source_dir).resolve()
    target_path = Path(target_dir).resolve() if target_dir else source_path

    if not source_path.is_dir():
        raise ValueError(f"Source directory does not exist: {source_path}")

    # Find all date-named directories
    date_dirs = []
    for item in source_path.iterdir():
        if item.is_dir():
            expanded = parse_date_directory(item.name)
            if expanded:
                date_dirs.append((item, expanded))

    result = ExpandResult(
        source_dir=str(source_path),
        target_dir=str(target_path),
        dirs_processed=0,
        dirs_skipped=0,
        files_moved=0,
        errors=[],
    )

    total = len(date_dirs)

    for idx, (dir_path, expanded_path) in enumerate(date_dirs):
        if progress_callback:
            progress_callback(idx, total, dir_path.name)

        try:
            new_path = target_path / expanded_path

            if dry_run:
                logger.info(f"Would expand: {dir_path.name} -> {expanded_path}")
                result.dirs_processed += 1
                # Count files that would be moved
                for _ in dir_path.rglob('*'):
                    if _.is_file():
                        result.files_moved += 1
                continue

            # Create target directory structure
            new_path.mkdir(parents=True, exist_ok=True)

            # Move/copy contents
            for item in dir_path.iterdir():
                dest = new_path / item.name
                if item.is_file():
                    if source_path == target_path or move_files:
                        shutil.move(str(item), str(dest))
                    else:
                        shutil.copy2(str(item), str(dest))
                    result.files_moved += 1
                elif item.is_dir():
                    # Handle nested directories
                    if dest.exists():
                        # Merge into existing
                        for sub_item in item.rglob('*'):
                            if sub_item.is_file():
                                rel_path = sub_item.relative_to(item)
                                sub_dest = dest / rel_path
                                sub_dest.parent.mkdir(parents=True, exist_ok=True)
                                if source_path == target_path or move_files:
                                    shutil.move(str(sub_item), str(sub_dest))
                                else:
                                    shutil.copy2(str(sub_item), str(sub_dest))
                                result.files_moved += 1
                    else:
                        if source_path == target_path or move_files:
                            shutil.move(str(item), str(dest))
                        else:
                            shutil.copytree(str(item), str(dest))
                        # Count files
                        for _ in dest.rglob('*'):
                            if _.is_file():
                                result.files_moved += 1

            # Remove original directory if it's in-place expansion
            if source_path == target_path:
                # Check if directory is empty
                remaining = list(dir_path.iterdir())
                if not remaining:
                    dir_path.rmdir()
                else:
                    logger.warning(f"Directory not empty after move: {dir_path}")

            result.dirs_processed += 1

        except Exception as e:
            logger.error(f"Error processing {dir_path}: {e}")
            result.errors.append((str(dir_path), str(e)))

    if progress_callback:
        progress_callback(total, total, "Done")

    return result


def get_directory_tree(root_dir: str, max_depth: int = 4) -> dict:
    """
    Get directory tree structure for web browsing.

    Args:
        root_dir: Root directory to scan
        max_depth: Maximum depth to scan

    Returns:
        Dictionary with tree structure
    """
    root_path = Path(root_dir).resolve()

    def scan_dir(path: Path, depth: int) -> dict:
        if depth > max_depth:
            return {"type": "truncated"}

        result = {
            "name": path.name or str(path),
            "path": str(path.relative_to(root_path)) if path != root_path else ".",
            "type": "directory",
            "children": [],
        }

        try:
            items = sorted(path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))

            for item in items:
                if item.name.startswith('.'):
                    continue

                if item.is_dir():
                    child = scan_dir(item, depth + 1)
                    result["children"].append(child)
                elif item.is_file():
                    ext = item.suffix.lower()
                    result["children"].append({
                        "name": item.name,
                        "path": str(item.relative_to(root_path)),
                        "type": "file",
                        "extension": ext,
                        "size": item.stat().st_size,
                        "is_image": ext in {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.heic', '.heif', '.bmp', '.tiff', '.tif'},
                    })
        except PermissionError:
            result["error"] = "Permission denied"

        return result

    return scan_dir(root_path, 0)


def list_images_in_directory(root_dir: str, relative_path: str = ".") -> List[dict]:
    """
    List all images in a specific directory.

    Args:
        root_dir: Root directory
        relative_path: Relative path within root

    Returns:
        List of image info dictionaries
    """
    root_path = Path(root_dir).resolve()
    target_path = root_path / relative_path if relative_path != "." else root_path

    if not target_path.is_dir():
        return []

    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.heic', '.heif', '.bmp', '.tiff', '.tif'}
    images = []

    for item in sorted(target_path.iterdir(), key=lambda x: x.name.lower()):
        if item.is_file() and item.suffix.lower() in image_extensions:
            images.append({
                "name": item.name,
                "path": str(item.relative_to(root_path)),
                "size": item.stat().st_size,
                "extension": item.suffix.lower(),
            })

    return images

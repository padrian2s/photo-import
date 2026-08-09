"""
Favorites for the photo browser.

Favorites are stored per served library, keyed by the path relative to the
served root, so they survive the drive being mounted somewhere else.
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator, List, Set

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS favorites (
    path TEXT PRIMARY KEY,
    added_at TIMESTAMP NOT NULL
);
"""


class FavoritesStore:
    """SQLite-backed set of favorite files."""

    def __init__(self, db_path: str | Path = "photo_favorites.db"):
        self.db_path = Path(db_path)
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)

    @contextmanager
    def _connect(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def add(self, path: str) -> bool:
        """Mark a path as favorite. Returns True if it was newly added."""
        with self._connect() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO favorites (path, added_at) VALUES (?, ?)",
                (path, datetime.now()),
            )
            return cursor.rowcount > 0

    def remove(self, path: str) -> bool:
        """Remove a favorite. Returns True if it existed."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM favorites WHERE path = ?", (path,))
            return cursor.rowcount > 0

    def contains(self, path: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM favorites WHERE path = ?", (path,)
            ).fetchone()
        return row is not None

    def all_paths(self) -> Set[str]:
        """Every favorite path - used to flag directory listings."""
        with self._connect() as conn:
            return {row['path'] for row in conn.execute("SELECT path FROM favorites")}

    def list(self, limit: int = 500) -> List[dict]:
        """Favorites, newest first."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT path, added_at FROM favorites ORDER BY added_at DESC LIMIT ?",
                (limit,),
            ).fetchall()

        return [{"path": row['path'], "added_at": str(row['added_at'])} for row in rows]

    def count(self) -> int:
        with self._connect() as conn:
            return conn.execute("SELECT COUNT(*) FROM favorites").fetchone()[0]

    def prune_missing(self, root: Path) -> int:
        """Drop favorites whose file no longer exists. Returns how many went."""
        removed = 0
        for path in self.all_paths():
            if not (root / path).exists():
                self.remove(path)
                removed += 1
        return removed

"""
Background job runner that exposes the CLI operations (scan, copy, retry,
expand) to the web UI.

Jobs run in daemon threads and report progress through a shared registry that
the HTTP handler polls. Only one job runs at a time - these operations are
disk-bound and the underlying batches are not designed to be mutated
concurrently.
"""

import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional

from .database import Database
from .video_database import VideoDatabase

logger = logging.getLogger(__name__)

MAX_HISTORY = 50

PHOTO = "photo"
VIDEO = "video"

# What counts as a photo worth asking about a camera
CAMERA_EXTENSIONS = {
    '.jpg', '.jpeg', '.jpe', '.jif', '.jfif', '.tif', '.tiff', '.png',
    '.heic', '.heif', '.webp', '.bmp',
    '.raw', '.cr2', '.cr3', '.nef', '.arw', '.dng', '.orf', '.rw2', '.pef',
    '.srw', '.raf',
}


class JobCancelled(Exception):
    """Raised inside a worker thread when the job has been cancelled."""


class JobBusy(Exception):
    """Raised when a job is requested while another one is still running."""


@dataclass
class Job:
    """A single background operation."""

    id: int
    kind: str          # scan | copy | retry | expand
    media: str         # photo | video | files
    title: str
    params: dict = field(default_factory=dict)
    status: str = "running"   # running | completed | cancelled | failed
    current: int = 0
    total: int = 0
    current_file: str = ""
    batch_id: Optional[int] = None
    result: Optional[dict] = None
    error: Optional[str] = None
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: Optional[datetime] = None
    _cancel: threading.Event = field(default_factory=threading.Event, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def update(self, current: int, total: int, current_file: str = ""):
        with self._lock:
            self.current = current
            self.total = total
            self.current_file = current_file

    def finish(self, status: str, result: Optional[dict] = None, error: Optional[str] = None):
        with self._lock:
            self.status = status
            self.result = result
            self.error = error
            self.finished_at = datetime.now()

    def cancel(self):
        self._cancel.set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancel.is_set()

    def check_cancelled(self):
        if self._cancel.is_set():
            raise JobCancelled()

    def to_dict(self) -> dict:
        with self._lock:
            elapsed = (self.finished_at or datetime.now()) - self.started_at
            percent = (100.0 * self.current / self.total) if self.total else 0.0
            return {
                "id": self.id,
                "kind": self.kind,
                "media": self.media,
                "title": self.title,
                "params": self.params,
                "status": self.status,
                "current": self.current,
                "total": self.total,
                "percent": round(percent, 1),
                "current_file": Path(self.current_file).name if self.current_file else "",
                "batch_id": self.batch_id,
                "result": self.result,
                "error": self.error,
                "started_at": self.started_at.isoformat(timespec="seconds"),
                "finished_at": self.finished_at.isoformat(timespec="seconds") if self.finished_at else None,
                "elapsed_seconds": int(elapsed.total_seconds()),
            }


class JobManager:
    """Owns the databases and runs CLI operations in background threads."""

    def __init__(self, db_path: str = "photo_import.db", video_db_path: str = "video_import.db"):
        self.db_path = str(db_path)
        self.video_db_path = str(video_db_path)
        self.db = Database(self.db_path)
        self.video_db = VideoDatabase(self.video_db_path)

        self._jobs: Dict[int, Job] = {}
        self._order: List[int] = []
        self._next_id = 1
        self._lock = threading.Lock()

    # -------------------------------------------------------------------------
    # Registry
    # -------------------------------------------------------------------------

    def database_for(self, media: str):
        """Return the database matching a media type."""
        if media == VIDEO:
            return self.video_db
        if media == PHOTO:
            return self.db
        raise ValueError(f"Unknown media type: {media}")

    def get(self, job_id: int) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self, limit: int = 20) -> List[Job]:
        with self._lock:
            ids = self._order[-limit:]
            return [self._jobs[i] for i in reversed(ids)]

    def active_job(self) -> Optional[Job]:
        with self._lock:
            for job_id in reversed(self._order):
                job = self._jobs[job_id]
                if job.status == "running":
                    return job
        return None

    def cancel(self, job_id: int) -> bool:
        job = self.get(job_id)
        if not job or job.status != "running":
            return False
        job.cancel()
        return True

    def _create(self, kind: str, media: str, title: str, params: dict) -> Job:
        with self._lock:
            for existing_id in reversed(self._order):
                if self._jobs[existing_id].status == "running":
                    raise JobBusy(
                        f"Job #{existing_id} ({self._jobs[existing_id].title}) is still running"
                    )

            job = Job(id=self._next_id, kind=kind, media=media, title=title, params=params)
            self._next_id += 1
            self._jobs[job.id] = job
            self._order.append(job.id)

            # Trim finished history
            while len(self._order) > MAX_HISTORY:
                dropped = self._order.pop(0)
                self._jobs.pop(dropped, None)

        return job

    def _spawn(self, job: Job, work: Callable[[Job], dict]):
        def target():
            try:
                result = work(job)
                job.finish("completed", result=result)
            except JobCancelled:
                logger.info("Job #%s cancelled", job.id)
                job.finish("cancelled", result={"note": "Progress has been saved"})
            except Exception as exc:  # noqa: BLE001 - surfaced to the UI
                logger.exception("Job #%s failed", job.id)
                job.finish("failed", error=str(exc))

        thread = threading.Thread(target=target, name=f"job-{job.id}", daemon=True)
        thread.start()
        return job

    def _progress_callback(self, job: Job) -> Callable[[int, int, str], None]:
        def callback(current: int, total: int, current_file: str = ""):
            job.check_cancelled()
            job.update(current, total, current_file)

        return callback

    # -------------------------------------------------------------------------
    # Operations
    # -------------------------------------------------------------------------

    def start_scan(
        self,
        media: str,
        source: str,
        target: str,
        checksum: bool = True,
        resume: bool = True,
        workers: Optional[int] = None,
    ) -> Job:
        """Scan a source directory (photos or videos)."""
        source_path = Path(source).expanduser().resolve()
        target_path = Path(target).expanduser().resolve()

        if not source_path.is_dir():
            raise ValueError(f"Source directory not found: {source_path}")
        if not target:
            raise ValueError("Target directory is required")

        job = self._create(
            "scan", media,
            f"{'Video' if media == VIDEO else 'Photo'} scan: {source_path.name or source_path}",
            {
                "source": str(source_path),
                "target": str(target_path),
                "checksum": checksum,
                "resume": resume,
                "workers": workers,
            },
        )

        def work(job: Job) -> dict:
            if media == VIDEO:
                from .video_scanner import VideoScanner

                scanner = VideoScanner(
                    self.video_db,
                    calculate_checksums=checksum,
                    progress_callback=self._progress_callback(job),
                    num_workers=workers,
                )
            else:
                from .scanner import PhotoScanner

                scanner = PhotoScanner(
                    self.db,
                    calculate_checksums=checksum,
                    progress_callback=self._progress_callback(job),
                    num_workers=workers,
                )

            batch = scanner.scan(source_path, target_path, resume=resume)
            job.batch_id = batch.id
            stats = self.database_for(media).get_batch_stats(batch.id)
            return {"batch_id": batch.id, "stats": _clean_stats(stats)}

        return self._spawn(job, work)

    def start_copy(
        self,
        media: str,
        batch_id: Optional[int] = None,
        dry_run: bool = False,
        skip_no_date: bool = False,
        use_file_date: bool = True,
    ) -> Job:
        """Copy the pending files of a batch to its target directory."""
        db = self.database_for(media)

        if batch_id is None:
            batch = db.get_latest_batch()
            if not batch:
                raise ValueError("No batches found - run a scan first")
            batch_id = batch.id
        else:
            batch = db.get_batch(batch_id)
            if not batch:
                raise ValueError(f"Batch {batch_id} not found")

        label = "Video" if media == VIDEO else "Photo"
        job = self._create(
            "copy", media,
            f"{label} copy: batch #{batch_id}{' (dry run)' if dry_run else ''}",
            {
                "batch_id": batch_id,
                "dry_run": dry_run,
                "skip_no_date": skip_no_date,
                "use_file_date": use_file_date,
            },
        )
        job.batch_id = batch_id

        def work(job: Job) -> dict:
            if media == VIDEO:
                from .video_copier import VideoCopier

                copier = VideoCopier(
                    self.video_db,
                    use_file_date_fallback=use_file_date,
                    skip_no_metadata=skip_no_date,
                    progress_callback=self._progress_callback(job),
                )
            else:
                from .copier import PhotoCopier

                copier = PhotoCopier(
                    self.db,
                    use_file_date_fallback=use_file_date,
                    skip_no_exif=skip_no_date,
                    progress_callback=self._progress_callback(job),
                )

            stats = copier.copy(batch_id, dry_run=dry_run)
            return _copy_result(batch_id, stats)

        return self._spawn(job, work)

    def start_retry(self, media: str, batch_id: int) -> Job:
        """Retry the failed files of a batch."""
        db = self.database_for(media)
        batch = db.get_batch(batch_id)
        if not batch:
            raise ValueError(f"Batch {batch_id} not found")

        stats = db.get_batch_stats(batch_id)
        if not stats.get("failed"):
            raise ValueError("No failed files to retry")

        label = "Video" if media == VIDEO else "Photo"
        job = self._create(
            "retry", media,
            f"{label} retry: batch #{batch_id}",
            {"batch_id": batch_id},
        )
        job.batch_id = batch_id

        def work(job: Job) -> dict:
            if media == VIDEO:
                from .video_copier import VideoCopier

                copier = VideoCopier(
                    self.video_db, progress_callback=self._progress_callback(job)
                )
            else:
                from .copier import PhotoCopier

                copier = PhotoCopier(
                    self.db, progress_callback=self._progress_callback(job)
                )

            result = copier.retry_failed(batch_id)
            return _copy_result(batch_id, result)

        return self._spawn(job, work)

    def start_resolve(
        self,
        media: str,
        batch_id: int,
        action: str,
        file_ids: Optional[List[int]] = None,
    ) -> Job:
        """Apply a conflict decision: overwrite, keep_both or skip."""
        if action not in ('overwrite', 'keep_both', 'skip'):
            raise ValueError(f"Unknown conflict action: {action}")

        db = self.database_for(media)
        if not db.get_batch(batch_id):
            raise ValueError(f"Batch {batch_id} not found")

        wording = {
            'overwrite': 'replace with imported',
            'keep_both': 'keep both',
            'skip': 'keep existing',
        }[action]
        scope = f"{len(file_ids)} file(s)" if file_ids else "all conflicts"

        job = self._create(
            "resolve", media,
            f"Conflicts ({wording}): {scope} in batch #{batch_id}",
            {"batch_id": batch_id, "action": action, "file_ids": file_ids},
        )
        job.batch_id = batch_id

        def work(job: Job) -> dict:
            if media == VIDEO:
                from .video_copier import VideoCopier

                copier = VideoCopier(
                    self.video_db, progress_callback=self._progress_callback(job)
                )
            else:
                from .copier import PhotoCopier

                copier = PhotoCopier(
                    self.db, progress_callback=self._progress_callback(job)
                )

            stats = copier.resolve_conflicts(batch_id, action, file_ids=file_ids)
            return _copy_result(batch_id, stats)

        return self._spawn(job, work)

    def start_expand(
        self,
        source: str,
        target: Optional[str] = None,
        dry_run: bool = False,
        move_files: bool = False,
    ) -> Job:
        """Expand flat date directories (2012_05_20 -> 2012/05/20)."""
        source_path = Path(source).expanduser().resolve()
        if not source_path.is_dir():
            raise ValueError(f"Source directory not found: {source_path}")

        target_path = Path(target).expanduser().resolve() if target else None

        job = self._create(
            "expand", "files",
            f"Expand: {source_path.name or source_path}{' (dry run)' if dry_run else ''}",
            {
                "source": str(source_path),
                "target": str(target_path) if target_path else None,
                "dry_run": dry_run,
                "move": move_files,
            },
        )

        def work(job: Job) -> dict:
            from .expander import expand_directories

            result = expand_directories(
                str(source_path),
                str(target_path) if target_path else None,
                dry_run=dry_run,
                move_files=move_files,
                progress_callback=self._progress_callback(job),
            )
            return {
                "dirs_processed": result.dirs_processed,
                "dirs_skipped": result.dirs_skipped,
                "files_moved": result.files_moved,
                "errors": [{"path": path, "error": message} for path, message in result.errors[:20]],
                "error_count": len(result.errors),
            }

        return self._spawn(job, work)


    def start_camera_scan(
        self,
        path: str,
        rescan: bool = False,
        workers: Optional[int] = None,
    ) -> Job:
        """Index which camera took every photo under a path."""
        from .exif_reader import HAS_EXIFREAD, HAS_PIL

        root = Path(path).expanduser().resolve()
        if not root.is_dir():
            raise ValueError(f"Folder not found: {root}")

        if not HAS_EXIFREAD and not HAS_PIL:
            # Without a reader every row would be blank, and the skip list
            # would then keep those blanks forever. Refuse the walk instead.
            raise ValueError(
                "No EXIF reader installed, so the index would be all blanks. "
                "Start the server from the project venv - ./photo-import serve "
                "- or install Pillow and exifread into the Python running it."
            )

        job = self._create(
            "cameras", "files",
            f"Camera index: {root.name or root}{' (full rescan)' if rescan else ''}",
            {"path": str(root), "rescan": rescan, "workers": workers},
        )

        def work(job: Job) -> dict:
            from concurrent.futures import ThreadPoolExecutor
            from .exif_reader import read_camera_fields
            from .scanner import DEFAULT_WORKERS

            prefix = str(root) + os.sep
            known = {} if rescan else self.db.known_camera_photos(prefix)

            # Walk first, so the progress bar has a total to count against
            files = []
            for current, dirs, names in os.walk(root):
                dirs[:] = [d for d in dirs if not d.startswith('.')]
                job.check_cancelled()
                for name in names:
                    if name.startswith('.'):
                        continue
                    if Path(name).suffix.lower() in CAMERA_EXTENSIONS:
                        files.append(os.path.join(current, name))

            total = len(files)
            job.update(0, total, "Checking what changed...")

            # What has not changed since the last pass needs no reading
            seen = set()
            to_read = []
            skipped = 0
            for file_path in files:
                seen.add(file_path)
                try:
                    stat = os.stat(file_path)
                except OSError:
                    continue
                if known.get(file_path) == (stat.st_mtime, stat.st_size):
                    skipped += 1
                else:
                    to_read.append((file_path, stat.st_mtime, stat.st_size))

            done = skipped
            job.update(done, total, "")

            CHUNK = 200
            workers_used = workers or min(16, DEFAULT_WORKERS)
            with ThreadPoolExecutor(max_workers=workers_used) as pool:
                for index in range(0, len(to_read), CHUNK):
                    job.check_cancelled()
                    chunk = to_read[index:index + CHUNK]

                    # Reading EXIF is I/O bound, so the chunk goes out in parallel
                    results = list(pool.map(read_camera_fields,
                                            [path for path, _, _ in chunk]))

                    rows = []
                    for (file_path, mtime, size), (camera, lens, taken) in zip(chunk, results):
                        rows.append((
                            file_path, camera, lens,
                            taken.isoformat(sep=' ', timespec='seconds') if taken else None,
                            size, mtime,
                        ))

                    self.db.save_camera_photos(rows)
                    done += len(chunk)
                    job.update(done, total, chunk[-1][0])

            # Files the index remembers but the disk no longer has
            gone = [path for path in known if path not in seen]
            self.db.forget_camera_photos(gone)

            cameras = self.db.camera_totals(prefix)
            return {
                "photos": total,
                "read": len(to_read),
                "skipped": skipped,
                "forgotten": len(gone),
                "cameras": len([row for row in cameras if row["camera"]]),
            }

        return self._spawn(job, work)


def _clean_stats(stats: dict) -> dict:
    """Replace SQL NULLs with zeros so the UI can render them directly."""
    return {key: (value or 0) for key, value in stats.items()}


def _copy_result(batch_id: int, stats: dict) -> dict:
    duration = stats.get("duration")
    return {
        "batch_id": batch_id,
        "total": stats.get("total", 0),
        "copied": stats.get("copied", 0),
        "skipped": stats.get("skipped", 0),
        "failed": stats.get("failed", 0),
        "duration_seconds": int(duration.total_seconds()) if duration else 0,
    }

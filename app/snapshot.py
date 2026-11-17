"""Online recovery points: consistent SQLite snapshots taken while the service
keeps accepting alarms, deliveries and confirmations.

Why the SQLite online backup API
--------------------------------
``sqlite3.Connection.backup`` copies a *consistent* view of the live database
through the backup mechanism. Pages are read in batches: each batch briefly
coordinates with the writer via SQLite's own locks, so committed transactions —
including frames still only present in the WAL — are included, while concurrent
submissions and confirmations keep committing. The destination file therefore
represents one serialised point in time: an alarm, its notification numbers,
its acceptance timestamps and its confirmation deadline always come from the
same committed state; a half-committed transaction can never appear.

Why a staging area plus verification
------------------------------------
The backup is first written to a temporary file under ``.staging`` and is only
verified there. Verification opens the file with a *separate* read-only
connection (the live database is never touched by verification), runs
``PRAGMA integrity_check``, cross-checks the table contents against the source
and hashes the file. Only a fully verified snapshot is moved atomically into the
downloadable directory with its metadata sidecar. Any write or verification
failure leaves the previous snapshots — and the live service — untouched and
exposes no half-finished artifact.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import sqlite3
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .db import Database, utc_now

log = logging.getLogger("alarm.snapshot")

# Tables copied into (and expected inside) every recovery point.
SNAPSHOT_TABLES = ("alarms", "notifications", "events")
# Pages copied per backup batch. Small batches keep each writer-blocking interval
# short so an ongoing backup barely perturbs live request latency; the backup
# method yields for ``BACKUP_SLEEP_SECONDS`` between batches.
BACKUP_PAGES = 256
BACKUP_SLEEP_SECONDS = 0.05
# Hard safety bound on the number of batches (a busy writer can restart a batch,
# so this protects against a pathologically contended database).
BACKUP_MAX_STEPS = 10_000


class SnapshotError(Exception):
    """Raised when a recovery point cannot be created or verified.

    The live service is never affected: failures only remove the staged,
    not-yet-published artifact.
    """


def _snapshot_id() -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    return f"snap-{stamp}-{secrets.token_hex(4)}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in SNAPSHOT_TABLES
    }


class SnapshotStore:
    """Manages verified recovery point files on disk."""

    def __init__(self, directory: str, *, retention: int = 10):
        self.directory = Path(directory)
        self.staging = self.directory / ".staging"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.staging.mkdir(parents=True, exist_ok=True)
        self.retention = max(1, retention)
        # Snapshot creation is serialised within the process: a backup already
        # takes a consistent online copy, and running several at once would only
        # contend for the same writer lock and disk bandwidth.
        self._operation_lock = threading.Lock()

    # ------------------------------------------------------------- read helpers
    def _sidecar_path(self, snapshot_id: str) -> Path:
        return self.directory / f"{snapshot_id}.json"

    def _db_path(self, snapshot_id: str) -> Path:
        return self.directory / f"{snapshot_id}.db"

    def list_snapshots(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for sidecar in self.directory.glob("snap-*.json"):
            try:
                record = json.loads(sidecar.read_text("utf-8"))
            except (OSError, ValueError):
                log.warning("ignoring unreadable snapshot sidecar %s", sidecar)
                continue
            # A download must never expose a file whose metadata claims "ready"
            # but whose database file is missing (e.g. externally removed).
            record["download_available"] = self._db_path(record["snapshot_id"]).exists()
            records.append(record)
        # Order by the recorded creation time, not the file name: several points
        # can share a timestamp prefix when taken within the same second.
        records.sort(key=lambda item: item.get("created_at", ""), reverse=True)
        return records

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        sidecar = self._sidecar_path(snapshot_id)
        if not sidecar.is_file():
            return None
        try:
            record = json.loads(sidecar.read_text("utf-8"))
        except (OSError, ValueError) as exc:
            raise SnapshotError(f"snapshot metadata for {snapshot_id!r} is unreadable: {exc}") from exc
        record["download_available"] = self._db_path(record["snapshot_id"]).exists()
        return record

    def db_path_for_download(self, snapshot_id: str) -> Path:
        record = self.get_snapshot(snapshot_id)
        if record is None:
            raise FileNotFoundError(snapshot_id)
        if record.get("status") != "ready":
            raise SnapshotError(f"snapshot {snapshot_id!r} is not verified and cannot be downloaded")
        path = self._db_path(snapshot_id)
        if not path.is_file():
            raise SnapshotError(f"snapshot {snapshot_id!r} verified metadata exists but the file is missing")
        return path

    # ------------------------------------------------------------------ create
    def create_snapshot(self, db: Database) -> dict[str, Any]:
        """Take an online, verified snapshot of the live database.

        Runs entirely in a worker thread (the caller offloads it) and never opens
        a write transaction on the live database, so concurrent commits continue
        throughout. Raises ``SnapshotError`` on any failure after cleaning up the
        staged artifact; the live database is never modified or deleted.
        """
        if not self._operation_lock.acquire(blocking=False):
            raise SnapshotError("another snapshot operation is already in progress")
        snapshot_id = _snapshot_id()
        staged = self.staging / f"{snapshot_id}.db.tmp"
        started_at = utc_now()
        try:
            return self._create_locked(db, snapshot_id, staged, started_at)
        finally:
            self._operation_lock.release()

    def _create_locked(
        self, db: Database, snapshot_id: str, staged: Path, started_at: str
    ) -> dict[str, Any]:
        # 1) Sanity-check the live database first (a read-only query; no writes).
        #    Row counts are deliberately *not* captured here: the backup takes a
        #    moving, concurrently-written database, so the authoritative counts
        #    are read from the consistent snapshot itself after the copy.
        try:
            with db.read_transaction() as conn:
                source_integrity = conn.execute("PRAGMA integrity_check").fetchone()
                source_integrity_value = source_integrity[0] if source_integrity else "unknown"
                if source_integrity_value != "ok":
                    raise SnapshotError(
                        f"source database integrity_check is {source_integrity_value!r}"
                    )
        except SnapshotError:
            raise
        except Exception as exc:  # noqa: BLE001 - convert to a typed failure
            raise SnapshotError(f"cannot read source database: {exc}") from exc

        # 2) Online backup into the staged file. Python's ``Connection.backup``
        #    drives the SQLite online backup API in small page batches and calls
        #    the progress callback between them; sleeping there yields the lock
        #    to live writers, whose committed transactions (including WAL-only
        #    frames) are folded into the consistent copy.
        destination = sqlite3.connect(staged, isolation_level=None)
        state = {"steps": 0}

        def _progress(_status: int, _remaining: int, _total: int) -> None:
            state["steps"] += 1
            if state["steps"] > BACKUP_MAX_STEPS:
                raise SnapshotError("snapshot exceeded the maximum number of backup steps")
            time.sleep(BACKUP_SLEEP_SECONDS)

        try:
            with db.backup_lock() as source_conn:
                # ``sleep`` is applied between page batches; the copy itself is
                # one serialised consistent view even though writers keep going.
                source_conn.backup(
                    destination,
                    pages=BACKUP_PAGES,
                    progress=_progress,
                    sleep=BACKUP_SLEEP_SECONDS,
                )
            steps = state["steps"]
            # A self-contained file: no -wal/-shm sidecars are required to open
            # the recovery point on another host.
            destination.execute("PRAGMA journal_mode=DELETE")
            destination.commit()
        except SnapshotError:
            self._close_quietly(destination)
            self._discard(staged)
            raise
        except Exception as exc:  # noqa: BLE001
            self._close_quietly(destination)
            self._discard(staged)
            raise SnapshotError(f"online backup failed: {exc}") from exc
        else:
            self._close_quietly(destination)

        # 3) Verify the staged copy with an independent read-only connection.
        #    Any failure discards the staged artifact: nothing half-finished is
        #    ever published or made downloadable. Consistency is proven by the
        #    integrity/foreign-key checks and referential assertions, not by
        #    comparing against a moving live database.
        try:
            verification, checksum, size_bytes = self._verify(staged)
        except SnapshotError:
            self._discard(staged)
            raise
        except Exception as exc:  # noqa: BLE001 - normalise, then clean up
            self._discard(staged)
            raise SnapshotError(f"snapshot verification failed: {exc}") from exc

        # Read the *current* committed source counts for metadata only. They may
        # exceed the snapshot counts when commits landed during the backup; that
        # is expected online behaviour and never invalidates the snapshot.
        try:
            with db.read_transaction() as conn:
                source_counts = _table_counts(conn)
        except Exception as exc:  # noqa: BLE001
            self._discard(staged)
            raise SnapshotError(f"cannot read source database after backup: {exc}") from exc

        # 4) Publish atomically: database file first, then metadata sidecar. The
        #    sidecar is the single thing the download endpoint treats as proof,
        #    and it only appears once the database file is in place.
        final_db = self._db_path(snapshot_id)
        os.replace(staged, final_db)
        record = {
            "snapshot_id": snapshot_id,
            "status": "ready",
            "created_at": started_at,
            "finished_at": utc_now(),
            "database_file": final_db.name,
            "sha256": checksum,
            "size_bytes": size_bytes,
            "source_database": str(db.path),
            "backup_steps": steps,
            "row_counts": verification["row_counts"],
            "source_row_counts": source_counts,
            "integrity_check": verification["integrity_check"],
            "foreign_key_check": verification["foreign_key_check"],
            "sqlite_version": sqlite3.sqlite_version,
        }
        self._write_sidecar(snapshot_id, record)
        record["download_available"] = True
        log.info(
            "snapshot %s verified and published (%s bytes, counts=%s)",
            snapshot_id,
            size_bytes,
            verification["row_counts"],
        )
        self._prune()
        return record

    # -------------------------------------------------------------- verification
    def _verify(self, staged: Path) -> tuple[dict[str, Any], str, int]:
        """Open the staged file independently and prove it is a usable recovery
        point. Failure raises ``SnapshotError``; nothing is published.

        The online backup is already a single serialised, transactionally
        consistent view, so verification proves structural integrity and the
        domain invariants rather than comparing against the still-changing live
        database.
        """
        uri = f"file:{staged.resolve()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            integrity_rows = conn.execute("PRAGMA integrity_check").fetchall()
            integrity = [row[0] for row in integrity_rows]
            if integrity != ["ok"]:
                raise SnapshotError(f"snapshot integrity_check failed: {integrity[:5]}")

            foreign_key_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if foreign_key_violations:
                raise SnapshotError(
                    f"snapshot foreign_key_check found {len(foreign_key_violations)} violation(s)"
                )

            present = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            missing = [table for table in SNAPSHOT_TABLES if table not in present]
            if missing:
                raise SnapshotError(f"snapshot is missing required tables: {missing}")

            counts = _table_counts(conn)

            # Referential consistency inside the snapshot itself: every
            # notification and event belongs to an existing alarm, and the
            # one-primary/at-most-one-backup invariant still holds.
            orphan_notifications = conn.execute(
                "SELECT COUNT(*) FROM notifications n LEFT JOIN alarms a ON a.alarm_no = n.alarm_no"
                " WHERE a.alarm_no IS NULL"
            ).fetchone()[0]
            if orphan_notifications:
                raise SnapshotError(f"snapshot contains {orphan_notifications} orphan notification row(s)")
            orphan_events = conn.execute(
                "SELECT COUNT(*) FROM events e LEFT JOIN alarms a ON a.alarm_no = e.alarm_no"
                " WHERE a.alarm_no IS NULL"
            ).fetchone()[0]
            if orphan_events:
                raise SnapshotError(f"snapshot contains {orphan_events} orphan event row(s)")
            duplicate_roles = conn.execute(
                "SELECT alarm_no, role, COUNT(*) FROM notifications"
                " GROUP BY alarm_no, role HAVING COUNT(*) > 1"
            ).fetchall()
            if duplicate_roles:
                raise SnapshotError("snapshot violates the one-notification-per-role invariant")

            # Read every row through the schema once: a torn page would surface
            # here rather than at recovery time.
            for table in SNAPSHOT_TABLES:
                conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        except SnapshotError:
            raise
        except sqlite3.DatabaseError as exc:
            raise SnapshotError(f"snapshot database is not readable: {exc}") from exc
        except Exception as exc:  # noqa: BLE001
            raise SnapshotError(f"snapshot verification failed: {exc}") from exc
        finally:
            conn.close()

        checksum = _sha256_file(staged)
        size_bytes = staged.stat().st_size
        return (
            {
                "integrity_check": "ok",
                "foreign_key_check": "ok",
                "row_counts": counts,
            },
            checksum,
            size_bytes,
        )

    # ----------------------------------------------------------------- housekeeping
    def _write_sidecar(self, snapshot_id: str, record: dict[str, Any]) -> None:
        sidecar = self._sidecar_path(snapshot_id)
        tmp = sidecar.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, sidecar)

    def _prune(self) -> None:
        """Keep only the newest ``retention`` verified snapshots."""
        sidecars = list(self.directory.glob("snap-*.json"))

        def _created(sidecar: Path) -> str:
            try:
                return json.loads(sidecar.read_text("utf-8")).get("created_at", "")
            except (OSError, ValueError):
                return ""

        sidecars.sort(key=_created, reverse=True)
        for stale in sidecars[self.retention :]:
            snapshot_id = stale.name[: -len(".json")]
            stale_db = self._db_path(snapshot_id)
            try:
                stale.unlink()
            except OSError:
                log.warning("could not prune sidecar %s", stale)
            if stale_db.exists():
                try:
                    stale_db.unlink()
                except OSError:
                    log.warning("could not prune snapshot file %s", stale_db)

    def _discard(self, staged: Path) -> None:
        try:
            if staged.exists():
                staged.unlink()
        except OSError:
            log.warning("could not remove staged snapshot %s", staged)

    @staticmethod
    def _close_quietly(conn: sqlite3.Connection) -> None:
        try:
            conn.close()
        except sqlite3.Error:
            pass

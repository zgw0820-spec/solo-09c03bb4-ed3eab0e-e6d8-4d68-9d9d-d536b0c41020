#!/usr/bin/env python3
"""Restore an online recovery point into a target SQLite database path.

The script is deliberately independent of the running API: it only copies and
verifies a snapshot file and swaps it into place atomically. It never starts the
service and never deletes the live database until the replacement has passed the
same integrity checks used when the snapshot was published.

Typical Docker Compose recovery
-------------------------------
1. Download a verified point from the still-running (or a temporary) API::

       python -I scripts/restore_snapshot.py \\
           --snapshot-url http://localhost:8000/recovery-points/snap-.../download \\
           --target ./data/alarms.db

   ...or pass an already downloaded file with ``--snapshot-file``.
2. Stop the API, place the restored file on the ``api-data`` volume at
   ``/srv/data/alarms.db`` (see README), then ``docker compose up -d``.

Recovery semantics
------------------
Only alarms committed at the snapshot point are restored. The notification
numbers inside the snapshot stay unchanged, so on startup the worker:

* looks every pending/unknown notification up against the *existing* gateway
  using its original notification number and either adopts the gateway's
  acceptance or continues delivery under that same idempotency key — an already
  accepted notification is never physically delivered twice;
* keeps the primary confirmation deadline anchored to the gateway's recorded
  acceptance timestamp (``primary_accepted_at``), not to the restore time.

Run with ``-I`` so an untrusted download directory cannot shadow stdlib modules.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import urllib.request
from pathlib import Path

REQUIRED_TABLES = ("alarms", "notifications", "events")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fetch(url: str, destination: Path) -> None:
    request = urllib.request.Request(url, headers={"accept": "application/vnd.sqlite3"})
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 - operator supplied URL
        if response.status != 200:
            raise SystemExit(f"download failed: HTTP {response.status}")
        with destination.open("wb") as out:
            shutil.copyfileobj(response, out)


def _verify(path: Path) -> dict[str, int]:
    if not path.is_file() or path.stat().st_size == 0:
        raise SystemExit(f"snapshot file missing or empty: {path}")
    uri = f"file:{path.resolve()}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        try:
            integrity = [row[0] for row in conn.execute("PRAGMA integrity_check").fetchall()]
            if integrity != ["ok"]:
                raise SystemExit(f"integrity_check failed: {integrity[:5]}")
            fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if fk_violations:
                raise SystemExit(f"foreign_key_check found {len(fk_violations)} violation(s)")
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            missing = [table for table in REQUIRED_TABLES if table not in tables]
            if missing:
                raise SystemExit(f"snapshot missing required tables: {missing}")
            counts = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in REQUIRED_TABLES
            }
        finally:
            conn.close()
    except sqlite3.DatabaseError as exc:
        raise SystemExit(f"snapshot is not a valid SQLite database: {exc}") from exc
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Restore a verified SQLite recovery point.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--snapshot-file", help="Path to an already downloaded snapshot .db file")
    source.add_argument("--snapshot-url", help="API download URL for the recovery point")
    parser.add_argument("--target", required=True, help="Destination SQLite path (e.g. ./data/alarms.db)")
    parser.add_argument(
        "--expected-sha256",
        help="Optional SHA-256 from the recovery point metadata; the restore aborts on mismatch",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the target even if it already exists (a .bak copy is kept)",
    )
    args = parser.parse_args(argv)

    target = Path(args.target).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    workdir = target.parent
    with tempfile.TemporaryDirectory(prefix=".restore-", dir=workdir) as tmp_name:
        tmp_dir = Path(tmp_name)
        staged = tmp_dir / "snapshot.db"
        if args.snapshot_file:
            shutil.copy2(Path(args.snapshot_file).expanduser(), staged)
        else:
            print(f"downloading recovery point from {args.snapshot_url} ...")
            _fetch(args.snapshot_url, staged)

        checksum = _sha256(staged)
        if args.expected_sha256 and checksum.lower() != args.expected_sha256.lower():
            raise SystemExit(
                f"checksum mismatch: got {checksum}, expected {args.expected_sha256}"
            )

        counts = _verify(staged)
        print(f"verified snapshot sha256={checksum} rows={json.dumps(counts, sort_keys=True)}")

        # Back up every file of the previous database (main file plus any WAL
        # sidecars) before touching it. WAL/SHM sidecars must never be applied on
        # top of the restored file, so they are moved away rather than copied.
        if target.exists():
            if not args.force:
                raise SystemExit(
                    f"target {target} already exists; re-run with --force to replace it"
                )
            os.replace(target, target.with_suffix(target.suffix + ".bak"))

        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(target) + suffix)
            if sidecar.exists():
                os.replace(sidecar, Path(str(target) + f"{suffix}.bak"))
                print(f"moved stale {sidecar.name} aside")

        os.replace(staged, target)

    print(f"restored {target} (previous database, if any, kept as *.bak)")
    print(
        "next: point DATABASE_PATH at this file and start the API "
        "(docker compose up -d); pending notifications reconcile via their "
        "original notification numbers, accepted ones are never re-delivered."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

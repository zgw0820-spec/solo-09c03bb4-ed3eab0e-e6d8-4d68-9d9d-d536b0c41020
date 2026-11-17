"""Online recovery point (SQLite snapshot) behaviour.

Covers:
* a snapshot contains a consistent, committed view including WAL frames;
* submissions, deliveries and confirmations keep completing while a snapshot is
  being taken;
* only verified snapshots are ever downloadable and failures leave no artifact;
* restoring a snapshot resumes from the original notification numbers without
  re-delivering an already accepted notification, and the primary deadline stays
  anchored to the gateway's acceptance time.
"""

from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import time
from pathlib import Path

import httpx
import pytest

from app.config import Settings
from app.db import Database
from app.main import create_app
from app.snapshot import SnapshotError, SnapshotStore
from tests.conftest import ApiHarness
from tests.fake_gateway import FakeGateway, build_fake_gateway_app
from tests.helpers import _status


@pytest.fixture
async def api(tmp_path, fake_gateway):
    """API harness with an isolated, empty snapshot directory per test."""
    harness = ApiHarness(tmp_path, fake_gateway)
    harness.settings = Settings(
        database_path=harness.settings.database_path,
        gateway_url=harness.settings.gateway_url,
        post_timeout_seconds=1.0,
        lookup_timeout_seconds=0.5,
        worker_interval_seconds=0.02,
        delivery_lease_seconds=2.0,
        max_backoff_seconds=0.05,
        reconcile_on_read=True,
        snapshot_dir=str(tmp_path / "snapshots"),
    )
    harness.app = create_app(harness.settings)
    async with harness:
        yield harness


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def test_snapshot_captures_committed_state_and_is_downloadable(api: ApiHarness, tmp_path):
    await api.client.post("/alarms", json=api.alarm_payload("SNAP-1", deadline=10))
    assert await api.wait_until(lambda: _status(api, "SNAP-1", "awaiting_confirmation"))
    confirm = await api.client.post("/alarms/SNAP-1/confirm", json={"confirmed_by": "张工"})
    assert confirm.status_code == 200

    created = await api.client.post("/recovery-points")
    assert created.status_code == 201, created.text
    record = created.json()
    assert record["status"] == "ready"
    assert record["integrity_check"] == "ok"
    assert record["foreign_key_check"] == "ok"
    assert record["row_counts"]["alarms"] >= 1
    assert record["row_counts"]["notifications"] >= 1
    assert record["download_available"] is True

    listing = (await api.client.get("/recovery-points")).json()
    assert listing["total"] == 1
    assert listing["items"][0]["snapshot_id"] == record["snapshot_id"]

    single = await api.client.get(f"/recovery-points/{record['snapshot_id']}")
    assert single.status_code == 200
    assert single.json()["sha256"] == record["sha256"]

    download = await api.client.get(f"/recovery-points/{record['snapshot_id']}/download")
    assert download.status_code == 200
    assert download.headers["content-type"] == "application/vnd.sqlite3"
    assert "attachment" in download.headers["content-disposition"]
    assert hashlib.sha256(download.content).hexdigest() == record["sha256"]

    # The downloaded file is a self-contained, openable database with the rows.
    target = tmp_path / "restored.db"
    target.write_bytes(download.content)
    conn = sqlite3.connect(target)
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        alarms = conn.execute("SELECT alarm_no, confirmed_by FROM alarms").fetchall()
        assert ("SNAP-1", "张工") in alarms
        notification_rows = conn.execute(
            "SELECT notification_no, state, accepted_at FROM notifications "
            "WHERE alarm_no = 'SNAP-1' AND role = 'primary'"
        ).fetchall()
        assert len(notification_rows) == 1
        notification_no, state, accepted_at = notification_rows[0]
        assert state == "accepted" and accepted_at is not None
        deadline = conn.execute(
            "SELECT primary_accepted_at, primary_ack_deadline_at FROM alarms WHERE alarm_no = 'SNAP-1'"
        ).fetchone()
        assert deadline[0] is not None and deadline[1] is not None
    finally:
        conn.close()


async def test_wal_commits_are_included_and_snapshot_is_a_single_consistent_point(
    api: ApiHarness,
):
    # Force a checkpoint boundary so recent commits still live in the WAL at
    # snapshot time; the online backup must include them regardless.
    db: Database = api.app.state.db
    db.execute("PRAGMA wal_checkpoint(PASSIVE)")

    for index in range(5):
        response = await api.client.post(
            "/alarms", json=api.alarm_payload(f"WAL-{index}", deadline=10)
        )
        assert response.status_code == 201
    assert await api.wait_until(lambda: _status(api, "WAL-4", "awaiting_confirmation"))

    record = (await api.client.post("/recovery-points")).json()
    counts = record["row_counts"]
    assert counts["alarms"] == 5
    assert counts["notifications"] == 5

    # Independent read of the published snapshot: every notification references
    # an alarm from the same committed point (no torn cross-transaction state).
    store: SnapshotStore = api.app.state.snapshot_store
    path = store.db_path_for_download(record["snapshot_id"])
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        orphans = conn.execute(
            "SELECT COUNT(*) FROM notifications n LEFT JOIN alarms a "
            "ON a.alarm_no = n.alarm_no WHERE a.alarm_no IS NULL"
        ).fetchone()[0]
        assert orphans == 0
        # Each captured alarm has its primary notification number and its
        # acceptance/deadline timestamps committed together.
        mismatched = conn.execute(
            "SELECT COUNT(*) FROM alarms a JOIN notifications n ON n.alarm_no = a.alarm_no "
            "WHERE n.role = 'primary' AND n.state = 'accepted' "
            "AND (a.primary_accepted_at IS NULL OR a.primary_ack_deadline_at IS NULL)"
        ).fetchone()[0]
        assert mismatched == 0
    finally:
        conn.close()


async def test_service_keeps_writing_throughout_snapshot(api: ApiHarness):
    store: SnapshotStore = api.app.state.snapshot_store

    stop = False

    async def hammer_writes() -> None:
        index = 0
        while not stop:
            index += 1
            await api.client.post(
                "/alarms", json=api.alarm_payload(f"CONC-{index % 20:02d}-{index}", deadline=10)
            )
            if index % 4 == 0:
                await api.client.post(
                    f"/alarms/CONC-{index % 20:02d}-{index}/confirm",
                    json={"confirmed_by": "张工"},
                )
            await asyncio.sleep(0.005)

    writer = asyncio.create_task(hammer_writes())
    try:
        # Take several snapshots back to back while writes run.
        records = []
        for _ in range(3):
            response = await api.client.post("/recovery-points")
            assert response.status_code == 201, response.text
            records.append(response.json())
            await asyncio.sleep(0.05)
    finally:
        stop = True
        await writer

    # Each snapshot is internally consistent and their committed counts are
    # monotonically non-decreasing as the service kept accepting work.
    totals = [record["row_counts"]["alarms"] for record in records]
    assert totals == sorted(totals)
    assert totals[-1] >= 3
    for record in records:
        path = store.db_path_for_download(record["snapshot_id"])
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        finally:
            conn.close()

    # The live service is still healthy and still accepting afterwards.
    after = await api.client.post("/alarms", json=api.alarm_payload("AFTER", deadline=10))
    assert after.status_code == 201
    health = await api.client.get("/health")
    assert health.json()["status"] == "ok"


async def test_concurrent_snapshot_requests_are_serialised_not_duplicated(api: ApiHarness):
    responses = await asyncio.gather(
        api.client.post("/recovery-points"),
        api.client.post("/recovery-points"),
    )
    statuses = sorted(response.status_code for response in responses)
    assert statuses == [201, 409]
    listing = (await api.client.get("/recovery-points")).json()
    assert listing["total"] == 1
    assert listing["items"][0]["status"] == "ready"


async def test_failed_verification_publishes_nothing_and_keeps_live_service(
    api: ApiHarness, monkeypatch
):
    store: SnapshotStore = api.app.state.snapshot_store

    def broken_verify(self, staged):  # noqa: ANN001
        raise SnapshotError("injected verification failure")

    monkeypatch.setattr(SnapshotStore, "_verify", broken_verify)

    response = await api.client.post("/recovery-points")
    assert response.status_code == 500
    assert "injected verification failure" in response.text

    # No sidecar, no downloadable database, an empty staging area.
    assert store.list_snapshots() == []
    assert not list(store.directory.glob("snap-*.db"))
    assert not list(store.staging.iterdir())

    # The live database and API are unaffected.
    health = await api.client.get("/health")
    assert health.json()["status"] == "ok"
    submit = await api.client.post("/alarms", json=api.alarm_payload("STILL-OK", deadline=10))
    assert submit.status_code == 201

    # A subsequent, healthy snapshot succeeds.
    monkeypatch.undo()
    retry = await api.client.post("/recovery-points")
    assert retry.status_code == 201
    assert retry.json()["status"] == "ready"


async def test_unknown_recovery_point_is_not_downloadable(api: ApiHarness):
    missing = await api.client.get("/recovery-points/snap-does-not-exist")
    assert missing.status_code == 404
    download = await api.client.get("/recovery-points/snap-does-not-exist/download")
    assert download.status_code == 404


async def test_snapshot_retention_prunes_only_verified_older_points(api: ApiHarness):
    store: SnapshotStore = api.app.state.snapshot_store
    store.retention = 2
    ids = []
    for _ in range(4):
        record = (await api.client.post("/recovery-points")).json()
        ids.append(record["snapshot_id"])
        await asyncio.sleep(0.01)
    listing = (await api.client.get("/recovery-points")).json()
    kept = {item["snapshot_id"] for item in listing["items"]}
    assert kept == set(ids[-2:])
    for old_id in ids[:-2]:
        assert not store._db_path(old_id).exists()
        assert not store._sidecar_path(old_id).exists()


async def test_restored_snapshot_resumes_without_duplicate_delivery(tmp_path):
    """Full recovery cycle on a fresh process with the same (persisted) gateway.

    The snapshot is taken after the gateway has accepted the primary. On restore
    the new API process must keep the original notification number, find the
    gateway acceptance through it, never post again, and keep the confirmation
    deadline based on the original gateway acceptance time.
    """
    fake = FakeGateway()
    db_path = str(tmp_path / "live" / "alarms.db")
    snapshot_dir = tmp_path / "snapshots"
    settings = Settings(
        database_path=db_path,
        gateway_url="http://gw.test",
        snapshot_dir=str(snapshot_dir),
        post_timeout_seconds=1.0,
        lookup_timeout_seconds=0.5,
        worker_interval_seconds=0.02,
        reconcile_on_read=True,
    )
    app = create_app(settings)
    gateway_app = build_fake_gateway_app(fake)
    lifespan = app.router.lifespan_context(app)
    await lifespan.__aenter__()
    app.state.gateway._client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app), base_url="http://gw.test"
    )
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test")
    try:
        submit = await client.post(
            "/alarms",
            json=ApiHarness.alarm_payload(ApiHarness(tmp_path, fake), "RESTORE-1", deadline=8),
        )
        assert submit.status_code == 201
        original_notification_no = submit.json()["notifications"][0]["notification_no"]

        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            view = (await client.get("/alarms/RESTORE-1")).json()
            if view["primary_accepted_at"] is not None:
                accepted_before = view["primary_accepted_at"]
                due_before = view["confirmation_due_at"]
                break
            await asyncio.sleep(0.02)
        else:
            raise AssertionError("primary never accepted before snapshot")

        record = (await client.post("/recovery-points")).json()
        assert record["status"] == "ready"
        snapshot_file = app.state.snapshot_store.db_path_for_download(record["snapshot_id"])
    finally:
        await client.aclose()
        await lifespan.__aexit__(None, None, None)

    # Restore into a brand new database directory, simulating a rebuilt volume.
    restored_dir = tmp_path / "restored"
    restored_dir.mkdir()
    restored_db = restored_dir / "alarms.db"
    restored_db.write_bytes(snapshot_file.read_bytes())
    assert _sha256(restored_db) == record["sha256"]

    # The gateway keeps its record (same persistent gateway), keyed by the
    # original notification number.
    assert original_notification_no in fake.records
    posts_before_restore = fake.post_calls

    restored_settings = Settings(
        database_path=str(restored_db),
        gateway_url="http://gw.test",
        snapshot_dir=str(tmp_path / "snapshots2"),
        post_timeout_seconds=1.0,
        lookup_timeout_seconds=0.5,
        worker_interval_seconds=0.02,
        reconcile_on_read=True,
    )
    restored_app = create_app(restored_settings)
    restored_lifespan = restored_app.router.lifespan_context(restored_app)
    await restored_lifespan.__aenter__()
    restored_app.state.gateway._client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app), base_url="http://gw.test"
    )
    restored_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=restored_app), base_url="http://api.test"
    )
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            view = (await restored_client.get("/alarms/RESTORE-1")).json()
            if view["primary_accepted_at"] is not None:
                break
            await asyncio.sleep(0.02)
        else:
            raise AssertionError((await restored_client.get("/alarms/RESTORE-1")).text)

        view = (await restored_client.get("/alarms/RESTORE-1")).json()
        primary = next(n for n in view["notifications"] if n["role"] == "primary")
        assert primary["notification_no"] == original_notification_no
        assert primary["state"] == "accepted"
        # Same gateway acceptance anchors the clock; the deadline is not reset.
        assert view["primary_accepted_at"] == accepted_before
        assert view["confirmation_due_at"] == due_before
        # No new physical delivery happened during reconciliation.
        assert fake.post_calls == posts_before_restore
        assert len(fake.role_notifications("primary")) == 1
    finally:
        await restored_client.aclose()
        await restored_lifespan.__aexit__(None, None, None)


async def test_restored_snapshot_reposts_unresolved_notification_under_same_number(
    tmp_path,
):
    """A notification the gateway never saw resumes from the snapshot and is
    delivered once, still under its original notification number."""
    fake = FakeGateway()
    db_path = str(tmp_path / "live" / "alarms.db")
    settings = Settings(
        database_path=db_path,
        gateway_url="http://gw.test",
        snapshot_dir=str(tmp_path / "snapshots"),
        post_timeout_seconds=1.0,
        lookup_timeout_seconds=0.5,
        worker_interval_seconds=0.02,
        max_backoff_seconds=0.02,
        not_found_misses_before_repost=1,
        reconcile_on_read=True,
    )
    app = create_app(settings)
    gateway_app = build_fake_gateway_app(fake)
    lifespan = app.router.lifespan_context(app)
    await lifespan.__aenter__()
    app.state.gateway._client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app), base_url="http://gw.test"
    )
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test")
    try:
        # Freeze delivery so the primary stays pending and never reaches the
        # gateway before the snapshot is taken.
        app.state.dispatcher.delivery_paused = True
        submit = await client.post(
            "/alarms",
            json=ApiHarness.alarm_payload(ApiHarness(tmp_path, fake), "RESTORE-2", deadline=8),
        )
        assert submit.status_code == 201
        original_notification_no = submit.json()["notifications"][0]["notification_no"]
        await asyncio.sleep(0.1)
        assert fake.records == {}
        record = (await client.post("/recovery-points")).json()
        snapshot_file = app.state.snapshot_store.db_path_for_download(record["snapshot_id"])
    finally:
        app.state.dispatcher.delivery_paused = False
        await client.aclose()
        await lifespan.__aexit__(None, None, None)

    restored_db = tmp_path / "restored" / "alarms.db"
    restored_db.parent.mkdir(parents=True, exist_ok=True)
    restored_db.write_bytes(snapshot_file.read_bytes())

    restored_settings = Settings(
        database_path=str(restored_db),
        gateway_url="http://gw.test",
        snapshot_dir=str(tmp_path / "snapshots2"),
        post_timeout_seconds=1.0,
        lookup_timeout_seconds=0.5,
        worker_interval_seconds=0.02,
        max_backoff_seconds=0.02,
        not_found_misses_before_repost=1,
        reconcile_on_read=True,
    )
    restored_app = create_app(restored_settings)
    restored_lifespan = restored_app.router.lifespan_context(restored_app)
    await restored_lifespan.__aenter__()
    restored_app.state.gateway._client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app), base_url="http://gw.test"
    )
    restored_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=restored_app), base_url="http://api.test"
    )
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            view = (await restored_client.get("/alarms/RESTORE-2")).json()
            if view["primary_accepted_at"] is not None:
                break
            await asyncio.sleep(0.02)
        else:
            raise AssertionError((await restored_client.get("/alarms/RESTORE-2")).text)

        view = (await restored_client.get("/alarms/RESTORE-2")).json()
        primary = next(n for n in view["notifications"] if n["role"] == "primary")
        assert primary["notification_no"] == original_notification_no
        assert primary["state"] == "accepted"
        # Exactly one physical page, under the number preserved by the snapshot.
        assert list(fake.records) == [original_notification_no]
        assert len(fake.role_notifications("primary")) == 1
    finally:
        await restored_client.aclose()
        await restored_lifespan.__aexit__(None, None, None)

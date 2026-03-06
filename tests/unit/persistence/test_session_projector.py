"""Tests for session projection: parity, staleness, corruption, benchmarks."""

import time

import pytest

from ouroboros.events.base import BaseEvent
from ouroboros.orchestrator.session import SessionRepository, SessionStatus
from ouroboros.persistence.event_store import EventStore
from ouroboros.persistence.session_projector import SessionProjector


@pytest.fixture
async def db_path(tmp_path):
    return tmp_path / "test_projector.db"


@pytest.fixture
async def event_store(db_path):
    projector_ref = []

    def _make_store(projectors=None):
        store = EventStore(f"sqlite+aiosqlite:///{db_path}", projectors=projectors)
        return store

    store = _make_store()
    await store.initialize()
    yield store
    await store.close()


@pytest.fixture
async def projector(event_store):
    return SessionProjector(event_store._engine)


@pytest.fixture
async def wired_store(db_path):
    """EventStore with projector wired in."""
    # Create store first to get engine
    store = EventStore(f"sqlite+aiosqlite:///{db_path}")
    await store.initialize()

    proj = SessionProjector(store._engine)
    store._projectors = [proj.apply_in_transaction]

    yield store, proj
    await store.close()


def _session_started(session_id, execution_id="exec-1", seed_id="seed-1"):
    return BaseEvent(
        type="orchestrator.session.started",
        aggregate_type="session",
        aggregate_id=session_id,
        data={
            "execution_id": execution_id,
            "seed_id": seed_id,
            "start_time": "2026-01-01T00:00:00+00:00",
        },
    )


def _progress(session_id, step=1):
    return BaseEvent(
        type="orchestrator.progress.updated",
        aggregate_type="session",
        aggregate_id=session_id,
        data={"progress": {"step": step}},
    )


def _completed(session_id):
    return BaseEvent(
        type="orchestrator.session.completed",
        aggregate_type="session",
        aggregate_id=session_id,
        data={"summary": {}, "completed_at": "2026-01-01T01:00:00+00:00"},
    )


def _failed(session_id, error="something broke"):
    return BaseEvent(
        type="orchestrator.session.failed",
        aggregate_type="session",
        aggregate_id=session_id,
        data={"error": error, "failed_at": "2026-01-01T01:00:00+00:00"},
    )


def _paused(session_id):
    return BaseEvent(
        type="orchestrator.session.paused",
        aggregate_type="session",
        aggregate_id=session_id,
        data={},
    )


# ============================================================================
# P1-AC1: Schema
# ============================================================================


class TestProjectionSchema:
    async def test_table_created_on_initialize(self, event_store):
        """session_projections table exists after initialize()."""
        from sqlalchemy import inspect

        async with event_store._engine.begin() as conn:
            tables = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_table_names()
            )
        assert "session_projections" in tables

    async def test_table_has_correct_columns(self, event_store):
        """session_projections has all expected columns."""
        from sqlalchemy import inspect

        async with event_store._engine.begin() as conn:
            columns = await conn.run_sync(
                lambda sync_conn: [
                    c["name"]
                    for c in inspect(sync_conn).get_columns("session_projections")
                ]
            )
        expected = {
            "session_id", "execution_id", "seed_id", "status",
            "start_time", "messages_processed", "last_progress",
            "last_error", "last_event_id", "updated_at",
        }
        assert expected == set(columns)


# ============================================================================
# P1-AC2: SessionProjector fold logic
# ============================================================================


class TestProjectorApply:
    async def test_apply_session_started(self, wired_store):
        store, proj = wired_store
        event = _session_started("s1")
        await store.append(event)

        row = await proj.read("s1")
        assert row is not None
        assert row["status"] == "running"
        assert row["execution_id"] == "exec-1"
        assert row["seed_id"] == "seed-1"
        assert row["messages_processed"] == 0
        assert row["last_event_id"] == event.id

    async def test_apply_progress(self, wired_store):
        store, proj = wired_store
        await store.append(_session_started("s1"))
        event = _progress("s1", step=1)
        await store.append(event)

        row = await proj.read("s1")
        assert row["messages_processed"] == 1
        assert row["last_progress"] == {"step": 1}

    async def test_apply_completed(self, wired_store):
        store, proj = wired_store
        await store.append(_session_started("s1"))
        await store.append(_completed("s1"))

        row = await proj.read("s1")
        assert row["status"] == "completed"

    async def test_apply_failed(self, wired_store):
        store, proj = wired_store
        await store.append(_session_started("s1"))
        await store.append(_failed("s1", "oom"))

        row = await proj.read("s1")
        assert row["status"] == "failed"
        assert row["last_error"] == "oom"

    async def test_apply_paused(self, wired_store):
        store, proj = wired_store
        await store.append(_session_started("s1"))
        await store.append(_paused("s1"))

        row = await proj.read("s1")
        assert row["status"] == "paused"

    async def test_read_missing_returns_none(self, projector):
        assert await projector.read("nonexistent") is None

    async def test_ignores_non_session_events(self, wired_store):
        store, proj = wired_store
        event = BaseEvent(
            type="ontology.concept.added",
            aggregate_type="ontology",
            aggregate_id="ont-1",
            data={"concept": "test"},
        )
        await store.append(event)
        assert await proj.read("ont-1") is None


# ============================================================================
# P1-AC3: Projector wired into EventStore transaction
# ============================================================================


class TestProjectorWiring:
    async def test_projector_runs_in_same_transaction(self, wired_store):
        """Projection is created atomically with event insert."""
        store, proj = wired_store
        event = _session_started("s1")
        await store.append(event)

        # Both event and projection should exist
        events = await store.replay("session", "s1")
        row = await proj.read("s1")
        assert len(events) == 1
        assert row is not None

    async def test_projector_failure_rolls_back_event(self, db_path):
        """If projector raises, event is not persisted."""
        async def failing_projector(conn, event):
            raise RuntimeError("projector boom")

        store = EventStore(
            f"sqlite+aiosqlite:///{db_path}",
            projectors=[failing_projector],
        )
        await store.initialize()

        from ouroboros.core.errors import PersistenceError
        with pytest.raises(PersistenceError):
            await store.append(_session_started("s1"))

        # Event should NOT be in the store
        events = await store.replay("session", "s1")
        assert len(events) == 0
        await store.close()

    async def test_no_projector_preserves_existing_behavior(self, tmp_path):
        """EventStore without projectors works exactly as before."""
        store = EventStore(f"sqlite+aiosqlite:///{tmp_path / 'plain.db'}")
        await store.initialize()
        await store.append(_session_started("s1"))
        events = await store.replay("session", "s1")
        assert len(events) == 1
        await store.close()


# ============================================================================
# P1-AC4: reconstruct_session() projection path
# ============================================================================


class TestReconstructWithProjection:
    async def test_projection_hit(self, wired_store):
        """reconstruct_session uses projection when fresh."""
        store, proj = wired_store
        await store.append(_session_started("s1"))
        await store.append(_progress("s1", step=1))
        await store.append(_completed("s1"))

        repo = SessionRepository(store, projector=proj)
        result = await repo.reconstruct_session("s1")
        assert result.is_ok
        tracker = result.value
        assert tracker.status == SessionStatus.COMPLETED
        assert tracker.messages_processed == 1

    async def test_missing_projection_falls_back_to_replay(self, wired_store):
        """Missing projection triggers replay + rebuild."""
        store, proj = wired_store
        # Append without projector wired (simulate missing projection)
        store._projectors = []
        await store.append(_session_started("s1"))
        store._projectors = [proj.apply_in_transaction]

        assert await proj.read("s1") is None

        repo = SessionRepository(store, projector=proj)
        result = await repo.reconstruct_session("s1")
        assert result.is_ok
        assert result.value.status == SessionStatus.RUNNING

        # Projection should be rebuilt as side effect
        row = await proj.read("s1")
        assert row is not None
        assert row["status"] == "running"

    async def test_stale_projection_falls_back_to_replay(self, wired_store):
        """Stale projection triggers replay + rebuild."""
        store, proj = wired_store
        await store.append(_session_started("s1"))

        # Append another event without projector (makes projection stale)
        store._projectors = []
        await store.append(_completed("s1"))
        store._projectors = [proj.apply_in_transaction]

        repo = SessionRepository(store, projector=proj)
        result = await repo.reconstruct_session("s1")
        assert result.is_ok
        assert result.value.status == SessionStatus.COMPLETED

    async def test_no_projector_uses_replay(self, wired_store):
        """SessionRepository without projector uses replay path."""
        store, proj = wired_store
        await store.append(_session_started("s1"))

        repo = SessionRepository(store)  # no projector
        result = await repo.reconstruct_session("s1")
        assert result.is_ok


# ============================================================================
# P2-AC1: Replay/projection parity
# ============================================================================


class TestReplayProjectionParity:
    """Verify projection state matches replay-reconstructed state."""

    @pytest.mark.parametrize(
        "scenario,events_fn",
        [
            ("started_only", lambda sid: [_session_started(sid)]),
            ("with_progress", lambda sid: [
                _session_started(sid),
                _progress(sid, 1),
                _progress(sid, 2),
                _progress(sid, 3),
            ]),
            ("completed", lambda sid: [
                _session_started(sid),
                _progress(sid, 1),
                _completed(sid),
            ]),
            ("failed", lambda sid: [
                _session_started(sid),
                _progress(sid, 1),
                _failed(sid, "timeout"),
            ]),
            ("paused_then_completed", lambda sid: [
                _session_started(sid),
                _progress(sid, 1),
                _paused(sid),
                _progress(sid, 2),
                _completed(sid),
            ]),
        ],
    )
    async def test_parity(self, wired_store, scenario, events_fn):
        store, proj = wired_store
        sid = f"parity-{scenario}"

        for event in events_fn(sid):
            await store.append(event)

        # Read from projection
        projection = await proj.read(sid)

        # Reconstruct via replay (no projector)
        repo_replay = SessionRepository(store)
        result = await repo_replay.reconstruct_session(sid)
        assert result.is_ok
        tracker = result.value

        # Compare
        assert projection["status"] == tracker.status.value
        assert projection["messages_processed"] == tracker.messages_processed
        assert projection["seed_id"] == tracker.seed_id
        assert projection["execution_id"] == tracker.execution_id


# ============================================================================
# P2-AC2: Startup gap detection
# ============================================================================


class TestRebuildAllStale:
    async def test_detects_and_rebuilds_stale(self, wired_store):
        store, proj = wired_store

        # Session 1: fully projected (fresh)
        await store.append(_session_started("s1"))

        # Session 2: projected then made stale
        await store.append(_session_started("s2"))
        store._projectors = []
        await store.append(_completed("s2"))
        store._projectors = [proj.apply_in_transaction]

        # Session 3: no projection at all
        store._projectors = []
        await store.append(_session_started("s3"))
        store._projectors = [proj.apply_in_transaction]

        # s1 fresh, s2 stale, s3 missing
        rebuilt = await proj.rebuild_all_stale(store)
        assert rebuilt == 2  # s2 + s3

        # All should now be correct
        assert (await proj.read("s1"))["status"] == "running"
        assert (await proj.read("s2"))["status"] == "completed"
        assert (await proj.read("s3"))["status"] == "running"

    async def test_fresh_projection_skipped(self, wired_store):
        store, proj = wired_store
        await store.append(_session_started("s1"))

        rebuilt = await proj.rebuild_all_stale(store)
        assert rebuilt == 0


# ============================================================================
# P2-AC3: Corrupt projection recovery
# ============================================================================


class TestCorruptProjectionRecovery:
    async def test_invalid_status_triggers_fallback(self, wired_store):
        """Projection with unknown status falls back to replay."""
        store, proj = wired_store
        await store.append(_session_started("s1"))

        # Corrupt the status
        from ouroboros.persistence.schema import session_projections_table
        async with store._engine.begin() as conn:
            await conn.execute(
                session_projections_table.update()
                .where(session_projections_table.c.session_id == "s1")
                .values(status="BOGUS")
            )

        repo = SessionRepository(store, projector=proj)
        result = await repo.reconstruct_session("s1")
        assert result.is_ok
        assert result.value.status == SessionStatus.RUNNING

    async def test_stale_last_event_id_triggers_fallback(self, wired_store):
        """Projection with wrong last_event_id falls back to replay."""
        store, proj = wired_store
        await store.append(_session_started("s1"))

        from ouroboros.persistence.schema import session_projections_table
        async with store._engine.begin() as conn:
            await conn.execute(
                session_projections_table.update()
                .where(session_projections_table.c.session_id == "s1")
                .values(last_event_id="nonexistent-event-id")
            )

        repo = SessionRepository(store, projector=proj)
        result = await repo.reconstruct_session("s1")
        assert result.is_ok
        assert result.value.execution_id == "exec-1"


# ============================================================================
# P2-AC4: Benchmark
# ============================================================================


class TestBenchmark:
    async def test_projection_faster_than_replay(self, wired_store):
        """Projection read is significantly faster than replay for 500 events."""
        store, proj = wired_store
        sid = "bench-500"

        await store.append(_session_started(sid))
        for i in range(500):
            await store.append(_progress(sid, step=i))
        await store.append(_completed(sid))

        # Time projection path
        repo_proj = SessionRepository(store, projector=proj)
        t0 = time.monotonic()
        for _ in range(10):
            result = await repo_proj.reconstruct_session(sid)
            assert result.is_ok
        proj_time = time.monotonic() - t0

        # Time replay path (no projector)
        repo_replay = SessionRepository(store)
        t0 = time.monotonic()
        for _ in range(10):
            result = await repo_replay.reconstruct_session(sid)
            assert result.is_ok
        replay_time = time.monotonic() - t0

        # Projection must be faster. With SQLite and small event counts
        # the gap is modest; in production with larger histories it's >10x.
        assert proj_time < replay_time, (
            f"Projection ({proj_time:.3f}s) should be faster than "
            f"replay ({replay_time:.3f}s)"
        )
        speedup = replay_time / proj_time
        assert speedup > 2, f"Expected >2x speedup, got {speedup:.1f}x"

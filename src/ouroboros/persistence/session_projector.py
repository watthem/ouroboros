"""Session projection for O(1) session status reads.

Maintains a materialized projection of session state, updated atomically
with each event append. Falls back to full replay if projection is missing
or stale.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from ouroboros.events.base import BaseEvent
from ouroboros.observability.logging import get_logger
from ouroboros.persistence.schema import session_projections_table

if TYPE_CHECKING:
    from ouroboros.persistence.event_store import EventStore

log = get_logger(__name__)

# Event types that affect session projections
_SESSION_EVENT_TYPES = frozenset({
    "orchestrator.session.started",
    "orchestrator.progress.updated",
    "orchestrator.session.completed",
    "orchestrator.session.failed",
    "orchestrator.session.paused",
})


class SessionProjector:
    """Maintains a materialized projection of session state.

    Designed to run inside the same transaction as event inserts for atomicity.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def apply_in_transaction(
        self, conn: AsyncConnection, event: BaseEvent
    ) -> None:
        """Fold a single session event into the projection.

        Must be called inside an existing transaction (engine.begin() block).
        Ignores events that don't affect session state.
        """
        if event.type not in _SESSION_EVENT_TYPES:
            return

        session_id = event.aggregate_id
        now = datetime.now(UTC)

        if event.type == "orchestrator.session.started":
            raw_start = event.data.get("start_time")
            if isinstance(raw_start, str):
                start_time = datetime.fromisoformat(raw_start)
            else:
                start_time = raw_start or now

            stmt = sqlite_upsert(session_projections_table).values(
                session_id=session_id,
                execution_id=event.data.get("execution_id", ""),
                seed_id=event.data.get("seed_id", ""),
                status="running",
                start_time=start_time,
                messages_processed=0,
                last_progress=None,
                last_error=None,
                last_event_id=event.id,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["session_id"],
                set_={
                    "last_event_id": event.id,
                    "updated_at": now,
                },
            )
        elif event.type == "orchestrator.progress.updated":
            # Read current row to increment messages_processed
            row = await conn.execute(
                select(session_projections_table.c.messages_processed)
                .where(session_projections_table.c.session_id == session_id)
            )
            current = row.scalar()
            if current is None:
                # No projection yet — skip (will be rebuilt on read)
                return

            stmt = (
                session_projections_table.update()
                .where(session_projections_table.c.session_id == session_id)
                .values(
                    messages_processed=current + 1,
                    last_progress=event.data.get("progress"),
                    last_event_id=event.id,
                    updated_at=now,
                )
            )
        elif event.type == "orchestrator.session.completed":
            stmt = (
                session_projections_table.update()
                .where(session_projections_table.c.session_id == session_id)
                .values(
                    status="completed",
                    last_event_id=event.id,
                    updated_at=now,
                )
            )
        elif event.type == "orchestrator.session.failed":
            stmt = (
                session_projections_table.update()
                .where(session_projections_table.c.session_id == session_id)
                .values(
                    status="failed",
                    last_error=event.data.get("error"),
                    last_event_id=event.id,
                    updated_at=now,
                )
            )
        elif event.type == "orchestrator.session.paused":
            stmt = (
                session_projections_table.update()
                .where(session_projections_table.c.session_id == session_id)
                .values(
                    status="paused",
                    last_event_id=event.id,
                    updated_at=now,
                )
            )
        else:
            return

        await conn.execute(stmt)

    async def read(self, session_id: str) -> dict[str, Any] | None:
        """Read current projection for a session.

        Returns None if no projection exists.
        """
        async with self._engine.begin() as conn:
            result = await conn.execute(
                select(session_projections_table)
                .where(session_projections_table.c.session_id == session_id)
            )
            row = result.mappings().first()
            return dict(row) if row else None

    async def rebuild(
        self, event_store: EventStore, session_id: str
    ) -> None:
        """Full replay rebuild of projection for one session."""
        events = await event_store.replay("session", session_id)
        if not events:
            return

        async with self._engine.begin() as conn:
            # Delete existing projection
            await conn.execute(
                session_projections_table.delete()
                .where(session_projections_table.c.session_id == session_id)
            )
            # Replay all events
            for event in events:
                await self.apply_in_transaction(conn, event)

        log.info(
            "session.projection.rebuilt",
            session_id=session_id,
            event_count=len(events),
        )

    async def rebuild_all_stale(self, event_store: EventStore) -> int:
        """Find and rebuild stale or missing projections.

        A projection is stale if its last_event_id doesn't match the
        latest event for that session aggregate.

        Returns count of projections rebuilt.
        """
        # Get all session start events to find all sessions
        all_sessions = await event_store.get_all_sessions()
        session_ids = {e.aggregate_id for e in all_sessions}

        rebuilt = 0
        for session_id in session_ids:
            # Get latest event for this session
            events = await event_store.replay("session", session_id)
            if not events:
                continue

            latest_event_id = events[-1].id

            # Check projection
            projection = await self.read(session_id)
            if projection is None or projection["last_event_id"] != latest_event_id:
                await self.rebuild(event_store, session_id)
                rebuilt += 1

        if rebuilt:
            log.info(
                "session.projection.rebuild_all_stale",
                total_sessions=len(session_ids),
                rebuilt=rebuilt,
            )

        return rebuilt

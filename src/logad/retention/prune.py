"""
prune.py — keep the database from growing forever on a long-running deployment.

At 10 requests/s the shop makes ~860,000 sessions a day. Every
retention_every_s this deletes rows older than retention_hours: sessions,
scores, rate anomalies, and alerts that are already resolved (an open alert is
never deleted, however old). Templates stay: there are only a few dozen.

Off by default (LOGAD_RETENTION_HOURS unset), so evaluations keep all data.
"""

import asyncio
import contextlib
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine

from logad.common.config import get_settings
from logad.common.db import Alert, RateAnomalyRow, Score, Session, make_engine
from logad.common.shutdown import TRANSIENT_ERRORS, stop_event

log = logging.getLogger("logad.retention")


async def prune(engine: AsyncEngine, cutoff: datetime) -> dict[str, int]:
    """Delete everything older than cutoff; returns rows deleted per table."""
    statements = {
        "scores": delete(Score).where(Score.scored_at < cutoff),
        "sessions": delete(Session).where(Session.started_at < cutoff),
        "rate_anomalies": delete(RateAnomalyRow).where(RateAnomalyRow.window_start < cutoff),
        "alerts": delete(Alert).where(Alert.status == "resolved", Alert.resolved_at < cutoff),
    }
    deleted = {}
    async with engine.begin() as conn:
        for table, statement in statements.items():
            deleted[table] = (await conn.execute(statement)).rowcount
    return deleted


async def run() -> None:
    settings = get_settings()
    if not settings.retention_hours:
        log.info("Retention off (LOGAD_RETENTION_HOURS not set); nothing to do")
        return
    engine = make_engine(settings.database_url)
    log.info("Retention: deleting data older than %.1f h", settings.retention_hours)
    stop = stop_event()
    try:
        while not stop.is_set():
            cutoff = datetime.now(UTC) - timedelta(hours=settings.retention_hours)
            try:
                deleted = await prune(engine, cutoff)
                log.info("Deleted rows older than %s: %s", cutoff.isoformat(), deleted)
            except TRANSIENT_ERRORS as error:
                log.warning("Prune failed (%r), retrying", error)
            # wake early on shutdown instead of sleeping the full interval
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), settings.retention_every_s)
    finally:
        await engine.dispose()

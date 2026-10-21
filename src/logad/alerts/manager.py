"""
manager.py — the alert manager: scores + rate anomalies -> alerts table.

Every few seconds it reads the recent evidence from Postgres (flagged
sessions and rate anomalies of the last few minutes) and the open alerts,
decides what to open, update or resolve (grouping.py), and writes that back.
It keeps nothing important in memory, so a restart simply carries on; the
"one open alert per service" index makes duplicate alerts impossible.

Usage:
    python -m logad.alerts
"""

import asyncio
import logging
import time
from datetime import UTC, datetime

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from logad.alerts.grouping import (
    AlertUpdate,
    Evidence,
    OpenAlert,
    decide,
    evidence_from_score,
    service_of_template,
)
from logad.common.config import Settings, get_settings
from logad.common.db import Alert, make_engine
from logad.common.shutdown import TRANSIENT_ERRORS, stop_event

LOOKBACK_S = 600  # evidence older than this is never needed (quiet_s is far shorter)
log = logging.getLogger("logad.alerts")


def _dt(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, UTC)


class AlertManager:
    def __init__(self, engine: AsyncEngine, settings: Settings):
        self.engine = engine
        self.settings = settings

    async def _evidence(self, conn, since: datetime) -> list[Evidence]:
        evidence = []
        scores = await conn.execute(
            text(
                "select session_id, scored_at, explanation from scores "
                "where anomalous and scored_at >= :since"
            ),
            {"since": since},
        )
        for row in scores:
            evidence += evidence_from_score(
                row.session_id, row.scored_at.timestamp(), row.explanation
            )
        rates = await conn.execute(
            text(
                "select r.detected_at, r.count, r.expected, r.z, r.direction, t.template "
                "from rate_anomalies r join templates t on t.id = r.template_id "
                "where r.detected_at >= :since"
            ),
            {"since": since},
        )
        for row in rates:
            evidence.append(
                Evidence(
                    service_of_template(row.template),
                    row.detected_at.timestamp(),
                    "rate",
                    None,
                    {
                        "template": row.template,
                        "count": row.count,
                        "expected": row.expected,
                        "z": row.z,
                        "direction": row.direction,
                    },
                )
            )
        return evidence

    async def cycle(self, now: float | None = None) -> list[AlertUpdate]:
        """One round: read evidence, decide, write. Returns the updates made."""
        now = time.time() if now is None else now
        async with self.engine.begin() as conn:
            evidence = await self._evidence(conn, _dt(now - LOOKBACK_S))
            rows = await conn.execute(
                select(Alert.id, Alert.service, Alert.opened_at).where(Alert.status == "open")
            )
            open_alerts = {
                r.service: OpenAlert(r.service, r.opened_at.timestamp(), r.id) for r in rows
            }
            updates = decide(
                evidence,
                open_alerts,
                now,
                min_sessions=self.settings.alert_min_sessions,
                window_s=self.settings.alert_window_s,
                quiet_s=self.settings.alert_quiet_s,
            )
            for u in updates:
                await self._write(conn, u, now)
        return updates

    async def _write(self, conn, u: AlertUpdate, now: float) -> None:
        details = {}
        if u.summary is not None:
            details = {
                "last_seen_at": _dt(u.last_seen_at),
                "sessions": u.sessions,
                "detectors": u.detectors,
                "summary": u.summary,
                "examples": u.examples,
            }
        if u.alert_id is None:
            statement = insert(Alert).values(
                service=u.service, status="open", opened_at=_dt(u.opened_at), **details
            )
            await conn.execute(
                statement.on_conflict_do_nothing(
                    index_elements=["service"], index_where=text("status = 'open'")
                )
            )
            log.warning("ALERT opened: %s", u.summary)
            return
        if u.resolve:
            details |= {"status": "resolved", "resolved_at": _dt(now)}
            log.info("Alert %d resolved: %s", u.alert_id, u.service)
        if details:
            await conn.execute(update(Alert).where(Alert.id == u.alert_id).values(**details))


async def run() -> None:
    settings = get_settings()
    engine = make_engine(settings.database_url)
    manager = AlertManager(engine, settings)
    log.info(
        "Alert manager: open at %d flagged requests in %.0fs, resolve after %.0fs quiet",
        settings.alert_min_sessions,
        settings.alert_window_s,
        settings.alert_quiet_s,
    )
    stop = stop_event()
    try:
        while not stop.is_set():
            try:
                await manager.cycle()
            except TRANSIENT_ERRORS as error:
                log.warning("Cycle failed (%r), retrying", error)
            await asyncio.sleep(settings.alert_every_s)
    finally:
        await engine.dispose()

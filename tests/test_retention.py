import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from logad.common.db import Alert, Score, Session, make_engine
from logad.retention.prune import prune

pytestmark = pytest.mark.integration
NOW = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)
OLD, NEW = NOW - timedelta(hours=30), NOW - timedelta(hours=1)


def _session(session_id, at):
    return dict(
        session_id=session_id,
        started_at=at,
        ended_at=at,
        closed_by="end",
        event_count=1,
        services=["shop"],
        events=[],
    )


def _score(session_id, at):
    return dict(
        session_id=session_id,
        model_version="v1",
        lstm_score=0.0,
        timing_score=0.0,
        anomalous=False,
        detectors=[],
        explanation={},
        scored_at=at,
    )


def _alert(service, status, at):
    return dict(
        service=service,
        status=status,
        opened_at=at,
        last_seen_at=at,
        resolved_at=at if status == "resolved" else None,
        sessions=5,
        detectors=["lstm"],
        summary="s",
        examples=[],
    )


async def _check(database_url):
    engine = make_engine(database_url)
    async with engine.begin() as conn:
        await conn.execute(Session.__table__.insert(), [_session("old", OLD), _session("new", NEW)])
        await conn.execute(Score.__table__.insert(), [_score("old", OLD), _score("new", NEW)])
        await conn.execute(
            Alert.__table__.insert(),
            [_alert("payment", "resolved", OLD), _alert("shop", "open", OLD)],
        )
    deleted = await prune(engine, NOW - timedelta(hours=24))
    async with engine.connect() as conn:
        sessions = (await conn.execute(select(Session.session_id))).scalars().all()
        scores = (await conn.execute(select(Score.session_id))).scalars().all()
        alerts = (await conn.execute(select(Alert.service))).scalars().all()
    await engine.dispose()
    return deleted, sessions, scores, alerts


def test_prune_deletes_old_rows_but_never_an_open_alert(database_url):
    deleted, sessions, scores, alerts = asyncio.run(_check(database_url))
    assert sessions == ["new"] and scores == ["new"]
    assert alerts == ["shop"]  # old but still open
    assert deleted == {"scores": 1, "sessions": 1, "rate_anomalies": 0, "alerts": 1}

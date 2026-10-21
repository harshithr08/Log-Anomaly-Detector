from datetime import UTC, datetime

import pytest
from sqlalchemy import inspect, select

from logad.common.db import Session, Template, make_engine

pytestmark = pytest.mark.integration


async def test_migrations_create_the_tables(database_url):
    engine = make_engine(database_url)
    async with engine.connect() as conn:
        tables = await conn.run_sync(lambda sync: inspect(sync).get_table_names())
    await engine.dispose()
    assert {"templates", "sessions", "alembic_version"} <= set(tables)


async def test_session_round_trip(database_url):
    now = datetime.now(UTC)
    events = [{"ts": now.isoformat(), "service": "shop", "template_id": 1, "params": {}}]
    engine = make_engine(database_url)
    async with engine.begin() as conn:
        await conn.execute(Template.__table__.insert().values(id=1, template="shop INFO hi"))
        await conn.execute(
            Session.__table__.insert().values(
                session_id="abc",
                started_at=now,
                ended_at=now,
                closed_by="end",
                event_count=1,
                services=["shop"],
                events=events,
            )
        )
    async with engine.connect() as conn:
        row = (await conn.execute(select(Session.__table__))).one()
    await engine.dispose()
    assert row.services == ["shop"]
    assert row.events == events

"""End to end: shop traffic -> raw-logs -> parser -> session builder -> Postgres.
The sessions must match exactly what the shop wrote."""

import re
from collections import Counter

import pytest
from redis.asyncio import Redis
from sqlalchemy import select

from logad.common.db import Session, make_engine
from logad.common.streams import Keys
from logad.parser.worker import ParserWorker
from logad.sessions.builder import GROUP, SessionBuilder

pytestmark = pytest.mark.integration
REQUEST_ID = re.compile(r"\[req=([^\]]+)\]")


async def test_every_request_becomes_one_complete_session(settings, database_url, shop):
    request_ids = shop.send_traffic(80)
    lines = shop.all_log_lines()
    lines_per_request = Counter(REQUEST_ID.search(line)[1] for line in lines)
    del lines_per_request["-"]  # startup / background lines are not sessions

    settings = settings.model_copy(update={"database_url": database_url})
    keys = Keys(settings.stream_prefix)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    engine = make_engine(database_url)
    for line in lines:
        await redis.xadd(keys.raw_logs, {"source": "test", "line": line})

    parser = ParserWorker(redis, engine, settings, "parser-1")
    builder = SessionBuilder(redis, engine, settings, "builder-1")
    await parser.setup()
    await builder.setup()
    while await parser.process_batch(block_ms=50):
        pass
    while await builder.read_batch(block_ms=50):
        pass
    await builder.save_due(now=float("inf"))  # pretend the grace period has passed

    async with engine.connect() as conn:
        rows = (await conn.execute(select(Session.__table__))).all()
    sessions = {row.session_id: row for row in rows}

    assert set(sessions) == set(request_ids), "one session per request, no extras"
    for session_id, row in sessions.items():
        assert row.event_count == lines_per_request[session_id], "no line lost or misplaced"
        assert row.closed_by == "end"
        assert row.events[0]["message"].startswith("Handling")
        assert row.events[-1]["message"].startswith("Completed")
        timestamps = [event["ts"] for event in row.events]
        assert timestamps == sorted(timestamps)

    checkouts = [row for row in rows if "payment" in row.services]
    assert checkouts and all(row.services[0] == "shop" for row in checkouts)

    for stream in builder.streams:
        assert (await redis.xpending(stream, GROUP))["pending"] == 0, "all events acked"
    await redis.aclose()
    await engine.dispose()

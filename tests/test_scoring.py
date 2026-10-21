"""End to end: shop traffic -> pipeline -> sessions announced -> scored."""

import pytest
from redis.asyncio import Redis
from sqlalchemy import func, select

from logad.common.db import Score, Session, make_engine
from logad.common.streams import Keys
from logad.models.serving import ServingModels
from logad.parser.worker import ParserWorker
from logad.scoring.worker import GROUP, ScoringWorker
from logad.sessions.builder import SessionBuilder

pytestmark = pytest.mark.integration


async def test_every_saved_session_is_scored_once(settings, database_url, shop, tmp_path):
    from models.test_serving import trained_models  # trains a tiny model (needs PyTorch)

    trained_models(tmp_path)
    settings = settings.model_copy(update={"database_url": database_url})
    keys = Keys(settings.stream_prefix)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    engine = make_engine(database_url)

    shop.send_traffic(40)
    for line in sorted(shop.all_log_lines(), key=lambda line: line[:24]):
        await redis.xadd(keys.raw_logs, {"source": "test", "line": line})
    parser = ParserWorker(redis, engine, settings, "parser")
    builder = SessionBuilder(redis, engine, settings, "builder")
    await parser.setup()
    await builder.setup()
    while await parser.process_batch(block_ms=20):
        pass
    while await builder.read_batch(block_ms=20):
        pass
    await builder.save_due(now=float("inf"))
    assert await redis.xlen(keys.sessions) == 40, "every saved session is announced"

    scorer = ScoringWorker(redis, engine, settings, "scorer", ServingModels(tmp_path))
    await scorer.setup()
    while await scorer.process_batch(block_ms=20):
        pass
    assert (await redis.xpending(keys.sessions, GROUP))["pending"] == 0

    # announcing everything again (e.g. after a crash) must not create duplicates
    for session_id in [s async for s in _session_ids(engine)]:
        await redis.xadd(keys.sessions, {"session_id": session_id})
    while await scorer.process_batch(block_ms=20):
        pass

    async with engine.connect() as conn:
        scores = (await conn.execute(select(func.count()).select_from(Score))).scalar()
        sessions = (await conn.execute(select(func.count()).select_from(Session))).scalar()
        versions = (await conn.execute(select(Score.model_version).distinct())).scalars().all()
    assert scores == sessions == 40
    assert versions == ["v1"]
    await redis.aclose()
    await engine.dispose()


async def _session_ids(engine):
    async with engine.connect() as conn:
        for (session_id,) in (await conn.execute(select(Session.session_id))).all():
            yield session_id

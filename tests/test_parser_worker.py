from collections import defaultdict

import pytest
from redis.asyncio import Redis
from sqlalchemy import select

from logad.common.db import Template, make_engine
from logad.common.streams import Keys
from logad.parser.worker import GROUP, ParserWorker

pytestmark = pytest.mark.integration


async def run_two_workers(settings, database_url, lines):
    """Queue the lines, then let two workers in one group process them all."""
    settings = settings.model_copy(update={"database_url": database_url, "parser_batch_size": 25})
    keys = Keys(settings.stream_prefix)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    engine = make_engine(database_url)
    for line in lines:
        await redis.xadd(keys.raw_logs, {"source": "test", "line": line})

    workers = [ParserWorker(redis, engine, settings, f"worker-{i}") for i in range(2)]
    await workers[0].setup()
    handled = [0, 0]
    while True:  # take turns, so both workers see the same kinds of lines
        done = [await w.process_batch(block_ms=50) for w in workers]
        handled = [a + b for a, b in zip(handled, done, strict=True)]
        if not any(done):
            break
    return redis, engine, keys, handled


async def test_two_workers_turn_every_line_into_one_event(settings, database_url, shop):
    shop.send_traffic(60)
    lines = shop.all_log_lines()
    redis, engine, keys, handled = await run_two_workers(settings, database_url, lines)

    assert sum(handled) == len(lines)
    assert all(handled), "both workers did some of the work"
    pending = await redis.xpending(keys.raw_logs, GROUP)
    assert pending["pending"] == 0, "every raw line was acked"

    shard_of_request = defaultdict(set)
    template_ids = defaultdict(set)
    texts = await redis.hgetall(keys.template_texts)
    total = 0
    for shard in range(settings.shard_count):
        for _, event in await redis.xrange(keys.events(shard)):
            total += 1
            shard_of_request[event["request_id"]].add(shard)
            template_ids[texts[event["template_id"]]].add(event["template_id"])
    assert total == len(lines)
    assert all(len(shards) == 1 for shards in shard_of_request.values())
    assert all(len(ids) == 1 for ids in template_ids.values()), "one ID per template"

    async with engine.connect() as conn:
        saved = dict((await conn.execute(select(Template.id, Template.template))).all())
    assert {str(k): v for k, v in saved.items()} == texts, "Postgres mirrors the registry"
    await redis.aclose()
    await engine.dispose()


async def test_unparsable_lines_are_skipped_and_acked(settings, database_url):
    lines = ["garbage", "2026-10-21T00:00:00.000Z INFO shop [req=abc] Handling GET /products"]
    redis, engine, keys, handled = await run_two_workers(settings, database_url, lines)
    events = sum([await redis.xlen(keys.events(shard)) for shard in range(settings.shard_count)])
    assert sum(handled) == 2 and events == 1
    assert (await redis.xpending(keys.raw_logs, GROUP))["pending"] == 0
    await redis.aclose()
    await engine.dispose()

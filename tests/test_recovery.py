"""Crash a worker at the worst moment, start a new one, and check that the
sessions in Postgres still match exactly what the shop wrote."""

import re
from collections import Counter

import pytest
from redis.asyncio import Redis
from sqlalchemy import select

from logad.common.db import Session, make_engine
from logad.common.streams import Keys
from logad.parser.worker import ParserWorker
from logad.sessions.builder import GROUP as BUILDERS
from logad.sessions.builder import SessionBuilder

pytestmark = pytest.mark.integration
REQUEST_ID = re.compile(r"\[req=([^\]]+)\]")


class Pipeline:
    """Shop lines queued in Redis, plus helpers to run workers by hand."""

    def __init__(self, settings, database_url, shop, requests=40):
        # model_copy skips validation, so tests may claim after a few milliseconds
        self.settings = settings.model_copy(
            update={"database_url": database_url, "claim_idle_s": 0.05, "claim_every_s": 0}
        )
        self.realistic_claims = settings.model_copy(update={"database_url": database_url})
        self.keys = Keys(self.settings.stream_prefix)
        self.redis = Redis.from_url(self.settings.redis_url, decode_responses=True)
        self.engine = make_engine(database_url)
        shop.send_traffic(requests)
        # in time order across services, as the shipper sends them
        self.lines = sorted(shop.all_log_lines(), key=lambda line: line[:24])
        self.expected = Counter(REQUEST_ID.search(line)[1] for line in self.lines)
        del self.expected["-"]

    async def queue_lines(self, copies=1):
        for _ in range(copies):
            for number, line in enumerate(self.lines):
                fields = {"source": "test", "line": line, "line_id": f"line-{number}"}
                await self.redis.xadd(self.keys.raw_logs, fields)

    async def parser(self, name):
        worker = ParserWorker(self.redis, self.engine, self.settings, name)
        await worker.setup()
        return worker

    async def builder(self, name):
        builder = SessionBuilder(self.redis, self.engine, self.settings, name)
        await builder.setup()
        return builder

    async def drain(self, worker):
        step = worker.process_batch if isinstance(worker, ParserWorker) else worker.read_batch
        while await step(block_ms=20):
            pass

    async def check_sessions(self):
        async with self.engine.connect() as conn:
            rows = (await conn.execute(select(Session.session_id, Session.event_count))).all()
        got = dict(rows)
        assert len(rows) == len(got), "no session saved twice"
        assert got == dict(self.expected), "every request: one session, every line once"
        for stream in [self.keys.events(s) for s in range(self.settings.shard_count)]:
            assert (await self.redis.xpending(stream, BUILDERS))["pending"] == 0

    async def close(self):
        await self.redis.aclose()
        await self.engine.dispose()


@pytest.fixture
async def pipeline(settings, database_url, shop):
    p = Pipeline(settings, database_url, shop)
    yield p
    await p.close()


async def test_parser_dies_before_its_transaction(pipeline):
    await pipeline.queue_lines()
    doomed = await pipeline.parser("parser-a")
    await doomed.reader.read(block_ms=20)  # reads a batch... and dies without EXEC

    survivor = await pipeline.parser("parser-b")
    await pipeline.drain(survivor)  # finishes the new lines and claims the dead one's
    assert survivor.reader.recovered > 0

    builder = await pipeline.builder("builder")
    await pipeline.drain(builder)
    await builder.save_due(now=float("inf"))
    await pipeline.check_sessions()


@pytest.mark.parametrize("new_name", ["builder-a", "builder-b"], ids=["restart", "replaced"])
async def test_builder_dies_with_open_sessions(pipeline, new_name):
    await pipeline.queue_lines()
    await pipeline.drain(await pipeline.parser("parser"))

    doomed = await pipeline.builder("builder-a")
    await pipeline.drain(doomed)  # sessions are open in memory... and it dies before saving

    survivor = await pipeline.builder(new_name)  # same name = restart, new name = replaced
    await pipeline.drain(survivor)
    await survivor.save_due(now=float("inf"))
    assert survivor.reader.recovered > 0
    await pipeline.check_sessions()


async def test_builder_dies_after_saving_before_acking(pipeline):
    await pipeline.queue_lines()
    await pipeline.drain(await pipeline.parser("parser"))

    doomed = await pipeline.builder("builder-a")
    await pipeline.drain(doomed)
    doomed.redis = None  # no Redis from here on: saving works, acking will crash
    with pytest.raises(AttributeError):
        await doomed.save_due(now=float("inf"))

    survivor = await pipeline.builder("builder-a")
    await pipeline.drain(survivor)
    await survivor.save_due(now=float("inf"))  # same sessions again: inserts are ignored
    await pipeline.check_sessions()


async def test_slow_parser_is_never_overtaken(pipeline):
    """Parser A holds an early batch while parser B races through the later
    lines. The builder must not close sessions still missing A's lines."""
    # small batches; tiny grace so sessions would close early if time ran ahead
    pipeline.settings = pipeline.realistic_claims.model_copy(
        update={"parser_batch_size": 25, "session_grace_s": 0.01}
    )
    await pipeline.queue_lines()
    slow = await pipeline.parser("parser-a")
    await slow.reader.read(block_ms=20)  # takes the first 25 lines... and is slow
    await pipeline.drain(await pipeline.parser("parser-b"))  # everything else

    builder = await pipeline.builder("builder")
    await pipeline.drain(builder)
    await builder.save_due(now=float("inf"))  # may only save what is complete

    await pipeline.drain(await pipeline.parser("parser-a"))  # A finally finishes
    await pipeline.drain(builder)
    await builder.save_due(now=float("inf"))
    assert builder.counts["late"] == 0
    await pipeline.check_sessions()


async def test_every_line_delivered_twice(pipeline):
    pipeline.settings = pipeline.realistic_claims  # no early claiming in this test
    await pipeline.queue_lines(copies=2)  # e.g. the shipper re-sent everything
    await pipeline.drain(await pipeline.parser("parser"))
    builder = await pipeline.builder("builder")
    await pipeline.drain(builder)
    await builder.save_due(now=float("inf"))
    assert builder.counts["duplicate"] == pipeline.expected.total()  # every second copy
    await pipeline.check_sessions()

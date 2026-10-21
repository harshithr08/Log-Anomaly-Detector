"""
worker.py — a parser worker: raw-logs stream -> events streams.

Several workers share the "parsers" consumer group, so Redis hands each raw
line to exactly one of them. For every batch the worker:
    1. parses each line and finds its template ID (shared registry)
    2. in ONE transaction: adds the events to their shard streams AND acks the
       raw lines. Either both happen or neither does; if the worker dies before
       that, the raw lines stay pending and are processed again.

Events of one request always go to the same shard: shard = crc32(request_id) % N.

Usage:
    python -m logad.parser
"""

import json
import logging
import zlib

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from logad.common.config import Settings, get_settings
from logad.common.db import Template, make_engine
from logad.common.streams import Keys
from logad.parser.parse import TemplateFinder, parse_line
from logad.parser.registry import PatternRegistry

GROUP = "parsers"
log = logging.getLogger("logad.parser")


def shard_for(request_id: str, shard_count: int) -> int:
    return zlib.crc32(request_id.encode()) % shard_count


class ParserWorker:
    def __init__(self, redis: Redis, engine: AsyncEngine, settings: Settings, consumer: str):
        self.redis = redis
        self.engine = engine
        self.settings = settings
        self.keys = Keys(settings.stream_prefix)
        self.consumer = consumer
        self.finder = TemplateFinder()
        self.registry = PatternRegistry(redis, self.keys)
        self.unparsable = 0

    async def setup(self) -> None:
        try:
            await self.redis.xgroup_create(self.keys.raw_logs, GROUP, id="0", mkstream=True)
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):  # the group already exists: fine
                raise

    async def process_batch(self, block_ms: int = 1000) -> int:
        """Handle one batch of raw lines. Returns how many were handled."""
        response = await self.redis.xreadgroup(
            GROUP,
            self.consumer,
            {self.keys.raw_logs: ">"},
            count=self.settings.parser_batch_size,
            block=block_ms,
        )
        if not response:
            return 0
        entries = response[0][1]

        events = []  # (shard, fields)
        for _, fields in entries:
            parsed = parse_line(fields["line"])
            if parsed is None:
                self.unparsable += 1
                log.warning("Unparsable line from %s: %.200s", fields["source"], fields["line"])
                continue
            template = self.finder.template(parsed)
            template_id, created = await self.registry.id_for(template)
            if created:
                await self._save_template(template_id, template)
            shard = shard_for(parsed.request_id, self.settings.shard_count)
            events.append(
                (
                    shard,
                    {
                        "request_id": parsed.request_id,
                        "ts": parsed.ts.isoformat(),
                        "service": parsed.service,
                        "level": parsed.level,
                        "template_id": template_id,
                        "message": parsed.message,
                        "params": json.dumps(parsed.params),
                    },
                )
            )

        async with self.redis.pipeline(transaction=True) as tx:  # MULTI ... EXEC
            for shard, fields in events:
                tx.xadd(self.keys.events(shard), fields)
            tx.xack(self.keys.raw_logs, GROUP, *[entry_id for entry_id, _ in entries])
            await tx.execute()
        return len(entries)

    async def _save_template(self, template_id: int, template: str) -> None:
        """Keep a copy in Postgres, for dashboards and model vocabularies."""
        statement = insert(Template).values(id=template_id, template=template)
        async with self.engine.begin() as conn:
            await conn.execute(statement.on_conflict_do_nothing())
        log.info("New template %d: %s", template_id, template)


async def run() -> None:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    engine = make_engine(settings.database_url)
    worker = ParserWorker(redis, engine, settings, settings.consumer_name)
    await worker.setup()
    log.info("Parser %s reading %s", worker.consumer, worker.keys.raw_logs)
    try:
        while True:
            await worker.process_batch()
    finally:
        await redis.aclose()
        await engine.dispose()

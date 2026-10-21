"""
worker.py — a parser worker: raw-logs stream -> events streams.

Several workers share the "parsers" consumer group, so Redis hands each raw
line to exactly one of them. For every batch the worker:
    1. parses each line and finds its template ID (shared registry)
    2. in ONE transaction: adds the events to their shard streams AND acks the
       raw lines. Either both happen or neither does; if the worker dies before
       that, the raw lines stay pending and are finished later (by this worker
       after a restart, or by another one that claims them).

Events of one request always go to the same shard: shard = crc32(request_id) % N.

Usage:
    python -m logad.parser
"""

import json
import logging
import zlib

from redis.asyncio import Redis
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from logad.common.config import Settings, get_settings
from logad.common.db import Template, make_engine
from logad.common.groups import GroupReader
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
        self.reader = GroupReader(
            redis,
            [self.keys.raw_logs],
            GROUP,
            consumer,
            count=settings.parser_batch_size,
            claim_idle_s=settings.claim_idle_s,
            claim_every_s=settings.claim_every_s,
        )
        self.finder = TemplateFinder()
        self.registry = PatternRegistry(redis, self.keys)
        self._saved_templates: set[int] = set()
        self.unparsable = 0

    async def setup(self) -> None:
        await self.reader.setup()

    async def process_batch(self, block_ms: int = 1000) -> int:
        """Handle one batch of raw lines. Returns how many were handled."""
        entries = await self.reader.read(block_ms)
        if not entries:
            return 0

        events = []  # (shard, fields)
        for entry in entries:
            fields = entry.fields
            parsed = parse_line(fields["line"])
            if parsed is None:
                self.unparsable += 1
                log.warning("Unparsable line from %s: %.200s", fields["source"], fields["line"])
                continue
            template = self.finder.template(parsed)
            template_id, created = await self.registry.id_for(template)
            if created:
                log.info("New template %d: %s", template_id, template)
            if template_id not in self._saved_templates:
                await self._save_template(template_id, template)
            shard = shard_for(parsed.request_id, self.settings.shard_count)
            events.append(
                (
                    shard,
                    {
                        # the line's permanent ID: the same line sent twice has the same ID
                        "line_id": fields.get("line_id") or entry.entry_id,
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
            tx.xack(self.keys.raw_logs, GROUP, *[entry.entry_id for entry in entries])
            await tx.execute()
        return len(entries)

    async def _save_template(self, template_id: int, template: str) -> None:
        """Make sure Postgres has a copy (for dashboards and model vocabularies).
        Done once per worker per template; inserting twice is harmless."""
        statement = insert(Template).values(id=template_id, template=template)
        async with self.engine.begin() as conn:
            await conn.execute(statement.on_conflict_do_nothing())
        self._saved_templates.add(template_id)


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

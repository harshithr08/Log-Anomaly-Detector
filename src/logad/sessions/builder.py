"""
builder.py — the session builder: events streams -> sessions table.

Each builder owns some of the events shards (shard % builder_count ==
builder_index), so all events of a request reach the same builder.

An event is acked only after its session is saved in Postgres. Until then it
stays pending in Redis, so a crash loses nothing. Saving is an insert that
ignores an existing session ID, so saving twice is harmless.

Usage:
    python -m logad.sessions
"""

import json
import logging
import time

from redis.asyncio import Redis
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from logad.common.config import Settings, get_settings
from logad.common.db import Session, make_engine
from logad.common.groups import GroupReader
from logad.common.streams import Keys
from logad.sessions.assembler import SessionAssembler, to_row

GROUP = "builders"
log = logging.getLogger("logad.sessions")


def decode_event(entry_id: str, fields: dict) -> dict:
    return {
        "line_id": fields.get("line_id") or entry_id,
        "request_id": fields["request_id"],
        "ts": fields["ts"],
        "service": fields["service"],
        "level": fields["level"],
        "template_id": int(fields["template_id"]),
        "message": fields["message"],
        "params": json.loads(fields["params"]),
    }


class SessionBuilder:
    def __init__(self, redis: Redis, engine: AsyncEngine, settings: Settings, consumer: str):
        self.redis = redis
        self.engine = engine
        self.settings = settings
        self.consumer = consumer
        keys = Keys(settings.stream_prefix)
        self.streams = [
            keys.events(shard)
            for shard in range(settings.shard_count)
            if shard % settings.builder_count == settings.builder_index
        ]
        self.assembler = SessionAssembler(
            settings.session_grace_s,
            settings.session_timeout_s,
            settings.session_end_service,
            settings.session_end_prefix,
        )
        self.reader = GroupReader(
            redis,
            self.streams,
            GROUP,
            consumer,
            count=settings.builder_batch_size,
            claim_idle_s=settings.claim_idle_s,
            claim_every_s=settings.claim_every_s,
        )
        self.counts = {"late": 0, "duplicate": 0}

    async def setup(self) -> None:
        await self.reader.setup()

    async def read_batch(self, block_ms: int = 200) -> int:
        """Read events into open sessions. Returns how many were read."""
        entries = await self.reader.read(block_ms)
        now = time.monotonic()
        ack_now: dict[str, list[str]] = {}  # events that won't be part of a saved session
        for entry in entries:
            event = decode_event(entry.entry_id, entry.fields)
            outcome = self.assembler.add(entry.stream, entry.entry_id, event, now)
            if outcome != "added":
                if outcome in self.counts:
                    self.counts[outcome] += 1
                ack_now.setdefault(entry.stream, []).append(entry.entry_id)
        for stream, entry_ids in ack_now.items():
            await self.redis.xack(stream, GROUP, *entry_ids)
        return len(entries)

    async def save_due(self, now: float | None = None) -> int:
        """Save every finished session, then ack its events. Returns how many."""
        sessions = self.assembler.due(time.monotonic() if now is None else now)
        if not sessions:
            return 0
        rows = [to_row(session) for session in sessions]
        async with self.engine.begin() as conn:
            await conn.execute(insert(Session).values(rows).on_conflict_do_nothing())

        acks: dict[str, list[str]] = {}
        for session in sessions:
            for stream, entry_id in session.entry_ids:
                acks.setdefault(stream, []).append(entry_id)
        async with self.redis.pipeline(transaction=False) as pipe:
            for stream, entry_ids in acks.items():
                pipe.xack(stream, GROUP, *entry_ids)
            await pipe.execute()
        return len(sessions)


async def run() -> None:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    engine = make_engine(settings.database_url)
    builder = SessionBuilder(redis, engine, settings, settings.consumer_name)
    await builder.setup()
    log.info("Session builder %s reading %s", builder.consumer, ", ".join(builder.streams))
    saved, last_report = 0, time.monotonic()
    try:
        while True:
            await builder.read_batch()
            saved += await builder.save_due()
            if time.monotonic() - last_report >= 10:
                log.info(
                    "Saved %d sessions in the last 10s, %d open, %d duplicates dropped, "
                    "%d late events dropped, %d events recovered after crashes",
                    saved,
                    len(builder.assembler.open),
                    builder.counts["duplicate"],
                    builder.counts["late"],
                    builder.reader.recovered,
                )
                saved, last_report = 0, time.monotonic()
    finally:
        await redis.aclose()
        await engine.dispose()

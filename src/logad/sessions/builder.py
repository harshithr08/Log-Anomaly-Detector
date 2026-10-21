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
from redis.exceptions import ResponseError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from logad.common.config import Settings, get_settings
from logad.common.db import Session, make_engine
from logad.common.streams import Keys
from logad.sessions.assembler import SessionAssembler, to_row

GROUP = "builders"
log = logging.getLogger("logad.sessions")


def decode_event(fields: dict) -> dict:
    return {
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
        self.late_events = 0

    async def setup(self) -> None:
        for stream in self.streams:
            try:
                await self.redis.xgroup_create(stream, GROUP, id="0", mkstream=True)
            except ResponseError as error:
                if "BUSYGROUP" not in str(error):
                    raise

    async def read_batch(self, block_ms: int = 200) -> int:
        """Read new events into open sessions. Returns how many were read."""
        response = await self.redis.xreadgroup(
            GROUP,
            self.consumer,
            {stream: ">" for stream in self.streams},
            count=self.settings.builder_batch_size,
            block=block_ms,
        )
        now = time.monotonic()
        no_session: dict[str, list[str]] = {}  # events to ack right away
        read = 0
        for stream, entries in response or []:
            for entry_id, fields in entries:
                read += 1
                outcome = self.assembler.add(stream, entry_id, decode_event(fields), now)
                if outcome != "added":
                    self.late_events += outcome == "late"
                    no_session.setdefault(stream, []).append(entry_id)
        for stream, entry_ids in no_session.items():
            await self.redis.xack(stream, GROUP, *entry_ids)
        return read

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
                    "Saved %d sessions in the last 10s, %d open, %d late events dropped",
                    saved,
                    len(builder.assembler.open),
                    builder.late_events,
                )
                saved, last_report = 0, time.monotonic()
    finally:
        await redis.aclose()
        await engine.dispose()

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

import asyncio
import json
import logging
import time
from datetime import datetime

from redis.asyncio import Redis
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from logad.common.config import Settings, get_settings
from logad.common.db import Session, make_engine
from logad.common.groups import GroupReader, backlog, lag
from logad.common.shutdown import TRANSIENT_ERRORS, stop_event
from logad.common.streams import BUILDERS, PARSERS, Keys
from logad.sessions.assembler import SessionAssembler, to_row

GROUP = BUILDERS
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
        keys = self.keys = Keys(settings.stream_prefix)
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
        """Read events into open sessions. Returns how many *new* events were read
        (entries re-read while their session is still open don't count)."""
        entries = await self.reader.read(block_ms)
        now = time.monotonic()
        ack_now: dict[str, list[str]] = {}  # events that won't be part of a saved session
        new = 0
        for entry in entries:
            event = decode_event(entry.entry_id, entry.fields)
            outcome = self.assembler.add(entry.stream, entry.entry_id, event, now)
            if outcome == "redelivered":
                continue  # still held by an open session: must stay pending
            new += 1
            if outcome != "added":
                if outcome in self.counts:
                    self.counts[outcome] += 1
                ack_now.setdefault(entry.stream, []).append(entry.entry_id)
        for stream, entry_ids in ack_now.items():
            await self.redis.xack(stream, GROUP, *entry_ids)
        return new

    async def _raw_line_time(self, **xrange_args) -> float | None:
        """Event time of one raw line (its leading ISO timestamp), or None if there
        is no such line. A line without a valid timestamp counts as very old, so
        it holds time back until the parsers have dealt with it."""
        entries = await self.redis.xrange(self.keys.raw_logs, count=1, **xrange_args)
        if not entries:
            return None
        line = entries[0][1]["line"]
        try:
            return datetime.fromisoformat(line[:24].replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0

    async def upstream_progress(self) -> tuple[bool, float | None]:
        """(idle, safe): idle means nothing is waiting upstream at all. Otherwise
        `safe` is the event time of the oldest raw line the parsers haven't
        finished: every line older than that has already reached the events
        streams (the shipper sends lines in time order)."""
        if not await backlog(self.redis, self.keys.raw_logs, PARSERS):
            return True, None
        candidates = []
        pending = await self.redis.xpending(self.keys.raw_logs, PARSERS)
        if pending["pending"]:  # being worked on by some parser
            oldest = pending["min"]
            candidates.append(await self._raw_line_time(min=oldest, max=oldest))
        groups = await self.redis.xinfo_groups(self.keys.raw_logs)
        info = next((g for g in groups if g["name"] == PARSERS), None)
        after = f"({info['last-delivered-id']}" if info else "-"
        candidates.append(await self._raw_line_time(min=after, max="+"))  # not handed out yet
        times = [t for t in candidates if t is not None]
        return False, min(times) if times else None

    async def save_due(self, now: float | None = None) -> int:
        """Save every finished session, then ack its events. Returns how many."""
        for stream in self.streams:
            if await lag(self.redis, stream, GROUP):
                return 0  # read everything available first; older events may be in there
        idle, safe = await self.upstream_progress()
        now = time.monotonic() if now is None else now
        sessions = self.assembler.due(now, upstream_idle=idle, upstream_safe=safe)
        if not sessions:
            return 0
        rows = [to_row(session) for session in sessions]
        try:
            async with self.engine.begin() as conn:
                await conn.execute(insert(Session).values(rows).on_conflict_do_nothing())
        except Exception:
            self.assembler.reopen(sessions)  # not saved: keep them for the next try
            raise

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
    stop = stop_event()
    backoff = 0.5
    try:
        while not stop.is_set():
            try:
                await builder.read_batch()
                saved += await builder.save_due()
                backoff = 0.5
            except TRANSIENT_ERRORS as error:
                # Unsaved sessions were put back and their events are still pending.
                log.warning("Batch failed (%r), retrying in %.1fs", error, backoff)
                builder.reader.retry_pending()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10)
                continue
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
        # Open sessions are not saved half-finished: their events stay pending
        # and the next builder rebuilds them.
        log.info(
            "Session builder %s stopping, %d open sessions left pending",
            builder.consumer,
            len(builder.assembler.open),
        )
        await redis.aclose()
        await engine.dispose()

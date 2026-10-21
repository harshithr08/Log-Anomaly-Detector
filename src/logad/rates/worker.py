"""
worker.py — the rate detector: events streams -> rate_anomalies table.

It reads the same events streams as the session builder, through its own
consumer group ("rates"), so every event reaches both. Events are acked only
after the window that counted them has been judged and saved; a restarted
worker re-reads them (duplicates are dropped by line ID). Its moving averages
live in memory, so after a restart it warms up again for a minute.

Usage:
    python -m logad.rates
"""

import asyncio
import logging
from datetime import UTC, datetime

from redis.asyncio import Redis
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from logad.common.config import Settings, get_settings
from logad.common.db import RateAnomalyRow, make_engine
from logad.common.groups import GroupReader
from logad.common.shutdown import TRANSIENT_ERRORS, stop_event
from logad.common.streams import RATES, Keys
from logad.rates.counter import RateCounter
from logad.sessions.assembler import epoch

GROUP = RATES
log = logging.getLogger("logad.rates")


class RateWorker:
    def __init__(self, redis: Redis, engine: AsyncEngine, settings: Settings, consumer: str):
        self.redis = redis
        self.engine = engine
        keys = Keys(settings.stream_prefix)
        self.reader = GroupReader(
            redis,
            [keys.events(shard) for shard in range(settings.shard_count)],
            GROUP,
            consumer,
            count=settings.builder_batch_size,
            claim_idle_s=settings.claim_idle_s,
            claim_every_s=settings.claim_every_s,
            exclusive=True,  # one rate detector: any other consumer here is gone
        )
        self.counter = RateCounter(window_s=settings.rate_window_s)
        self.found = 0

    async def setup(self) -> None:
        await self.reader.setup()

    async def process_batch(self, block_ms: int = 1000) -> int:
        """Count one batch of events; judge and save finished windows."""
        entries = await self.reader.read(block_ms)
        for entry in entries:
            outcome = self.counter.add(
                epoch(entry.fields["ts"]),
                int(entry.fields["template_id"]),
                entry.fields.get("line_id") or entry.entry_id,
                (entry.stream, entry.entry_id),
            )
            if outcome == "late":  # its window is already saved
                await self.redis.xack(entry.stream, GROUP, entry.entry_id)

        for window, anomalies in self.counter.close_ready():
            if anomalies:
                rows = [
                    {
                        "window_start": datetime.fromtimestamp(a.window_start, UTC),
                        "template_id": a.template_id,
                        "count": a.count,
                        "expected": round(a.expected, 2),
                        "z": round(a.z, 2),
                        "direction": a.direction,
                    }
                    for a in anomalies
                ]
                async with self.engine.begin() as conn:
                    await conn.execute(insert(RateAnomalyRow).values(rows).on_conflict_do_nothing())
                self.found += len(rows)
            by_stream: dict[str, list[str]] = {}
            for stream, entry_id in window.entry_ids:
                by_stream.setdefault(stream, []).append(entry_id)
            async with self.redis.pipeline(transaction=False) as pipe:
                for stream, ids in by_stream.items():
                    pipe.xack(stream, GROUP, *ids)
                await pipe.execute()
        return len(entries)


async def run() -> None:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    engine = make_engine(settings.database_url)
    worker = RateWorker(redis, engine, settings, settings.consumer_name)
    await worker.setup()
    log.info(
        "Rate detector %s counting %ss windows", worker.reader.consumer, settings.rate_window_s
    )
    stop = stop_event()
    backoff = 0.5
    try:
        while not stop.is_set():
            try:
                await worker.process_batch()
                backoff = 0.5
            except TRANSIENT_ERRORS as error:
                log.warning("Batch failed (%r), retrying in %.1fs", error, backoff)
                worker.reader.retry_pending()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10)
    finally:
        log.info("Rate detector stopping: %d rate anomalies found", worker.found)
        await redis.aclose()
        await engine.dispose()

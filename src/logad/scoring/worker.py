"""
worker.py — a scoring worker: sessions stream -> scores table.

The session builder announces every saved session on the sessions stream.
Scoring workers share the "scorers" consumer group (the same safe reading as
the other workers: own pending entries first, claims from dead workers). For
each batch they load the sessions from Postgres, score them with the current
model version (LSTM via ONNX Runtime + timing), save one row per (session,
model version) and only then ack.

When models/latest points to a new version, the worker switches to it.

Usage:
    python -m logad.scoring
"""

import asyncio
import logging
import time
from pathlib import Path

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from logad.common.config import Settings, get_settings
from logad.common.db import Score, Session, Template, make_engine
from logad.common.groups import GroupReader
from logad.common.shutdown import TRANSIENT_ERRORS, stop_event
from logad.common.streams import SCORERS, Keys
from logad.models.dataset import session_from_row
from logad.models.serving import ServingModels, resolve_version

GROUP = SCORERS
CHECK_FOR_NEW_MODEL_S = 30
log = logging.getLogger("logad.scoring")


class ScoringWorker:
    def __init__(
        self,
        redis: Redis,
        engine: AsyncEngine,
        settings: Settings,
        consumer: str,
        models: ServingModels,
    ):
        self.redis = redis
        self.engine = engine
        self.settings = settings
        self.models = models
        self.keys = Keys(settings.stream_prefix)
        self.reader = GroupReader(
            redis,
            [self.keys.sessions],
            GROUP,
            consumer,
            count=settings.scoring_batch_size,
            claim_idle_s=settings.claim_idle_s,
            claim_every_s=settings.claim_every_s,
        )
        self.templates: dict[int, str] = {}
        self.flagged = 0

    async def setup(self) -> None:
        await self.reader.setup()

    async def _load(self, session_ids: list[str]) -> list:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(Session.__table__).where(Session.session_id.in_(session_ids))
                )
            ).all()
            needed = {e["template_id"] for row in rows for e in row.events}
            if not needed <= self.templates.keys():  # new templates since last time
                result = await conn.execute(select(Template.id, Template.template))
                self.templates = dict(result.all())
        return [session_from_row(row, self.templates) for row in rows]

    async def process_batch(self, block_ms: int = 1000) -> int:
        """Score one batch of announced sessions. Returns how many were read."""
        entries = await self.reader.read(block_ms)
        if not entries:
            return 0
        sessions = await self._load(list({e.fields["session_id"] for e in entries}))
        if len(sessions) < len({e.fields["session_id"] for e in entries}):
            log.warning("Some announced sessions are not in the database; skipping them")

        rows = []
        for verdict in self.models.score(sessions):
            fired = [name for name in ("lstm", "timing") if getattr(verdict, f"{name}_flag")]
            rows.append(
                {
                    "session_id": verdict.session_id,
                    "model_version": self.models.version,
                    "lstm_score": verdict.lstm_score,
                    "timing_score": verdict.timing_score,
                    "anomalous": verdict.anomalous,
                    "detectors": fired,
                    "explanation": verdict.explanation,
                }
            )
        if rows:
            async with self.engine.begin() as conn:
                await conn.execute(insert(Score).values(rows).on_conflict_do_nothing())
        await self.redis.xack(self.keys.sessions, GROUP, *[e.entry_id for e in entries])
        self.flagged += sum(row["anomalous"] for row in rows)
        return len(entries)


async def wait_for_models(models_dir: Path, stop: asyncio.Event) -> ServingModels | None:
    while not stop.is_set():
        try:
            return ServingModels(models_dir)
        except FileNotFoundError:
            log.warning(
                "No trained model in %s yet (run: python -m logad.models.train)", models_dir
            )
            await asyncio.sleep(10)
    return None


async def run() -> None:
    settings = get_settings()
    stop = stop_event()
    models = await wait_for_models(settings.models_dir, stop)
    if models is None:
        return
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    engine = make_engine(settings.database_url)
    worker = ScoringWorker(redis, engine, settings, settings.consumer_name, models)
    await worker.setup()
    log.info("Scorer %s using model %s", worker.reader.consumer, models.version)
    backoff, last_model_check, scored = 0.5, time.monotonic(), 0
    try:
        while not stop.is_set():
            if time.monotonic() - last_model_check >= CHECK_FOR_NEW_MODEL_S:
                last_model_check = time.monotonic()
                if resolve_version(settings.models_dir) != worker.models.version:
                    worker.models = ServingModels(settings.models_dir)
                    log.info("Switched to model %s", worker.models.version)
            try:
                scored += await worker.process_batch()
                backoff = 0.5
            except TRANSIENT_ERRORS as error:
                log.warning("Batch failed (%r), retrying in %.1fs", error, backoff)
                worker.reader.retry_pending()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10)
    finally:
        log.info("Scorer stopping: %d sessions scored, %d flagged", scored, worker.flagged)
        await redis.aclose()
        await engine.dispose()

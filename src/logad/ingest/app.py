"""
app.py — the ingest API.

    POST /ingest   {"lines": [{"source": "payment", "line": "..."}, ...]}

Each line becomes one entry in the raw-logs Redis stream. The response is sent
only after Redis has stored every line, so a 200 means "safely queued" and the
shipper may move on.

Run: uvicorn logad.ingest.app:create_app --factory --port 8100
"""

from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from logad.common.config import Settings, get_settings
from logad.common.streams import Keys

MAX_BATCH_LINES = 5000


class Line(BaseModel):
    source: str = Field(max_length=100)
    line: str = Field(max_length=10_000)


class Batch(BaseModel):
    lines: list[Line] = Field(max_length=MAX_BATCH_LINES)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    keys = Keys(settings.stream_prefix)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
        yield
        await app.state.redis.aclose()

    app = FastAPI(title="logad-ingest", lifespan=lifespan)

    @app.get("/health")
    async def health():
        await app.state.redis.ping()
        return {"status": "ok"}

    @app.post("/ingest")
    async def ingest(batch: Batch):
        received_at = datetime.now(UTC).isoformat()
        async with app.state.redis.pipeline(transaction=False) as pipe:
            for item in batch.lines:
                pipe.xadd(
                    keys.raw_logs,
                    {"source": item.source, "line": item.line, "received_at": received_at},
                )
            await pipe.execute()
        return {"accepted": len(batch.lines)}

    return app

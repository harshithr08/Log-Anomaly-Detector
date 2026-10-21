"""
stats.py — the live numbers shown on the dashboard, sampled once a second.

Lines per second come from Redis's own counter of entries ever added to the
raw-logs stream: the difference between two samples, divided by the time
between them. Session and anomaly rates come from Postgres (last 10 seconds).
"""

import time

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from logad.common.groups import backlog
from logad.common.streams import BUILDERS, PARSERS, SCORERS, Keys

RATE_WINDOW_S = 10


class StatsSampler:
    def __init__(self, redis: Redis, engine: AsyncEngine, keys: Keys, shard_count: int):
        self.redis = redis
        self.engine = engine
        self.keys = keys
        self.shard_count = shard_count
        self._previous: tuple[float, int] | None = None  # (time, raw lines ever added)
        self.latest: dict = {}

    async def _lines_added(self) -> int:
        if not await self.redis.exists(self.keys.raw_logs):
            return 0
        info = await self.redis.xinfo_stream(self.keys.raw_logs)
        return int(info.get("entries-added", info.get("length", 0)))

    async def sample(self) -> dict:
        now, added = time.time(), await self._lines_added()
        lines_per_s = 0.0
        if self._previous and now > self._previous[0]:
            lines_per_s = max(0.0, (added - self._previous[1]) / (now - self._previous[0]))
        self._previous = (now, added)

        events_backlog = 0
        for shard in range(self.shard_count):
            events_backlog += await backlog(self.redis, self.keys.events(shard), BUILDERS)
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "select "
                        " (select count(*) from sessions"
                        "   where created_at > now() - make_interval(secs => :w)) as sessions,"
                        " (select count(*) from scores"
                        "   where anomalous and scored_at > now() - make_interval(secs => :w))"
                        "   as flagged,"
                        " (select count(*) from alerts where status = 'open') as open_alerts,"
                        " (select model_version from scores order by scored_at desc limit 1)"
                        "   as model_version"
                    ),
                    {"w": RATE_WINDOW_S},
                )
            ).one()
        self.latest = {
            "at": now,
            "lines_per_s": round(lines_per_s, 1),
            "sessions_per_s": round(row.sessions / RATE_WINDOW_S, 2),
            "flagged_per_s": round(row.flagged / RATE_WINDOW_S, 2),
            "backlog": {
                "raw_lines": await backlog(self.redis, self.keys.raw_logs, PARSERS),
                "events": events_backlog,
                "sessions_to_score": await backlog(self.redis, self.keys.sessions, SCORERS),
            },
            "open_alerts": row.open_alerts,
            "model_version": row.model_version,
        }
        return self.latest

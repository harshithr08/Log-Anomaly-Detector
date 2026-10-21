"""
groups.py — read a Redis consumer group so that no entry is ever left behind.

An entry is "pending" from the moment a consumer reads it until it acks it.
If the consumer crashes in between, the entry stays pending. GroupReader makes
sure someone finishes it:

    1. after a (re)start: first this consumer's own pending entries
    2. every few seconds: entries pending longer than claim_idle_s at *any*
       consumer, i.e. one that died or was replaced (XAUTOCLAIM)
    3. otherwise: new entries

Entries can therefore be processed more than once; consumers must be idempotent.
"""

import time
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import ResponseError


@dataclass
class Entry:
    stream: str
    entry_id: str
    fields: dict


async def ensure_group(redis: Redis, stream: str, group: str) -> None:
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except ResponseError as error:
        if "BUSYGROUP" not in str(error):  # the group already exists: fine
            raise


class GroupReader:
    def __init__(
        self,
        redis: Redis,
        streams: list[str],
        group: str,
        consumer: str,
        count: int,
        claim_idle_s: float,
        claim_every_s: float = 5.0,
    ):
        self.redis = redis
        self.streams = streams
        self.group = group
        self.consumer = consumer
        self.count = count
        self.claim_idle_ms = int(claim_idle_s * 1000)
        self.claim_every_s = claim_every_s
        self._own_cursor = {stream: "0" for stream in streams}  # paging through own pending
        self._last_claim = time.monotonic()
        self.recovered = 0  # entries finished on behalf of a crashed consumer (incl. self)

    async def setup(self) -> None:
        for stream in self.streams:
            await ensure_group(self.redis, stream, self.group)

    async def read(self, block_ms: int) -> list[Entry]:
        if self._own_cursor:
            entries = await self._read_own_pending()
            if entries:
                return entries
        if time.monotonic() - self._last_claim >= self.claim_every_s:
            self._last_claim = time.monotonic()
            entries = await self._claim_stale()
            if entries:
                return entries
        response = await self.redis.xreadgroup(
            self.group,
            self.consumer,
            {stream: ">" for stream in self.streams},
            count=self.count,
            block=block_ms,
        )
        return [Entry(s, eid, f) for s, entries in response or [] for eid, f in entries]

    async def _read_own_pending(self) -> list[Entry]:
        """Entries this consumer read before a restart but never acked."""
        response = await self.redis.xreadgroup(
            self.group, self.consumer, dict(self._own_cursor), count=self.count
        )
        found = []
        for stream, entries in response or []:
            if not entries:
                self._own_cursor.pop(stream, None)  # nothing left on this stream
                continue
            self._own_cursor[stream] = entries[-1][0]
            found += [Entry(stream, eid, f) for eid, f in entries]
        for stream in [s for s in self._own_cursor if s not in {r[0] for r in response or []}]:
            self._own_cursor.pop(stream)
        found = await self._ack_deleted(found)
        self.recovered += len(found)
        return found

    async def _claim_stale(self) -> list[Entry]:
        """Take over entries that have been pending too long at any consumer."""
        claimed = []
        for stream in self.streams:
            _, entries, deleted = await self.redis.xautoclaim(
                stream, self.group, self.consumer, self.claim_idle_ms, "0-0", count=self.count
            )
            if deleted:
                await self.redis.xack(stream, self.group, *deleted)
            claimed += [Entry(stream, eid, f) for eid, f in entries]
        claimed = await self._ack_deleted(claimed)
        self.recovered += len(claimed)
        return claimed

    async def _ack_deleted(self, entries: list[Entry]) -> list[Entry]:
        """Entries trimmed from the stream come back without fields: just ack them."""
        for entry in entries:
            if not entry.fields:
                await self.redis.xack(entry.stream, self.group, entry.entry_id)
        return [entry for entry in entries if entry.fields]

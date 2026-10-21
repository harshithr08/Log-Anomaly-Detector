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
Recovered entries carry how many times they were handed out, so a consumer can
spot a "poison" entry that keeps crashing workers.

Every few seconds the reader also trims the stream: entries that every consumer
has finished are deleted, so streams don't grow forever.
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
    deliveries: int = 1  # how many times this entry has been handed to a consumer


async def ensure_group(redis: Redis, stream: str, group: str) -> None:
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except ResponseError as error:
        if "BUSYGROUP" not in str(error):  # the group already exists: fine
            raise


def _id_key(entry_id: str) -> tuple[int, int]:
    millis, _, seq = entry_id.partition("-")
    return int(millis), int(seq or 0)


async def trim_finished(redis: Redis, stream: str, group: str) -> int:
    """Delete entries that EVERY consumer group on the stream has finished (a
    stream can feed several groups, e.g. the session builder and the rate
    detector): for each group, everything older than its oldest pending entry,
    or than the last entry it was handed if nothing is pending."""
    keep = []
    for info in await redis.xinfo_groups(stream):
        if info["last-delivered-id"] == "0-0":
            return 0  # this group hasn't read anything yet: keep everything
        pending = await redis.xpending(stream, info["name"])
        keep.append(pending["min"] if pending["pending"] else info["last-delivered-id"])
    if not keep:
        return 0
    return await redis.xtrim(stream, minid=min(keep, key=_id_key), approximate=False)


async def backlog(redis: Redis, stream: str, group: str) -> int:
    """Entries waiting for this group: not yet handed out, plus handed out but unfinished."""
    groups = await redis.xinfo_groups(stream) if await redis.exists(stream) else []
    info = next((g for g in groups if g["name"] == group), None)
    if info is None:
        return await redis.xlen(stream)
    return (info["lag"] or 0) + info["pending"]


async def forget_departed(redis: Redis, stream: str, group: str, idle_s: float = 3600) -> int:
    """Remove consumers that hold no entries and have been silent for a long
    time (containers that were replaced), so the group's list stays short."""
    removed = 0
    for consumer in await redis.xinfo_consumers(stream, group):
        if consumer["pending"] == 0 and consumer["idle"] >= idle_s * 1000:
            await redis.xgroup_delconsumer(stream, group, consumer["name"])
            removed += 1
    return removed


async def lag(redis: Redis, stream: str, group: str) -> int:
    """Entries not yet handed out to any consumer of this group."""
    groups = await redis.xinfo_groups(stream) if await redis.exists(stream) else []
    info = next((g for g in groups if g["name"] == group), None)
    return await redis.xlen(stream) if info is None else (info["lag"] or 0)


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
        self._last_claim = time.monotonic()
        self.recovered = 0  # entries finished on behalf of a crashed consumer (incl. self)
        self.retry_pending()

    def retry_pending(self) -> None:
        """Read this consumer's own pending entries again (after a restart, or
        after an error left a batch unfinished)."""
        self._own_cursor = {stream: "0" for stream in self.streams}

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
            for stream in self.streams:
                await trim_finished(self.redis, stream, self.group)
                await forget_departed(self.redis, stream, self.group)
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
        """Entries this consumer read earlier but never acked."""
        response = await self.redis.xreadgroup(
            self.group, self.consumer, dict(self._own_cursor), count=self.count
        )
        found = []
        returned = {stream for stream, _ in response or []}
        for stream, entries in response or []:
            if not entries:
                self._own_cursor.pop(stream, None)  # nothing left on this stream
                continue
            self._own_cursor[stream] = entries[-1][0]
            found += [Entry(stream, eid, f) for eid, f in entries]
        for stream in [s for s in self._own_cursor if s not in returned]:
            self._own_cursor.pop(stream)
        return await self._recovered(found)

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
        return await self._recovered(claimed)

    async def _recovered(self, entries: list[Entry]) -> list[Entry]:
        """Ack entries that were trimmed away (they come back without fields) and
        look up how often the others have been handed out."""
        for entry in entries:
            if not entry.fields:
                await self.redis.xack(entry.stream, self.group, entry.entry_id)
        entries = [entry for entry in entries if entry.fields]
        for entry in entries:
            info = await self.redis.xpending_range(
                entry.stream, self.group, min=entry.entry_id, max=entry.entry_id, count=1
            )
            entry.deliveries = info[0]["times_delivered"] if info else 1
        self.recovered += len(entries)
        return entries

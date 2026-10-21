"""
ship.py — send new log lines to the ingest API, at least once.

Loop: read new lines from every *.log file -> send them as one batch ->
only if the send succeeded, save the new positions. A crash or a failed send
means the same lines are sent again (never lost; maybe duplicated, which the
pipeline handles downstream).

Lines from different files are merged in time order (every line starts with an
ISO-8601 timestamp, which sorts correctly as text). Otherwise, whenever batches
are full (e.g. while catching up), the first file would take every batch and run
far ahead of the others, and the session builder would see one service's lines
long before another's.

Usage:
    python -m logad.shipper
"""

import asyncio
import heapq
import itertools
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path

import httpx

from logad.common.config import get_settings
from logad.common.shutdown import stop_event
from logad.shipper.tail import FileTailer, OffsetStore, Position

log = logging.getLogger("logad.shipper")

Send = Callable[[list[dict]], Awaitable[None]]  # raises if the batch was not accepted
TIMESTAMP_CHARS = 24  # "2026-10-21T02:50:38.384Z"


class Shipper:
    def __init__(self, log_dir: Path, store: OffsetStore, send: Send, batch_lines: int = 500):
        self.log_dir = log_dir
        self.store = store
        self.send = send
        self.batch_lines = batch_lines
        self.tailers = {
            name: FileTailer(log_dir / name, position) for name, position in store.load().items()
        }

    def _discover(self) -> None:
        for path in sorted(self.log_dir.glob("*.log")):
            self.tailers.setdefault(path.name, FileTailer(path))

    async def ship_once(self) -> int:
        """Send one batch of new lines. Returns how many lines were sent."""
        self._discover()
        per_file = [
            [(name, line) for line in tailer.read(max_lines=self.batch_lines)]
            for name, tailer in self.tailers.items()
        ]
        # Oldest lines first across files; each file's own order is kept.
        merged = heapq.merge(*per_file, key=lambda item: item[1].text[:TIMESTAMP_CHARS])
        chosen = list(itertools.islice(merged, self.batch_lines))
        if not chosen:
            return 0

        batch = [
            {"source": name.removesuffix(".log"), "line": line.text, "id": line.id}
            for name, line in chosen
        ]
        await self.send(batch)  # raises on failure: nothing is committed
        last_sent: dict[str, Position] = {name: line.after for name, line in chosen}
        for name, position in last_sent.items():  # each file: just after its last line sent
            self.tailers[name].commit(position)
        self.store.save({name: t.position for name, t in self.tailers.items()})
        return len(batch)


def http_sender(client: httpx.AsyncClient, url: str) -> Send:
    async def send(batch: list[dict]) -> None:
        response = await client.post(url, json={"lines": batch})
        response.raise_for_status()

    return send


async def run() -> None:
    settings = get_settings()
    async with httpx.AsyncClient(timeout=10) as client:
        shipper = Shipper(
            settings.shipper_log_dir,
            OffsetStore(settings.shipper_state_file),
            http_sender(client, f"{settings.ingest_url}/ingest"),
            settings.shipper_batch_lines,
        )
        log.info("Shipping %s/*.log to %s", settings.shipper_log_dir, settings.ingest_url)
        stop = stop_event()
        backoff = settings.shipper_poll_s
        while not stop.is_set():
            try:
                sent = await shipper.ship_once()
                backoff = settings.shipper_poll_s
                if sent == 0 or sent < settings.shipper_batch_lines:
                    await asyncio.sleep(settings.shipper_poll_s)
            except (httpx.HTTPError, OSError) as error:
                # includes 429 Too Many Requests: back off, lines wait in the files
                log.warning("Send failed (%s), retrying in %.1fs", error, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
        log.info("Shipper stopping")

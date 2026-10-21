"""
ship.py — send new log lines to the ingest API, at least once.

Loop: read new lines from every *.log file -> send them as one batch ->
only if the send succeeded, save the new positions. A crash or a failed send
means the same lines are sent again (never lost; maybe duplicated, which the
pipeline handles downstream).

Usage:
    python -m logad.shipper
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path

import httpx

from logad.common.config import get_settings
from logad.shipper.tail import FileTailer, OffsetStore, Position

log = logging.getLogger("logad.shipper")

Send = Callable[[list[dict]], Awaitable[None]]  # raises if the batch was not accepted


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
        batch: list[dict] = []
        proposed: dict[str, Position] = {}
        for name, tailer in self.tailers.items():
            room = self.batch_lines - len(batch)
            if room <= 0:
                break  # the remaining files go in the next batch
            lines, position = tailer.read(max_lines=room)
            source = name.removesuffix(".log")
            batch += [{"source": source, "line": line, "id": line_id} for line_id, line in lines]
            proposed[name] = position

        if batch:
            await self.send(batch)  # raises on failure: nothing is committed
        for name, position in proposed.items():
            self.tailers[name].commit(position)
        if proposed:
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
        backoff = settings.shipper_poll_s
        while True:
            try:
                sent = await shipper.ship_once()
                backoff = settings.shipper_poll_s
                if sent == 0 or sent < settings.shipper_batch_lines:
                    await asyncio.sleep(settings.shipper_poll_s)
            except (httpx.HTTPError, OSError) as error:
                log.warning("Send failed (%s), retrying in %.1fs", error, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)

"""
loadgen.py — replay real shop sessions into the ingest API, as fast as asked.

The shop itself tops out at a few hundred lines per second, which would
measure the shop, not the pipeline. So we replay recorded sessions
(data/shop) with fresh request IDs and current timestamps, in the exact
format the shop writes, straight to the ingest API. A 429 (backpressure)
is respected: wait and resend the same batch.
"""

import asyncio
import gzip
import itertools
import json
import time
import uuid
from pathlib import Path

import httpx

DATA = Path("data/shop/sessions.jsonl.gz")
BATCH_LINES = 500


def load_sessions(limit: int = 5000) -> list[list[tuple[float, dict]]]:
    """Normal recorded sessions as [(offset from first line in s, event)]."""
    sessions = []
    with gzip.open(DATA, "rt", encoding="utf-8") as f:
        for line in f:
            s = json.loads(line)
            if s["label"] or s["closed_by"] != "end":
                continue
            events = s["events"]
            first = events[0]["ts"]
            offsets = [_epoch(e["ts"]) - _epoch(first) for e in events]
            sessions.append([(o, e) for o, e in zip(offsets, events, strict=True)])
            if len(sessions) >= limit:
                break
    return sessions


def _epoch(ts: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(ts).timestamp()


def _stamp(t: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + f".{int(t * 1000) % 1000:03d}Z"


def render(session, now: float, serial: int) -> list[dict]:
    """One session as fresh log lines: new request ID, last line stamped `now`."""
    request_id = uuid.uuid4().hex[:16]
    span = session[-1][0]
    return [
        {
            "source": e["service"],
            "line": f"{_stamp(now - span + offset)} {e['level']} {e['service']} "
            f"[req={request_id}] {e['message']}",
            "id": f"bench.{serial}.{i}",
        }
        for i, (offset, e) in enumerate(session)
    ]


async def blast(url: str, rate: float | None, duration: float, sessions) -> dict:
    """Send sessions for `duration` seconds at `rate` lines/s (None = as fast as
    the pipeline accepts). Returns lines accepted and 429s received."""
    accepted = refused = 0
    pool = itertools.cycle(sessions)
    serial = itertools.count()
    started = time.monotonic()
    async with httpx.AsyncClient(timeout=30) as client:
        while time.monotonic() - started < duration:
            batch = []
            while len(batch) < BATCH_LINES:
                batch += render(next(pool), time.time(), next(serial))
            while True:
                response = await client.post(f"{url}/ingest", json={"lines": batch})
                if response.status_code != 429:
                    response.raise_for_status()
                    break
                refused += 1
                await asyncio.sleep(float(response.headers.get("retry-after", 1)))
            accepted += len(batch)
            if rate:  # pace to the target rate
                ahead = accepted / rate - (time.monotonic() - started)
                if ahead > 0:
                    await asyncio.sleep(ahead)
    return {"lines": accepted, "refused_batches": refused, "seconds": time.monotonic() - started}

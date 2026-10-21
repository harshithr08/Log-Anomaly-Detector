"""
stack.py — helpers shared by the eval scripts: drive the Docker Compose stack.
"""

import asyncio
import os
import shutil
import subprocess
import sys
import time

from redis.asyncio import Redis

from logad.common.config import get_settings
from logad.common.groups import backlog
from logad.common.streams import BUILDERS, PARSERS, Keys


def compose(*args: str, check: bool = True, env: dict | None = None) -> str:
    result = subprocess.run(
        ["docker", "compose", *args],
        capture_output=True,
        text=True,
        encoding="utf-8",  # Docker prints UTF-8; Windows would default to cp1252
        errors="replace",
        check=False,
        env={**os.environ, **(env or {})},
    )
    if check and result.returncode != 0:
        sys.exit(f"docker compose {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout.strip()


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], capture_output=True, check=True)


def containers(service: str) -> list[str]:
    return compose("ps", "-q", service).split()


def bring_up(service: str) -> None:
    """Start the service's missing or stopped containers (two parsers)."""
    scale = ["--scale", "parser=2"] if service == "parser" else []
    compose("up", "-d", "--no-deps", *scale, service)


def start_stack(traffic_rate: float, fresh: bool) -> None:
    """Build and start everything. fresh=True starts from empty data."""
    if fresh:
        compose("down", "-v")
        shutil.rmtree("runtime", ignore_errors=True)
    print("Starting the stack...", flush=True)
    compose("up", "-d", "--wait", "--build", env={"TRAFFIC_RATE": str(traffic_rate)})


async def wait_until_drained(timeout: float) -> float:
    """Wait until every raw line is parsed and every event is in a saved
    session. Returns how long it took."""
    settings = get_settings()
    keys = Keys(settings.stream_prefix)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    started = time.monotonic()
    calm = 0
    try:
        while time.monotonic() - started < timeout:
            waiting = await backlog(redis, keys.raw_logs, PARSERS)
            for shard in range(settings.shard_count):
                waiting += await backlog(redis, keys.events(shard), BUILDERS)
            calm = calm + 1 if waiting == 0 else 0
            if calm >= 3:
                return time.monotonic() - started
            await asyncio.sleep(3)
    finally:
        await redis.aclose()
    sys.exit(f"pipeline did not drain within {timeout:.0f}s")

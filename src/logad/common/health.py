"""
health.py — check that Redis and Postgres are reachable.

Usage:
    python -m logad.common.health     # exit code 0 = all ok, 1 = something is down
"""

import asyncio

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from logad.common.config import get_settings

TIMEOUT_SECONDS = 3


async def check_redis(url: str) -> None:
    client = Redis.from_url(url, socket_connect_timeout=TIMEOUT_SECONDS)
    try:
        await client.ping()
    finally:
        await client.aclose()


async def check_postgres(url: str) -> None:
    engine = create_async_engine(url, connect_args={"timeout": TIMEOUT_SECONDS})
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    finally:
        await engine.dispose()


async def check_all(redis_url: str, database_url: str) -> dict[str, str]:
    """Run every check. Returns {"redis": "ok" | error, "postgres": ...}."""
    checks = {
        "redis": check_redis(redis_url),
        "postgres": check_postgres(database_url),
    }
    results = {}
    for name, check in checks.items():
        try:
            await check
            results[name] = "ok"
        except Exception as error:
            results[name] = f"{type(error).__name__}: {error}"
    return results


def main() -> int:
    settings = get_settings()
    results = asyncio.run(check_all(settings.redis_url, settings.database_url))
    for name, status in results.items():
        print(f"{name:<9} {status}")
    return 0 if all(status == "ok" for status in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())

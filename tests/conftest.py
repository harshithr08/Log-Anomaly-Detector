import asyncio
import random
import uuid
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import create_async_engine

from logad.common.config import Settings, get_settings
from shop import gateway, inventory, payment, shipping
from shop.settings import SERVICES, ShopSettings
from shop.traffic import REQUEST_MIX

ALEMBIC_INI = Path(__file__).parents[1] / "alembic.ini"


# ── Redis and Postgres ───────────────────────────────────────────────────────


async def _delete_keys(redis_url: str, prefix: str) -> None:
    redis = Redis.from_url(redis_url)
    keys = [key async for key in redis.scan_iter(f"{prefix}:*")]
    if keys:
        await redis.delete(*keys)
    await redis.aclose()


@pytest.fixture
def settings():
    """Settings with a Redis key prefix unique to this test; its keys are deleted afterwards."""
    test_settings = Settings(_env_file=None, stream_prefix=f"test-{uuid.uuid4().hex[:8]}")
    yield test_settings
    asyncio.run(_delete_keys(test_settings.redis_url, test_settings.stream_prefix))


async def _run_admin(url: str, sql: str) -> None:
    engine = create_async_engine(url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as conn:
        await conn.execute(text(sql))
    await engine.dispose()


@pytest.fixture
def database_url():
    """A fresh, fully migrated database for one test, dropped afterwards."""
    base_url = get_settings().database_url
    name = f"logad_test_{uuid.uuid4().hex[:8]}"
    asyncio.run(_run_admin(base_url, f'CREATE DATABASE "{name}"'))
    url = make_url(base_url).set(database=name).render_as_string(hide_password=False)

    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", url)
    command.upgrade(config, "head")
    try:
        yield url
    finally:
        asyncio.run(_run_admin(base_url, f'DROP DATABASE "{name}" WITH (FORCE)'))


# ── in-process shop ──────────────────────────────────────────────────────────


class InProcessRouter(httpx.AsyncBaseTransport):
    """Send each request to the app whose name matches the URL's host."""

    def __init__(self):
        self.apps = {}

    async def handle_async_request(self, request):
        transport = httpx.ASGITransport(app=self.apps[request.url.host])
        return await transport.handle_async_request(request)


@pytest.fixture
def shop(tmp_path):
    """All four shop services running in-process: calls between them are routed
    straight to the right app, so no network or Docker is needed."""
    shop_settings = ShopSettings(
        _env_file=None,
        shop_url="http://shop",
        inventory_url="http://inventory",
        payment_url="http://payment",
        shipping_url="http://shipping",
        log_dir=tmp_path / "logs",
        ground_truth_dir=tmp_path / "ground_truth",
    )
    router = InProcessRouter()
    modules = {"shop": gateway, "inventory": inventory, "payment": payment, "shipping": shipping}
    for name, module in modules.items():
        router.apps[name] = module.create_app(
            settings=shop_settings, transport=router, rng=random.Random(7)
        )

    class Shop:
        apps = router.apps
        settings = shop_settings

        def __init__(self, client):
            self.client = client

        def log_lines(self, service: str) -> list[str]:
            path = shop_settings.log_dir / f"{service}.log"
            return path.read_text(encoding="utf-8").splitlines() if path.exists() else []

        def all_log_lines(self) -> list[str]:
            return [line for service in SERVICES for line in self.log_lines(service)]

        def lines_for(self, service: str, request_id: str) -> list[str]:
            return [line for line in self.log_lines(service) if f"[req={request_id}]" in line]

        def admin(self, service: str) -> TestClient:
            return TestClient(router.apps[service])

        def send_traffic(self, requests: int, seed: int = 1) -> list[str]:
            """Send a realistic mix of requests; returns their request IDs."""
            rng = random.Random(seed)
            makers, weights = zip(*REQUEST_MIX, strict=True)
            request_ids = []
            for _ in range(requests):
                method, path, kwargs = rng.choices(makers, weights)[0](rng)
                response = self.client.request(method, path, **kwargs)
                request_ids.append(response.headers["x-request-id"])
            return request_ids

    with TestClient(router.apps["shop"]) as client:
        yield Shop(client)

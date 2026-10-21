"""Run all four shop services in-process: HTTP calls between them are routed
straight to the right app, so no network or Docker is needed."""

import random

import httpx
import pytest
from fastapi.testclient import TestClient

from shop import gateway, inventory, payment, shipping
from shop.settings import ShopSettings


class InProcessRouter(httpx.AsyncBaseTransport):
    """Send each request to the app whose name matches the URL's host."""

    def __init__(self):
        self.apps = {}

    async def handle_async_request(self, request):
        transport = httpx.ASGITransport(app=self.apps[request.url.host])
        return await transport.handle_async_request(request)


@pytest.fixture
def shop(tmp_path):
    """A running in-process shop. Returns an object with .client, .apps, .settings."""
    settings = ShopSettings(
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
            settings=settings, transport=router, rng=random.Random(7)
        )

    class Shop:
        apps = router.apps

        def __init__(self, client):
            self.client = client
            self.settings = settings

        def log_lines(self, service: str) -> list[str]:
            return (settings.log_dir / f"{service}.log").read_text(encoding="utf-8").splitlines()

        def lines_for(self, service: str, request_id: str) -> list[str]:
            return [line for line in self.log_lines(service) if f"[req={request_id}]" in line]

        def admin(self, service: str) -> TestClient:
            return TestClient(router.apps[service])

    with TestClient(router.apps["shop"]) as client:
        yield Shop(client)

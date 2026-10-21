import asyncio
from datetime import UTC, datetime

import pytest
from conftest import InProcessRouter
from fastapi.testclient import TestClient

from logad.api.app import create_app
from logad.common.db import Alert, Score, Session, Template, make_engine

pytestmark = pytest.mark.integration
NOW = datetime(2026, 10, 21, 10, 0, tzinfo=UTC)
WHY = {"lstm": {"after": "a", "expected": "b", "got": "payment INFO c", "probability": 0.001}}


async def _seed(database_url):
    engine = make_engine(database_url)
    async with engine.begin() as conn:
        await conn.execute(Template.__table__.insert().values(id=1, template="shop INFO Handling"))
        await conn.execute(
            Session.__table__.insert().values(
                session_id="req1",
                started_at=NOW,
                ended_at=NOW,
                closed_by="end",
                event_count=1,
                services=["shop"],
                events=[
                    {
                        "ts": NOW.isoformat(),
                        "service": "shop",
                        "level": "INFO",
                        "template_id": 1,
                        "message": "Handling GET /",
                        "params": {},
                    }
                ],
            )
        )
        await conn.execute(
            Score.__table__.insert().values(
                session_id="req1",
                model_version="v1",
                lstm_score=9.0,
                timing_score=0.0,
                anomalous=True,
                detectors=["lstm"],
                explanation=WHY,
            )
        )
        for i, service in enumerate(["payment", "shop", "inventory"]):
            await conn.execute(
                Alert.__table__.insert().values(
                    service=service,
                    status="resolved" if i else "open",
                    opened_at=NOW,
                    last_seen_at=NOW,
                    sessions=5,
                    detectors=["lstm"],
                    summary=f"{service}: x",
                    examples=[{"session_id": "req1", "detector": "lstm", "explanation": WHY}],
                )
            )
    await engine.dispose()


@pytest.fixture
def client(settings, database_url, shop):
    asyncio.run(_seed(database_url))
    router = InProcessRouter()
    router.apps = shop.apps
    app = create_app(
        settings.model_copy(update={"database_url": database_url}), shop.settings, router
    )
    with TestClient(app) as client:
        yield client


def test_alerts_are_paginated_newest_first(client):
    first = client.get("/api/alerts", params={"limit": 2}).json()
    assert [a["service"] for a in first["items"]] == ["inventory", "shop"]
    second = client.get(
        "/api/alerts", params={"limit": 2, "before_id": first["next_before_id"]}
    ).json()
    assert [a["service"] for a in second["items"]] == ["payment"]
    assert second["next_before_id"] is None
    assert [a["service"] for a in client.get("/api/alerts?status=open").json()["items"]] == [
        "payment"
    ]


def test_alert_detail_has_the_example_requests_in_full(client):
    alert_id = client.get("/api/alerts?status=open").json()["items"][0]["id"]
    detail = client.get(f"/api/alerts/{alert_id}").json()
    [example] = detail["examples"]
    assert example["explanation"] == WHY
    assert example["session"]["events"][0]["template"] == "shop INFO Handling"
    assert example["session"]["scores"][0]["anomalous"] is True
    assert client.get("/api/alerts/999").status_code == 404
    assert client.get("/api/sessions/nope").status_code == 404


def test_break_and_fix_buttons_reach_the_shop(client, shop):
    response = client.post("/api/faults/payment", json={"kind": "slow", "delay_ms": 800})
    assert response.json()["fault"]["kind"] == "slow"
    assert shop.admin("payment").get("/admin/fault").json()["fault"]["delay_ms"] == 800
    assert client.get("/api/faults").json()["payment"]["kind"] == "slow"
    client.delete("/api/faults/payment")
    assert shop.admin("payment").get("/admin/fault").json()["fault"] is None
    assert client.post("/api/faults/nope", json={"kind": "slow"}).status_code == 404


def test_dashboard_is_served(client):
    page = client.get("/")
    assert page.status_code == 200 and "<title>Log Anomaly Detector</title>" in page.text


def test_websocket_works_through_a_real_uvicorn_server(settings, database_url):
    """TestClient bypasses uvicorn, which needs a WebSocket library of its own:
    this test caught the dashboard's live feed silently not connecting."""
    import json
    import socket as sockets
    import threading
    import time

    import uvicorn
    from websockets.sync.client import connect

    asyncio.run(_seed(database_url))
    with sockets.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    app = create_app(settings.model_copy(update={"database_url": database_url}))
    server = uvicorn.Server(uvicorn.Config(app, port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        for _ in range(100):
            if server.started:
                break
            time.sleep(0.05)
        with connect(f"ws://127.0.0.1:{port}/ws", open_timeout=5) as ws:
            message = json.loads(ws.recv(timeout=5))
        assert message["stats"]["open_alerts"] == 1
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def test_websocket_pushes_stats_and_open_alerts(client):
    with client.websocket_connect("/ws") as socket:
        message = socket.receive_json()
    assert {"lines_per_s", "sessions_per_s", "flagged_per_s", "backlog"} <= set(message["stats"])
    assert [a["service"] for a in message["open_alerts"]] == ["payment"]
    assert message["stats"]["open_alerts"] == 1

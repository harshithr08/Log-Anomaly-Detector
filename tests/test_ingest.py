import pytest
from fastapi.testclient import TestClient
from redis import Redis

from logad.common.streams import Keys
from logad.ingest.app import MAX_BATCH_LINES, create_app

pytestmark = pytest.mark.integration


def test_lines_are_queued_in_order(settings):
    batch = [{"source": "shop", "line": f"line {i}"} for i in range(3)]
    with TestClient(create_app(settings)) as client:
        response = client.post("/ingest", json={"lines": batch})
    assert response.json() == {"accepted": 3}

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    entries = redis.xrange(Keys(settings.stream_prefix).raw_logs)
    redis.close()
    assert [fields["line"] for _, fields in entries] == ["line 0", "line 1", "line 2"]
    assert all(fields["source"] == "shop" and fields["received_at"] for _, fields in entries)


def test_oversized_batch_is_rejected(settings):
    batch = [{"source": "shop", "line": "x"}] * (MAX_BATCH_LINES + 1)
    with TestClient(create_app(settings)) as client:
        assert client.post("/ingest", json={"lines": batch}).status_code == 422


def test_health(settings):
    with TestClient(create_app(settings)) as client:
        assert client.get("/health").json() == {"status": "ok"}

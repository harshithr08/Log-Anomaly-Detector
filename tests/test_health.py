import pytest

from logad.common.config import get_settings
from logad.common.health import check_all

# Nothing listens on port 1, so connections are refused straight away.
DEAD_REDIS = "redis://127.0.0.1:1/0"
DEAD_POSTGRES = "postgresql+asyncpg://logad:logad@127.0.0.1:1/logad"


async def test_reports_unreachable_services():
    results = await check_all(DEAD_REDIS, DEAD_POSTGRES)
    assert results["redis"] != "ok"
    assert results["postgres"] != "ok"


@pytest.mark.integration
async def test_services_are_reachable():
    settings = get_settings()
    results = await check_all(settings.redis_url, settings.database_url)
    assert results == {"redis": "ok", "postgres": "ok"}

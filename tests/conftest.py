import asyncio
import uuid
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import create_async_engine

from logad.common.config import get_settings

ALEMBIC_INI = Path(__file__).parents[1] / "alembic.ini"


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

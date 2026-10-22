import asyncio

from logad.common.logs import setup_logging
from logad.sessions.builder import run

setup_logging()
asyncio.run(run())

import asyncio

from logad.common.logs import setup_logging
from logad.retention.prune import run

setup_logging()
asyncio.run(run())

import asyncio

from logad.common.logs import setup_logging
from logad.parser.worker import run

setup_logging()
asyncio.run(run())

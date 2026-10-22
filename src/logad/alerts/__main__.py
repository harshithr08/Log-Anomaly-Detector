import asyncio

from logad.alerts.manager import run
from logad.common.logs import setup_logging

setup_logging()
asyncio.run(run())

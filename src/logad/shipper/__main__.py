import asyncio

from logad.common.logs import setup_logging
from logad.shipper.ship import run

setup_logging()
asyncio.run(run())

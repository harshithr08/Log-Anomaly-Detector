import asyncio
import logging

from logad.shipper.ship import run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
asyncio.run(run())

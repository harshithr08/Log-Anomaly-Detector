"""
shutdown.py — stop cleanly on `docker stop` (SIGTERM) or Ctrl+C (SIGINT).

Workers check the event between batches, so they never stop halfway through
one. Unfinished work stays pending in Redis and is picked up after a restart.
"""

import asyncio
import signal

import redis.exceptions
import sqlalchemy.exc


def stop_event() -> asyncio.Event:
    """An event that is set when the process is asked to stop."""
    event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, event.set)
        except NotImplementedError:  # Windows: Ctrl+C still raises KeyboardInterrupt
            pass
    return event


# Errors worth waiting out (Redis or Postgres briefly unreachable) instead of crashing.
TRANSIENT_ERRORS = (
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    sqlalchemy.exc.OperationalError,
    sqlalchemy.exc.InterfaceError,
    OSError,  # includes ConnectionRefusedError, TimeoutError
)

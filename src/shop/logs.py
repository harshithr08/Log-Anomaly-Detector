"""
logs.py — plain-text logs, the way a typical app writes them:

    2026-10-21T10:00:00.123Z INFO payment [req=7f3a91c2d4e5b6a7] Running fraud check score=0.12

Every line carries the request ID of the request being handled, so all lines of
one customer request can be grouped later, across all services. Lines written
outside a request (startup, background jobs) get "req=-".
"""

import logging
import sys
import time
from contextvars import ContextVar
from logging.handlers import RotatingFileHandler
from pathlib import Path

# The request being handled right now. A ContextVar keeps concurrent requests apart.
request_id_var: ContextVar[str] = ContextVar("request_id", default="-")

LOG_FORMAT = "%(asctime)s %(levelname)s %(service)s [req=%(request_id)s] %(message)s"
MAX_FILE_BYTES = 50 * 1024 * 1024


class _UTCFormatter(logging.Formatter):
    converter = time.gmtime

    def formatTime(self, record, datefmt=None):  # noqa: N802 (logging's name)
        stamp = time.strftime("%Y-%m-%dT%H:%M:%S", self.converter(record.created))
        return f"{stamp}.{int(record.msecs):03d}Z"


class _ContextFilter(logging.Filter):
    """Adds the service name and current request ID to every record."""

    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def filter(self, record):
        record.service = self.service
        record.request_id = request_id_var.get()
        return True


def setup_logging(service: str, log_dir: Path) -> logging.Logger:
    """Logger that writes to <log_dir>/<service>.log and to stdout."""
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"shop.{service}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    close_logging(logger)

    formatter = _UTCFormatter(LOG_FORMAT)
    handlers = [
        RotatingFileHandler(
            log_dir / f"{service}.log", maxBytes=MAX_FILE_BYTES, backupCount=2, encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]
    for handler in handlers:
        handler.setFormatter(formatter)
        handler.addFilter(_ContextFilter(service))
        logger.addHandler(handler)
    return logger


def close_logging(logger: logging.Logger) -> None:
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)

"""
config.py — settings for every process, read from environment variables.

Each field can be overridden by an environment variable named LOGAD_<FIELD>
(e.g. LOGAD_REDIS_URL) or by a line in a .env file. The defaults match
docker-compose.yml, so local development works with no setup.
"""

import os
import socket
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LOGAD_", env_file=".env", extra="ignore")

    redis_url: str = "redis://127.0.0.1:6379/0"
    database_url: str = "postgresql+asyncpg://logad:logad@127.0.0.1:5433/logad"
    stream_prefix: str = "logad"  # every Redis key starts with this
    shard_count: int = 4  # events streams; a request's events always go to one shard
    # This process's name inside a Redis consumer group.
    consumer_name: str = Field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")

    # parser
    parser_batch_size: int = 200

    # session builder
    builder_index: int = 0  # this builder owns shards where shard % builder_count == index
    builder_count: int = 1
    builder_batch_size: int = 500
    session_grace_s: float = 2.0  # after the last line, wait this long for late lines
    session_timeout_s: float = 30.0  # close a session that went quiet without a last line
    session_end_service: str = "shop"  # the last line of a request is this service's
    session_end_prefix: str = "Completed "  # ... line starting with this

    # shipper
    ingest_url: str = "http://127.0.0.1:8100"
    shipper_log_dir: Path = Path("runtime/logs")
    shipper_state_file: Path = Path("runtime/shipper/offsets.json")
    shipper_poll_s: float = 0.2
    shipper_batch_lines: int = 500


@lru_cache
def get_settings() -> Settings:
    """The settings, loaded once per process."""
    return Settings()

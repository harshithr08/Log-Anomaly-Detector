"""
config.py — settings for every process, read from environment variables.

Each field can be overridden by an environment variable named LOGAD_<FIELD>
(e.g. LOGAD_REDIS_URL) or by a line in a .env file. The defaults match
docker-compose.yml, so local development works with no setup.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LOGAD_", env_file=".env", extra="ignore")

    redis_url: str = "redis://127.0.0.1:6379/0"
    database_url: str = "postgresql+asyncpg://logad:logad@127.0.0.1:5433/logad"
    stream_prefix: str = "logad"  # every Redis key starts with this

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

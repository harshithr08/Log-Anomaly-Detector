"""
config.py — settings for every process, read from environment variables.

Each field can be overridden by an environment variable named LOGAD_<FIELD>
(e.g. LOGAD_REDIS_URL) or by a line in a .env file. The defaults match
docker-compose.yml, so local development works with no setup.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LOGAD_", env_file=".env", extra="ignore")

    redis_url: str = "redis://127.0.0.1:6379/0"
    database_url: str = "postgresql+asyncpg://logad:logad@127.0.0.1:5433/logad"


@lru_cache
def get_settings() -> Settings:
    """The settings, loaded once per process."""
    return Settings()

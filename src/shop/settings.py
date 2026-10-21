"""
settings.py — shop settings, read from SHOP_* environment variables.

Defaults are for running on the host: each service on its own port.
docker-compose.yml overrides the URLs with container names.
"""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

SERVICES = ("shop", "inventory", "payment", "shipping")


class ShopSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SHOP_", env_file=".env", extra="ignore")

    shop_url: str = "http://127.0.0.1:8000"
    inventory_url: str = "http://127.0.0.1:8001"
    payment_url: str = "http://127.0.0.1:8002"
    shipping_url: str = "http://127.0.0.1:8003"

    log_dir: Path = Path("runtime/logs")
    ground_truth_dir: Path = Path("runtime/ground_truth")
    downstream_timeout_s: float = 5.0

    def url_for(self, service: str) -> str:
        return getattr(self, f"{service}_url")

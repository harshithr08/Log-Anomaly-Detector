"""
streams.py — names of every Redis key the pipeline uses, in one place.

All keys start with a prefix ("logad" by default), so tests can use their own
prefix and never touch real data.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Keys:
    prefix: str = "logad"

    @property
    def raw_logs(self) -> str:
        """Stream: raw lines from the ingest API."""
        return f"{self.prefix}:raw-logs"

    def events(self, shard: int) -> str:
        """Stream: parsed events, one stream per shard (shard = hash of the request ID)."""
        return f"{self.prefix}:events:{shard}"

    @property
    def template_ids(self) -> str:
        """Hash: template text -> global template ID."""
        return f"{self.prefix}:templates:ids"

    @property
    def template_texts(self) -> str:
        """Hash: global template ID -> template text."""
        return f"{self.prefix}:templates:texts"

    @property
    def template_counter(self) -> str:
        """Counter used to hand out new template IDs."""
        return f"{self.prefix}:templates:next-id"

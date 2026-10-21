"""
parse.py — split a raw line into its parts and find its template.

    2026-10-21T02:50:38.384Z INFO payment [req=90d78a6e] Charging order=o7f2 amount=42.10
    └── timestamp ─────────┘ └lvl┘ └service┘ └ request ┘ └────────── message ──────────┘

The template is the message with its variable parts replaced by <*>:
    "payment INFO Charging order=<*> amount=<*>"
Variables are masked first with simple rules (key=value pairs, IDs, numbers),
then Drain3 merges lines that are still nearly identical. The variable values
are kept as `params`, e.g. {"order": "o7f2", "amount": 42.1}.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

HEADER = re.compile(
    r"^(?P<ts>\S+) (?P<level>[A-Z]+) (?P<service>[\w-]+) \[req=(?P<request_id>[^\]]+)\] "
    r"(?P<message>.*)$"
)
KEY_VALUE = re.compile(r"\b(\w+)=(\S+)")
PATH_ID = re.compile(r"/(?=[^/\s]*\d)[^/\s]+")  # path segments with a digit: /products/p03
HEX_ID = re.compile(r"\b[0-9a-f]{8,}\b")
NUMBER = re.compile(r"\b\d+(?:\.\d+)?\b")
NO_REQUEST = "-"

logging.getLogger("drain3").setLevel(logging.WARNING)


@dataclass
class ParsedLine:
    ts: datetime
    level: str
    service: str
    request_id: str  # "-" for lines written outside a request
    message: str
    params: dict = field(default_factory=dict)


def _value(text: str):
    text = text.rstrip(",;")
    for convert in (int, float):
        try:
            return convert(text)
        except ValueError:
            pass
    return text


def parse_line(line: str) -> ParsedLine | None:
    """None if the line doesn't have the expected header."""
    match = HEADER.match(line)
    if not match:
        return None
    try:
        ts = datetime.fromisoformat(match["ts"].replace("Z", "+00:00"))
    except ValueError:
        return None
    message = match["message"]
    params = {key: _value(value) for key, value in KEY_VALUE.findall(message)}
    return ParsedLine(ts, match["level"], match["service"], match["request_id"], message, params)


def mask(message: str) -> str:
    """Replace the obviously variable parts of a message with <*>."""
    message = KEY_VALUE.sub(r"\1=<*>", message)
    message = PATH_ID.sub("/<*>", message)
    message = HEX_ID.sub("<*>", message)
    return NUMBER.sub("<*>", message)


class TemplateFinder:
    """Drain3 on top of the masking rules. Each worker has its own; the shared
    registry turns template *text* into an ID, so workers still agree."""

    def __init__(self, similarity: float = 0.9):
        config = TemplateMinerConfig()
        config.drain_sim_th = similarity
        config.drain_depth = 4
        config.profiling_enabled = False
        self.miner = TemplateMiner(config=config)

    def template(self, parsed: ParsedLine) -> str:
        text = f"{parsed.service} {parsed.level} {mask(parsed.message)}"
        return self.miner.add_log_message(text)["template_mined"]

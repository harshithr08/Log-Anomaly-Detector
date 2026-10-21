"""
dataset.py — a labelled dataset of shop sessions.

export: sessions from Postgres + the fault injector's ground truth
        -> data/shop/sessions.jsonl.gz, one session per line.
        Template IDs are replaced by template *text*: IDs restart whenever the
        database is reset, the text doesn't, so models stay valid.
load:   -> Sessions, split by time (see `split`).

Labels come only from the fault injector: a session is anomalous if one of the
services recorded breaking that request. Models never see labels in training.

Usage:
    python -m logad.models.dataset export     # after eval/collect.py
"""

import asyncio
import gzip
import json
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import text

from logad.common.config import get_settings
from logad.common.db import make_engine

DATA_DIR = Path("data/shop")
GROUND_TRUTH_DIR = Path("runtime/ground_truth")
COLLECTION = Path("runtime/collection.json")
TRAIN_SHARE = 0.7  # of the clean period; the rest is validation


@dataclass
class Session:
    session_id: str
    started_at: float  # seconds since 1970
    ended_at: float
    closed_by: str
    events: list[dict]  # {"ts", "service", "level", "template", "message", "params"}
    label: int = 0  # 1 = the fault injector broke this request
    fault: str | None = None  # e.g. "payment skip"

    @property
    def templates(self) -> list[str]:
        return [event["template"] for event in self.events]

    @property
    def route(self) -> str:
        """The request type, e.g. 'shop INFO Handling POST /checkout'."""
        return self.events[0]["template"]


@dataclass
class Splits:
    train: list[Session]  # clean traffic: what normal looks like
    val: list[Session]  # later clean traffic: to set thresholds
    test: list[Session]  # traffic with scheduled faults: to grade the detectors
    info: dict = field(default_factory=dict)


def _ground_truth(directory: Path) -> dict[str, str]:
    """request ID -> "service kind" for every request the fault injector broke."""
    broken = {}
    for path in sorted(directory.glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            event = json.loads(line)
            if event["event"] == "affected":
                broken.setdefault(event["request_id"], f"{event['service']} {event['kind']}")
    return broken


async def _read_sessions(database_url: str) -> tuple[list, dict[int, str]]:
    engine = make_engine(database_url)
    async with engine.connect() as conn:
        templates = dict((await conn.execute(text("select id, template from templates"))).all())
        rows = (
            await conn.execute(
                text(
                    "select session_id, started_at, ended_at, closed_by, events "
                    "from sessions order by started_at"
                )
            )
        ).all()
    await engine.dispose()
    return rows, templates


def export(
    out_dir: Path = DATA_DIR,
    ground_truth_dir: Path = GROUND_TRUTH_DIR,
    collection: Path = COLLECTION,
    database_url: str | None = None,
) -> Path:
    rows, templates = asyncio.run(_read_sessions(database_url or get_settings().database_url))
    broken = _ground_truth(ground_truth_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "sessions.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in rows:
            record = {
                "session_id": row.session_id,
                "started_at": row.started_at.timestamp(),
                "ended_at": row.ended_at.timestamp(),
                "closed_by": row.closed_by,
                "events": [
                    {
                        "ts": event["ts"],
                        "service": event["service"],
                        "level": event["level"],
                        "template": templates[event["template_id"]],
                        "message": event["message"],
                        "params": event["params"],
                    }
                    for event in row.events
                ],
                "label": int(row.session_id in broken),
                "fault": broken.get(row.session_id),
            }
            f.write(json.dumps(record) + "\n")
    shutil.copy(collection, out_dir / "collection.json")
    print(f"Exported {len(rows):,} sessions ({len(broken):,} broken requests) to {path}")
    return path


def load(data_dir: Path = DATA_DIR) -> list[Session]:
    with gzip.open(data_dir / "sessions.jsonl.gz", "rt", encoding="utf-8") as f:
        return [Session(**json.loads(line)) for line in f]


def split(sessions: list[Session], collection: dict) -> Splits:
    """By time, never at random:
    train = first 70% of the clean period, val = the rest of the clean period,
    test = the fault period (normal and broken requests mixed, as in real life)."""
    clean_start, clean_end = collection["clean"]
    clean = [s for s in sessions if clean_start <= s.started_at < clean_end]
    test = [s for s in sessions if s.started_at >= clean_end]
    assert not any(s.label for s in clean), "the clean period must contain no faults"
    cut = int(len(clean) * TRAIN_SHARE)
    return Splits(
        train=clean[:cut],
        val=clean[cut:],
        test=test,
        info={"clean": len(clean), "test_broken": sum(s.label for s in test)},
    )


def load_splits(data_dir: Path = DATA_DIR) -> Splits:
    collection = json.loads((data_dir / "collection.json").read_text(encoding="utf-8"))
    return split(load(data_dir), collection)


if __name__ == "__main__":
    if sys.argv[1:] != ["export"]:
        sys.exit("usage: python -m logad.models.dataset export")
    export()

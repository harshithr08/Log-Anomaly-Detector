import asyncio
import json
from datetime import UTC, datetime

import pytest

from logad.common.db import Session, Template, make_engine
from logad.models.dataset import export, load

pytestmark = pytest.mark.integration


async def _insert(database_url):
    now = datetime(2026, 10, 21, 10, 0, tzinfo=UTC)
    engine = make_engine(database_url)
    async with engine.begin() as conn:
        await conn.execute(
            Template.__table__.insert(),
            [
                {"id": 1, "template": "shop INFO Handling GET /x"},
                {"id": 2, "template": "shop INFO Done"},
            ],
        )
        for session_id in ("ok", "broken"):
            events = [
                {
                    "ts": now.isoformat(),
                    "service": "shop",
                    "level": "INFO",
                    "template_id": tid,
                    "message": "m",
                    "params": {},
                    "line_id": f"{session_id}{tid}",
                }
                for tid in (1, 2)
            ]
            await conn.execute(
                Session.__table__.insert().values(
                    session_id=session_id,
                    started_at=now,
                    ended_at=now,
                    closed_by="end",
                    event_count=2,
                    services=["shop"],
                    events=events,
                )
            )
    await engine.dispose()


def test_export_uses_template_text_and_injector_labels(tmp_path, database_url):
    asyncio.run(_insert(database_url))
    truth = tmp_path / "ground_truth"
    truth.mkdir()
    (truth / "shop.jsonl").write_text(
        json.dumps({"event": "affected", "service": "shop", "kind": "skip", "request_id": "broken"})
        + "\n"
    )
    collection = tmp_path / "collection.json"
    collection.write_text(json.dumps({"clean": [0, 1]}))

    export(tmp_path / "out", truth, collection, database_url)
    sessions = {s.session_id: s for s in load(tmp_path / "out")}
    assert sessions["ok"].templates == ["shop INFO Handling GET /x", "shop INFO Done"]
    assert sessions["ok"].route == "shop INFO Handling GET /x"
    assert (sessions["ok"].label, sessions["ok"].fault) == (0, None)
    assert (sessions["broken"].label, sessions["broken"].fault) == (1, "shop skip")

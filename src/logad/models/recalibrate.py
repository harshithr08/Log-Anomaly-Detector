"""
recalibrate.py — reset the thresholds on recent normal traffic (drift).

Normal behaviour shifts over time (the HDFS benchmark showed it: a threshold
from last week flags too much this week). This re-scores the last few hours
of normal traffic with the current model and sets each threshold again to the
same budget: at most 1 in 1,000 normal requests flagged. Sessions during an
alert (plus a minute around it) and sessions that never finished are left out.

The model itself is unchanged; the result is saved as a new version (a copy
with new thresholds) and models/latest points to it, so the scoring workers
switch to it within 30 seconds.

Usage:
    python -m logad.models.recalibrate              # last 2 hours
    python -m logad.models.recalibrate --hours 6
"""

import argparse
import asyncio
import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
from sqlalchemy import text

from logad.common.config import get_settings
from logad.common.db import make_engine
from logad.models.dataset import Session, session_from_row
from logad.models.metrics import FALSE_ALARM_BUDGET, threshold_for
from logad.models.serving import ServingModels

MIN_SESSIONS = 2000  # fewer than this: too little to set a 1-in-1,000 threshold


def new_thresholds(models: ServingModels, sessions: list[Session]) -> dict:
    verdicts = models.score(sessions)
    return {
        "false_alarm_budget": FALSE_ALARM_BUDGET,
        "lstm": threshold_for(np.array([v.lstm_score for v in verdicts])),
        "timing": threshold_for(np.array([v.timing_score for v in verdicts])),
    }


def save_version(models_dir: Path, models: ServingModels, thresholds: dict, note: dict) -> str:
    """A copy of the current version with new thresholds; models/latest -> it."""
    version = datetime.now(UTC).strftime("v%Y%m%d-%H%M%S") + "-recal"
    source, target = models_dir / models.version, models_dir / version
    shutil.copytree(source, target)
    (target / "thresholds.json").write_text(json.dumps(thresholds, indent=2), encoding="utf-8")
    meta = json.loads((source / "meta.json").read_text(encoding="utf-8"))
    meta.update(version=version, recalibrated=note)
    (target / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    (models_dir / "latest").write_text(version, encoding="utf-8")
    return version


async def recent_normal_sessions(database_url: str, hours: float) -> list[Session]:
    since = datetime.now(UTC) - timedelta(hours=hours)
    engine = make_engine(database_url)
    async with engine.connect() as conn:
        templates = dict((await conn.execute(text("select id, template from templates"))).all())
        rows = await conn.execute(
            text(
                "select s.* from sessions s where s.started_at >= :since and s.closed_by = 'end'"
                " and not exists (select 1 from alerts a where s.started_at between"
                "   a.opened_at - interval '60 seconds'"
                "   and coalesce(a.resolved_at, now()) + interval '60 seconds')"
            ),
            {"since": since},
        )
        sessions = [session_from_row(row, templates) for row in rows]
    await engine.dispose()
    return sessions


def main():
    parser = argparse.ArgumentParser(description="Reset thresholds on recent normal traffic.")
    parser.add_argument("--hours", type=float, default=2)
    args = parser.parse_args()
    settings = get_settings()
    models = ServingModels(settings.models_dir)
    sessions = asyncio.run(recent_normal_sessions(settings.database_url, args.hours))
    if len(sessions) < MIN_SESSIONS:
        raise SystemExit(
            f"Only {len(sessions)} normal sessions in {args.hours} h; need {MIN_SESSIONS}."
        )
    thresholds = new_thresholds(models, sessions)
    note = {
        "from": models.version,
        "sessions": len(sessions),
        "hours": args.hours,
        "old": models.thresholds,
        "at": datetime.now(UTC).isoformat(),
    }
    version = save_version(settings.models_dir, models, thresholds, note)
    print(f"{models.version} -> {version} on {len(sessions):,} normal sessions")
    for name in ("lstm", "timing"):
        print(f"  {name:<7} {models.thresholds[name]:.3f} -> {thresholds[name]:.3f}")


if __name__ == "__main__":
    main()

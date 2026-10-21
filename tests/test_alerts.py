import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from logad.alerts.grouping import Evidence, OpenAlert, decide, evidence_from_score
from logad.alerts.manager import AlertManager
from logad.common.db import Alert, RateAnomalyRow, Score, Template, make_engine

SKIP = {
    "lstm": {
        "after": "payment INFO Charging order=<*>",
        "expected": "payment INFO Fraud check passed order=<*>",
        "got": "payment INFO Payment authorized order=<*>",
        "probability": 0.0003,
    }
}
SLOW = {"timing": {"slowest": "shop", "spreads_slower": 14.0, "whole_request_spreads": 9.0}}


def flagged(n, at, explanation=SKIP, prefix="s"):
    return [e for i in range(n) for e in evidence_from_score(f"{prefix}{i}", at + i, explanation)]


def rate(at, service="shipping"):
    template = f"{service} ERROR Carrier API rate limit exceeded"
    return Evidence(
        service,
        at,
        "rate",
        None,
        {"template": template, "count": 18, "expected": 0.0, "z": 18.0, "direction": "spike"},
    )


def test_a_few_stray_flags_are_not_an_alert():
    assert decide(flagged(4, at=1000), {}, now=1010) == []


def test_enough_flags_in_a_minute_open_an_explained_alert():
    [alert] = decide(flagged(6, at=1000), {}, now=1010)
    assert (alert.service, alert.sessions, alert.detectors) == ("payment", 6, ["lstm"])
    assert "after 'Charging order=<*>' expected 'Fraud check passed order=<*>'" in alert.summary
    assert len(alert.examples) == 5 and not alert.resolve


def test_a_rate_anomaly_alone_opens_an_alert():
    [alert] = decide([rate(1000)], {}, now=1005)
    assert alert.service == "shipping" and "spike of 'Carrier API rate limit" in alert.summary


def test_services_get_separate_alerts():
    evidence = flagged(5, at=1000) + flagged(5, at=1000, explanation=SLOW, prefix="t")
    alerts = {a.service: a for a in decide(evidence, {}, now=1010)}
    assert set(alerts) == {"payment", "shop"}
    assert "5 slow requests: shop typically 14 spreads slower" in alerts["shop"].summary


def test_open_alert_resolves_after_a_quiet_minute_and_keeps_its_summary():
    evidence = flagged(6, at=1000)
    open_alerts = {"payment": OpenAlert("payment", 1000, alert_id=7)}
    [still_open] = decide(evidence, open_alerts, now=1030)
    assert not still_open.resolve
    [resolved] = decide(evidence, open_alerts, now=1070)
    assert resolved.resolve and resolved.summary  # last evidence 1005, quiet since 65s
    [aged_out] = decide([], open_alerts, now=2000)
    assert aged_out.resolve and aged_out.summary is None  # keep the saved summary


async def _seed(database_url, at: datetime, burst: str):
    engine = make_engine(database_url)
    async with engine.begin() as conn:
        if burst == "first":
            await conn.execute(
                Template.__table__.insert().values(
                    id=1, template="shipping ERROR Label printer down"
                )
            )
        await conn.execute(
            Score.__table__.insert(),
            [
                {
                    "session_id": f"{burst}{i}",
                    "model_version": "v1",
                    "lstm_score": 9.0,
                    "timing_score": 0.0,
                    "anomalous": True,
                    "detectors": ["lstm"],
                    "explanation": SKIP,
                    "scored_at": at,
                }
                for i in range(6)
            ],
        )
        await conn.execute(
            RateAnomalyRow.__table__.insert().values(
                window_start=at,
                template_id=1,
                count=12,
                expected=0.0,
                z=12.0,
                direction="spike",
                detected_at=at,
            )
        )
    await engine.dispose()


@pytest.mark.integration
def test_manager_opens_once_resolves_and_reopens(database_url, settings):
    settings = settings.model_copy(update={"database_url": database_url})
    t0 = datetime.now(UTC).timestamp()
    asyncio.run(_seed(database_url, datetime.fromtimestamp(t0, UTC), "first"))

    async def run_cycles():
        engine = make_engine(database_url)
        manager = AlertManager(engine, settings)
        await manager.cycle(now=t0 + 5)
        await manager.cycle(now=t0 + 6)  # again: must not create duplicates
        async with engine.connect() as conn:
            first = (await conn.execute(select(Alert.__table__))).all()
        await manager.cycle(now=t0 + 120)  # quiet for two minutes
        async with engine.connect() as conn:
            after_quiet = (await conn.execute(select(Alert.__table__))).all()
        await engine.dispose()
        return first, after_quiet

    first, after_quiet = asyncio.run(run_cycles())
    assert sorted((a.service, a.status) for a in first) == [
        ("payment", "open"),
        ("shipping", "open"),
    ]
    payment = next(a for a in first if a.service == "payment")
    assert payment.sessions == 6 and payment.detectors == ["lstm"]
    assert payment.raised_at >= payment.opened_at  # raised once there was enough evidence
    assert all(a.status == "resolved" and a.resolved_at for a in after_quiet)
    assert {a.summary for a in after_quiet} == {a.summary for a in first}  # kept

    # a new burst later opens a NEW alert; the old one stays resolved
    asyncio.run(_seed(database_url, datetime.fromtimestamp(t0 + 300, UTC), "second"))

    async def again():
        engine = make_engine(database_url)
        await AlertManager(engine, settings).cycle(now=t0 + 305)
        async with engine.connect() as conn:
            rows = (await conn.execute(select(Alert.service, Alert.status))).all()
        await engine.dispose()
        return rows

    rows = asyncio.run(again())
    assert sorted(rows) == [
        ("payment", "open"),
        ("payment", "resolved"),
        ("shipping", "open"),
        ("shipping", "resolved"),
    ]

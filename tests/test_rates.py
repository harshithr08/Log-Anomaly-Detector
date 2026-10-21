import json

import pytest
from redis.asyncio import Redis
from sqlalchemy import select

from logad.common.db import RateAnomalyRow, make_engine
from logad.common.groups import trim_finished
from logad.common.streams import Keys
from logad.rates.counter import RateCounter
from logad.rates.worker import GROUP, RateWorker

NORMAL, ERROR = 1, 2


def feed(counter, window, counts):
    """Put `counts` {template: n} into the 10-second window number `window`."""
    for template, n in counts.items():
        for i in range(n):
            ts = window * 10 + i * 0.01
            counter.add(ts, template, f"{window}-{template}-{i}", ("s", "x"))


def run(counter, windows):
    """Feed windows one after another; return the anomalies of each."""
    found = []
    for window, counts in enumerate(windows):
        feed(counter, window, counts)
        found.append([a for _, anomalies in counter.close_ready() for a in anomalies])
    return found


def test_an_error_flood_is_a_spike():
    counter = RateCounter(warmup_windows=3)
    quiet = [{NORMAL: 50}] * 10
    found = run(counter, [*quiet, {NORMAL: 50, ERROR: 40}, {NORMAL: 50}, {NORMAL: 50}])
    # a window closes once event time is 5s (lateness) past its end: here that
    # happens when the window two later begins
    assert not any(found[:12])
    [spike] = found[12]
    assert (spike.template_id, spike.direction, spike.count) == (ERROR, "spike", 40)


def test_an_incident_stays_flagged_while_it_lasts_but_not_forever():
    counter = RateCounter(warmup_windows=3)
    flood = {NORMAL: 50, ERROR: 30}
    found = run(counter, [{NORMAL: 50}] * 10 + [flood] * 6 + [{NORMAL: 50}] * 2)
    flagged = [any(a.template_id == ERROR for a in f) for f in found]
    assert flagged[12:18] == [True] * 6  # windows 10-15, each judged two windows later

    counter = RateCounter(warmup_windows=3)
    found = run(counter, [{NORMAL: 50}] * 10 + [flood] * 200)  # the new normal
    assert not any(a.template_id == ERROR for a in found[-1])


def test_traffic_falling_off_is_a_drop():
    counter = RateCounter(warmup_windows=3)
    found = run(counter, [{NORMAL: 100}] * 10 + [{NORMAL: 5}, {NORMAL: 100}, {NORMAL: 100}])
    [drop] = found[12]
    assert (drop.template_id, drop.direction) == (NORMAL, "drop")


def test_nothing_is_reported_while_warming_up():
    counter = RateCounter(warmup_windows=6)
    found = run(counter, [{NORMAL: 50}, {NORMAL: 50, ERROR: 40}, {NORMAL: 50}])
    assert not any(found)


def test_normal_wobble_is_not_an_anomaly():
    counter = RateCounter(warmup_windows=3)
    found = run(counter, [{NORMAL: n} for n in [48, 55, 50, 44, 57, 51, 46, 53] * 4])
    assert not any(found)


def test_windows_close_only_as_event_time_moves():
    """A stalled pipeline (no new events) must not close windows: no fake drops."""
    counter = RateCounter()
    feed(counter, 0, {NORMAL: 10})
    assert counter.close_ready() == []  # time hasn't moved past window 0 + lateness
    counter.add(16.0, NORMAL, "later", ("s", "y"))
    [(window, _)] = counter.close_ready()
    assert window.counts == {NORMAL: 10}


def test_duplicates_and_late_events():
    counter = RateCounter()
    assert counter.add(1.0, NORMAL, "a", ("s", "1")) == "counted"
    assert counter.add(1.0, NORMAL, "a", ("s", "2")) == "duplicate"
    counter.add(20.0, NORMAL, "b", ("s", "3"))
    counter.close_ready()
    assert counter.add(2.0, NORMAL, "c", ("s", "4")) == "late"


@pytest.mark.integration
async def test_trimming_keeps_what_another_group_still_needs(settings):
    keys = Keys(settings.stream_prefix)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    stream = keys.events(0)
    for i in range(10):
        await redis.xadd(stream, {"n": i})
    for group in ("fast", "slow"):
        await redis.xgroup_create(stream, group, id="0")
    for entry_id, _ in (await redis.xreadgroup("fast", "c", {stream: ">"}))[0][1]:
        await redis.xack(stream, "fast", entry_id)
    await redis.xreadgroup("slow", "c", {stream: ">"}, count=3)  # slow has read 3, none acked

    await trim_finished(redis, stream, "fast")
    assert await redis.xlen(stream) == 10, "the slow group still needs all of them"
    await redis.aclose()


@pytest.mark.integration
async def test_worker_saves_rate_anomalies_and_acks_after(settings, database_url):
    settings = settings.model_copy(update={"database_url": database_url})
    keys = Keys(settings.stream_prefix)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    engine = make_engine(database_url)
    worker = RateWorker(redis, engine, settings, "rates-1")
    worker.counter.warmup_windows = 3
    await worker.setup()

    windows = [{NORMAL: 30}] * 8 + [{NORMAL: 30, ERROR: 25}] + [{NORMAL: 30}] * 2
    for window, counts in enumerate(windows):
        for template, n in counts.items():
            for i in range(n):
                ts = f"2026-10-21T10:{window // 6:02d}:{window % 6 * 10 + i * 0.1:06.3f}+00:00"
                await redis.xadd(
                    keys.events(0),
                    {
                        "ts": ts,
                        "template_id": template,
                        "line_id": f"{window}.{template}.{i}",
                        "request_id": "-",
                        "service": "shop",
                        "level": "INFO",
                        "message": "m",
                        "params": json.dumps({}),
                    },
                )
    while await worker.process_batch(block_ms=20):
        pass

    async with engine.connect() as conn:
        rows = (await conn.execute(select(RateAnomalyRow.__table__))).all()
    assert [(r.template_id, r.direction, r.count) for r in rows] == [(ERROR, "spike", 25)]
    pending = (await redis.xpending(keys.events(0), GROUP))["pending"]
    assert pending > 0, "the newest windows are still open, so their events are not acked"
    assert pending < 100
    await redis.aclose()
    await engine.dispose()

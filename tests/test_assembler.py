"""Session closing rules. `ts` is the log's own timestamp (event time, seconds
after 10:00); `now` is the wall clock of the builder."""

from logad.sessions.assembler import SessionAssembler, to_row


def event(request_id, ts, service="payment", message="Charging order=o1"):
    return {
        "line_id": f"{request_id}/{ts}/{service}/{message}",
        "request_id": request_id,
        "ts": f"2026-10-21T10:00:{ts:06.3f}+00:00",
        "service": service,
        "level": "INFO",
        "template_id": 1,
        "message": message,
        "params": {},
    }


END = {"service": "shop", "message": "Completed POST /checkout status=200 duration_ms=80"}


def make():
    return SessionAssembler(grace_s=2, timeout_s=30, end_service="shop", end_prefix="Completed ")


def test_busy_system_closes_by_event_time():
    a = make()
    a.add("s", "1", event("r1", 1), now=0)
    a.add("s", "2", event("r1", 2, **END), now=0)
    a.add("s", "3", event("r2", 3), now=0)  # other traffic moves event time to 3
    assert a.due(now=0, upstream_idle=False) == []  # 3 - 2 < grace
    a.add("s", "4", event("r2", 4.5), now=0)  # ... and now to 4.5
    [session] = a.due(now=0, upstream_idle=False)
    assert (session.session_id, session.closed_by) == ("r1", "end")


def test_late_line_within_grace_still_makes_it():
    a = make()
    a.add("s", "1", event("r1", 2, **END), now=0)
    a.add("s", "2", event("r1", 1.5, service="shipping"), now=1)  # arrives after the end line
    [session] = a.due(now=3.5)
    assert len(session.events) == 2


def test_stalled_pipeline_does_not_time_out():
    """Lines are stuck upstream: event time stands still, nothing times out."""
    a = make()
    a.add("s", "1", event("r1", 1), now=0)
    assert a.due(now=100, upstream_idle=False) == []
    assert "r1" in a.open


def test_quiet_system_closes_by_wall_clock():
    """Nothing waiting upstream: wall time moves event time forward."""
    a = make()
    a.add("s", "1", event("r1", 1, **END), now=0)
    assert a.due(now=1.9, upstream_idle=True) == []
    [session] = a.due(now=2.1, upstream_idle=True)
    assert session.closed_by == "end"


def test_closes_on_timeout_without_a_last_line():
    a = make()
    a.add("s", "1", event("r1", 1), now=0)
    assert a.due(now=29) == []
    [session] = a.due(now=30)
    assert session.closed_by == "timeout"


def test_events_after_close_are_late():
    a = make()
    a.add("s", "1", event("r1", 1, **END), now=0)
    a.due(now=5)
    assert a.add("s", "2", event("r1", 2), now=6) == "late"
    assert "r1" not in a.open


def test_a_line_delivered_twice_is_kept_once():
    a = make()
    assert a.add("s", "1", event("r1", 1), now=0) == "added"
    assert a.add("s", "9", event("r1", 1), now=1) == "duplicate"  # same line, new entry
    assert len(a.open["r1"].events) == 1


def test_the_same_entry_read_again_is_redelivered_not_duplicate():
    a = make()
    a.add("s", "1", event("r1", 1), now=0)
    assert a.add("s", "1", event("r1", 1), now=1) == "redelivered"  # must not be acked


def test_reopen_puts_unsaved_sessions_back():
    a = make()
    a.add("s", "1", event("r1", 1, **END), now=0)
    sessions = a.due(now=5)
    a.reopen(sessions)  # e.g. the database insert failed
    assert "r1" in a.open
    assert a.add("s", "2", event("r1", 2), now=6) == "added"  # not "late"


def test_lines_outside_requests_are_not_sessions():
    a = make()
    assert a.add("s", "1", event("-", 1), now=0) == "unsessioned"
    assert a.open == {}


def test_row_sorts_events_by_time():
    a = make()
    a.add("s", "1", event("r1", 5, **END), now=0)
    a.add("s", "2", event("r1", 3, service="inventory"), now=0)
    a.add("s", "3", event("r1", 1, service="shop", message="Handling POST /checkout"), now=0)
    row = to_row(a.due(now=10)[0])
    assert [e["ts"][17:19] for e in row["events"]] == ["01", "03", "05"]
    assert row["services"] == ["shop", "inventory"]
    assert row["event_count"] == 3
    assert (row["ended_at"] - row["started_at"]).total_seconds() == 4

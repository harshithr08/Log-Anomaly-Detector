from logad.sessions.assembler import SessionAssembler, to_row


def event(request_id, ts, service="payment", message="Charging order=o1"):
    return {
        "request_id": request_id,
        "ts": f"2026-10-21T10:00:{ts:02d}.000000+00:00",
        "service": service,
        "level": "INFO",
        "template_id": 1,
        "message": message,
        "params": {},
    }


END = {"service": "shop", "message": "Completed POST /checkout status=200 duration_ms=80"}


def make():
    return SessionAssembler(grace_s=2, timeout_s=30, end_service="shop", end_prefix="Completed ")


def test_closes_after_the_last_line_plus_grace():
    a = make()
    a.add("s", "1", event("r1", 1), now=0)
    a.add("s", "2", event("r1", 2, **END), now=1)
    assert a.due(now=2.9) == []  # still in the grace period
    a.add("s", "3", event("r1", 1, service="shipping"), now=2.5)  # a late line still makes it
    [session] = a.due(now=3.1)
    assert session.closed_by == "end" and len(session.events) == 3


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

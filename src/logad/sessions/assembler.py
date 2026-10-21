"""
assembler.py — decide which events belong together and when a session is done.

Pure logic (no Redis, no database), so it is easy to test with a fake clock.

A session closes when:
    end + grace   its last line was seen (e.g. shop "Completed ...") and a short
                  grace period passed, for late lines from other services
    timeout       no new line for a while and no last line was seen

Time is measured with the logs' own timestamps ("event time"), not the clock on
the wall, and it may only advance as far as is *safe*:

    busy upstream   time = the newest timestamp seen, but never past the oldest
                    line the parsers haven't finished (`upstream_safe`). Lines
                    older than that have all arrived here; newer lines may still
                    be on their way (parallel parsers can overtake each other).
                    If the pipeline stalls, time stops: nothing times out by mistake.
    quiet upstream  nothing is waiting anywhere: wall time since the last arrival
                    moves time forward, so the last sessions still close.
"""

from dataclasses import dataclass, field
from datetime import datetime

NO_REQUEST = "-"
REMEMBER_CLOSED_S = 300  # how long to recognise late events of a closed session


def _epoch(ts: str) -> float:
    return datetime.fromisoformat(ts).timestamp()


@dataclass
class OpenSession:
    session_id: str
    events: list[dict] = field(default_factory=list)
    entry_ids: set[tuple[str, str]] = field(default_factory=set)  # (stream, entry id)
    line_ids: set[str] = field(default_factory=set)  # to drop lines delivered twice
    last_ts: float = 0.0  # newest event time in this session
    end_ts: float | None = None  # event time of its last line, once seen
    closed_by: str | None = None


class SessionAssembler:
    def __init__(self, grace_s: float, timeout_s: float, end_service: str, end_prefix: str):
        self.grace_s = grace_s
        self.timeout_s = timeout_s
        self.end_service = end_service
        self.end_prefix = end_prefix
        self.open: dict[str, OpenSession] = {}
        self.recently_closed: dict[str, float] = {}  # session id -> wall time closed
        self.watermark: float | None = None  # newest event time seen
        self.last_arrival = 0.0  # wall time of the newest event's arrival

    def add(self, stream: str, entry_id: str, event: dict, now: float) -> str:
        """Add one event (now = wall time). Returns one of:
        "added"        new event for an open session
        "redelivered"  this very entry was already added (re-read after an
                       error); it is still needed, so it must NOT be acked
        "duplicate"    another entry carrying the same line (sent twice)
        "late"         its session already closed
        "unsessioned"  written outside any request"""
        ts = _epoch(event["ts"])
        if self.watermark is None or ts > self.watermark:
            self.watermark = ts
        self.last_arrival = now

        session_id = event["request_id"]
        if session_id == NO_REQUEST:
            return "unsessioned"
        if session_id in self.recently_closed:
            return "late"
        session = self.open.setdefault(session_id, OpenSession(session_id))
        if (stream, entry_id) in session.entry_ids:
            return "redelivered"
        if event["line_id"] in session.line_ids:
            return "duplicate"
        session.line_ids.add(event["line_id"])
        session.events.append(event)
        session.entry_ids.add((stream, entry_id))
        session.last_ts = max(session.last_ts, ts)
        if event["service"] == self.end_service and event["message"].startswith(self.end_prefix):
            session.end_ts = ts
        return "added"

    def event_clock(
        self, now: float, upstream_idle: bool, upstream_safe: float | None = None
    ) -> float | None:
        """Current time in event time (see the module docstring)."""
        if self.watermark is None:
            return None
        if upstream_idle:
            return self.watermark + max(0.0, now - self.last_arrival)
        if upstream_safe is not None:
            return min(self.watermark, upstream_safe)
        return self.watermark

    def due(
        self, now: float, upstream_idle: bool = True, upstream_safe: float | None = None
    ) -> list[OpenSession]:
        """Remove and return every session that is ready to be saved."""
        clock = self.event_clock(now, upstream_idle, upstream_safe)
        if clock is None:
            return []
        ready = []
        for session in list(self.open.values()):
            if session.end_ts is not None and clock - session.end_ts >= self.grace_s:
                session.closed_by = "end"
            elif clock - session.last_ts >= self.timeout_s:
                session.closed_by = "timeout"
            else:
                continue
            ready.append(self.open.pop(session.session_id))
            self.recently_closed[session.session_id] = now
        self._forget_old(now)
        return ready

    def reopen(self, sessions: list[OpenSession]) -> None:
        """Put sessions back (e.g. saving them failed), so nothing is lost."""
        for session in sessions:
            self.open[session.session_id] = session
            self.recently_closed.pop(session.session_id, None)

    def _forget_old(self, now: float) -> None:
        for session_id, closed_at in list(self.recently_closed.items()):
            if now - closed_at > REMEMBER_CLOSED_S:
                del self.recently_closed[session_id]


def to_row(session: OpenSession) -> dict:
    """The database row for a closed session; events sorted by their timestamp."""
    # sorted() is stable: events with the same timestamp keep their arrival order
    events = sorted(session.events, key=lambda e: datetime.fromisoformat(e["ts"]))
    services = list(dict.fromkeys(e["service"] for e in events))
    return {
        "session_id": session.session_id,
        "started_at": datetime.fromisoformat(events[0]["ts"]),
        "ended_at": datetime.fromisoformat(events[-1]["ts"]),
        "closed_by": session.closed_by,
        "event_count": len(events),
        "services": services,
        "events": events,
    }

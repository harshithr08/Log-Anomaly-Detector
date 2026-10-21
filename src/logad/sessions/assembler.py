"""
assembler.py — decide which events belong together and when a session is done.

Pure logic (no Redis, no database), so it is easy to test with a fake clock.

A session closes when:
    end + grace   its last line was seen (e.g. shop "Completed ...") and a short
                  grace period passed, for late lines from other services
    timeout       nothing new arrived for a while and no last line was seen
"""

from dataclasses import dataclass, field
from datetime import datetime

NO_REQUEST = "-"
REMEMBER_CLOSED_S = 300  # how long to recognise late events of a closed session


@dataclass
class OpenSession:
    session_id: str
    events: list[dict] = field(default_factory=list)
    entry_ids: list[tuple[str, str]] = field(default_factory=list)  # (stream, entry id)
    last_seen: float = 0.0
    end_seen_at: float | None = None
    closed_by: str | None = None


class SessionAssembler:
    def __init__(self, grace_s: float, timeout_s: float, end_service: str, end_prefix: str):
        self.grace_s = grace_s
        self.timeout_s = timeout_s
        self.end_service = end_service
        self.end_prefix = end_prefix
        self.open: dict[str, OpenSession] = {}
        self.recently_closed: dict[str, float] = {}

    def add(self, stream: str, entry_id: str, event: dict, now: float) -> str:
        """Add one event. Returns "added", "late" (its session already closed)
        or "unsessioned" (written outside any request)."""
        session_id = event["request_id"]
        if session_id == NO_REQUEST:
            return "unsessioned"
        if session_id in self.recently_closed:
            return "late"
        session = self.open.setdefault(session_id, OpenSession(session_id))
        session.events.append(event)
        session.entry_ids.append((stream, entry_id))
        session.last_seen = now
        if event["service"] == self.end_service and event["message"].startswith(self.end_prefix):
            session.end_seen_at = now
        return "added"

    def due(self, now: float) -> list[OpenSession]:
        """Remove and return every session that is ready to be saved."""
        ready = []
        for session in list(self.open.values()):
            if session.end_seen_at is not None and now - session.end_seen_at >= self.grace_s:
                session.closed_by = "end"
            elif now - session.last_seen >= self.timeout_s:
                session.closed_by = "timeout"
            else:
                continue
            ready.append(self.open.pop(session.session_id))
            self.recently_closed[session.session_id] = now
        self._forget_old(now)
        return ready

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

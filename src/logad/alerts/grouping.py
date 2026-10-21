"""
grouping.py — turn individual anomalies into alerts. Pure logic, no database.

Evidence comes from three detectors, each naming a service:
    lstm    a request with an unexpected step   -> the service of that step
    timing  a slow request                      -> the slowest service
    rate    an event type spiking or dropping   -> the service of that event type

Rules, per service:
    open     at least `min_sessions` flagged requests within `window_s`, or any
             rate anomaly (the rate detector already demands a big change).
             A single flagged request is not an alert: with 1 in 1,000 normal
             requests flagged, that would fire every few minutes.
    update   while open: count, detectors, summary and examples follow the evidence
    resolve  no new evidence for `quiet_s`
"""

from collections import Counter
from dataclasses import dataclass


@dataclass
class Evidence:
    service: str
    at: float  # when it was detected (seconds since 1970)
    detector: str  # "lstm" | "timing" | "rate"
    session_id: str | None  # None for rate anomalies
    explanation: dict


@dataclass
class OpenAlert:
    service: str
    opened_at: float
    alert_id: int | None = None  # set once saved


@dataclass
class AlertUpdate:
    """What the alert of one service should look like now."""

    service: str
    opened_at: float
    last_seen_at: float
    sessions: int
    detectors: list[str]
    summary: str | None  # None: keep the saved summary and examples
    examples: list[dict]
    resolve: bool = False
    alert_id: int | None = None


def _short(template: str) -> str:
    """'payment INFO Fraud check passed order=<*>' -> 'Fraud check passed order=<*>'."""
    parts = template.split(" ", 2)
    return parts[2] if len(parts) == 3 else template


def service_of_template(template: str) -> str:
    return template.split(" ", 1)[0]


def evidence_from_score(session_id: str, at: float, explanation: dict) -> list[Evidence]:
    """One piece of evidence per detector that flagged this session."""
    found = []
    if "lstm" in explanation:
        service = service_of_template(explanation["lstm"]["got"])
        found.append(Evidence(service, at, "lstm", session_id, explanation["lstm"]))
    if "timing" in explanation:
        found.append(
            Evidence(
                explanation["timing"]["slowest"], at, "timing", session_id, explanation["timing"]
            )
        )
    return found


def summarize(service: str, evidence: list[Evidence]) -> str:
    parts = []
    lstm = [e for e in evidence if e.detector == "lstm"]
    if lstm:
        (after, expected, got), _ = Counter(
            (e.explanation["after"], e.explanation["expected"], e.explanation["got"]) for e in lstm
        ).most_common(1)[0]
        parts.append(
            f"{len(lstm)} requests with an unexpected step: after '{_short(after)}' "
            f"expected '{_short(expected)}', got '{_short(got)}'"
        )
    timing = [e for e in evidence if e.detector == "timing"]
    if timing:
        spreads = sorted(e.explanation["spreads_slower"] for e in timing)
        parts.append(
            f"{len(timing)} slow requests: {service} typically {spreads[len(spreads) // 2]:.0f} "
            f"spreads slower than normal"
        )
    rate = [e for e in evidence if e.detector == "rate"]
    if rate:
        top = max(rate, key=lambda e: abs(e.explanation["z"]))
        x = top.explanation
        parts.append(
            f"{x['direction']} of '{_short(x['template'])}': {x['count']} in 10 s, "
            f"usually {x['expected']:.0f}"
        )
    return f"{service}: " + "; ".join(parts)


def decide(
    evidence: list[Evidence],
    open_alerts: dict[str, OpenAlert],
    now: float,
    min_sessions: int = 5,
    window_s: float = 60.0,
    quiet_s: float = 60.0,
) -> list[AlertUpdate]:
    """What to open, update or resolve, given recent evidence and open alerts."""
    by_service: dict[str, list[Evidence]] = {}
    for e in sorted(evidence, key=lambda e: e.at):
        by_service.setdefault(e.service, []).append(e)

    updates = []
    for service in set(by_service) | set(open_alerts):
        found = by_service.get(service, [])
        alert = open_alerts.get(service)
        if alert is None:
            recent = [e for e in found if e.at >= now - window_s]
            flagged = {e.session_id for e in recent if e.session_id}
            if len(flagged) < min_sessions and not any(e.detector == "rate" for e in recent):
                continue
            opened_at = recent[0].at
        else:
            opened_at = alert.opened_at

        incident = [e for e in found if e.at >= opened_at]
        if not incident:  # an open alert whose evidence has aged out: just resolve it
            if now - opened_at >= quiet_s:
                updates.append(
                    AlertUpdate(
                        service, opened_at, opened_at, 0, [], None, [], True, alert.alert_id
                    )
                )
            continue
        last_seen = incident[-1].at
        sessions = {e.session_id for e in incident if e.session_id}
        examples, seen = [], set()
        for e in reversed(incident):  # newest first, one per session
            if e.session_id and e.session_id not in seen and len(examples) < 5:
                seen.add(e.session_id)
                examples.append(
                    {
                        "session_id": e.session_id,
                        "detector": e.detector,
                        "explanation": e.explanation,
                    }
                )
        updates.append(
            AlertUpdate(
                service=service,
                opened_at=opened_at,
                last_seen_at=last_seen,
                sessions=len(sessions),
                detectors=sorted({e.detector for e in incident}),
                summary=summarize(service, incident),
                examples=examples,
                resolve=alert is not None and now - last_seen >= quiet_s,
                alert_id=alert.alert_id if alert else None,
            )
        )
    return updates

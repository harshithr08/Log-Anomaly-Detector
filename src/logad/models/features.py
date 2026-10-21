"""
features.py — how slow each part of a session was, compared with sessions
that took exactly the same path.

A raw time means little on its own: a product view served from cache takes
~2 ms, the same view on a cache miss ~10 ms, a checkout ~200 ms. So each time
is compared with training sessions that went through exactly the same steps
(same sequence of event types): z = (x - median) / spread, the spread being a
robust one (MAD). Paths seen too rarely fall back to all sessions of the same
request type.

Only "slower than normal" counts (z < 0 becomes 0): this detector exists to
catch slow requests. Missing or extra steps are the order detectors' job.

Design notes (chosen on the development dataset): grouping by request type
alone mixed cache hits with misses, and declined with completed checkouts,
making normal look so varied that slow requests hid in it; counts of events,
warnings or retries are constant within a path, so only times are kept.
"""

import math
from collections import defaultdict

import numpy as np

from logad.models.dataset import Session

SERVICES = ("shop", "inventory", "payment", "shipping")
FEATURES = ("total_ms", *(f"{service}_ms" for service in SERVICES))
MIN_SPREAD = 0.1  # on the log scale; so a very steady path doesn't explode
MIN_PATH_SESSIONS = 30  # fewer than this: use the request type's numbers


def raw_features(session: Session) -> np.ndarray:
    """Times on a log scale (they vary over orders of magnitude)."""
    service_ms = dict.fromkeys(SERVICES, 0.0)
    for event in session.events:
        if event["message"].startswith("Completed ") and "duration_ms" in event["params"]:
            service = event["service"]
            service_ms[service] = service_ms.get(service, 0.0) + float(
                event["params"]["duration_ms"]
            )
    total_ms = (session.ended_at - session.started_at) * 1000
    values = [total_ms, *(service_ms[service] for service in SERVICES)]
    return np.array([math.log1p(max(v, 0.0)) for v in values])


def _stats(rows: list[np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    rows = np.array(rows)
    median = np.median(rows, axis=0)
    spread = np.median(np.abs(rows - median), axis=0) * 1.4826  # robust standard deviation
    return median, np.maximum(spread, MIN_SPREAD)


class PathScaler:
    """Median and spread of each time, per path (and per request type as fallback)."""

    def fit(self, sessions: list[Session]):
        by_path, by_route = defaultdict(list), defaultdict(list)
        for session in sessions:
            features = raw_features(session)
            by_path[tuple(session.templates)].append(features)
            by_route[session.route].append(features)
        self.paths = {
            path: _stats(rows) for path, rows in by_path.items() if len(rows) >= MIN_PATH_SESSIONS
        }
        self.routes = {route: _stats(rows) for route, rows in by_route.items()}
        self.fallback = _stats([f for rows in by_route.values() for f in rows])
        return self

    def stats_for(self, session: Session) -> tuple[np.ndarray, np.ndarray]:
        found = self.paths.get(tuple(session.templates))
        return found if found is not None else self.routes.get(session.route, self.fallback)

    def transform(self, sessions: list[Session]) -> np.ndarray:
        """How many spreads slower than normal, per time (never below 0)."""
        rows = []
        for session in sessions:
            median, spread = self.stats_for(session)
            rows.append(np.maximum((raw_features(session) - median) / spread, 0.0))
        return np.array(rows).reshape(-1, len(FEATURES))

    def to_dict(self) -> dict:
        def pack(stats):
            return [stats[0].tolist(), stats[1].tolist()]

        return {
            "features": list(FEATURES),
            "paths": [[list(path), *pack(stats)] for path, stats in self.paths.items()],
            "routes": {route: pack(stats) for route, stats in self.routes.items()},
            "fallback": pack(self.fallback),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PathScaler":
        def unpack(median, spread):
            return np.array(median), np.array(spread)

        scaler = cls()
        scaler.paths = {tuple(path): unpack(m, s) for path, m, s in data["paths"]}
        scaler.routes = {route: unpack(*stats) for route, stats in data["routes"].items()}
        scaler.fallback = unpack(*data["fallback"])
        return scaler

"""
timing.py — how much slower than normal a request was, compared with requests
that took exactly the same path.

For each of the request's times (total, shop, inventory, payment, shipping):
"how many spreads slower than the same path?" (see features.py). The score
combines the five into one number, the distance from normal (L2 norm).

Why not the Isolation Forest (iforest.py)? On the development data it caught
fewer slow requests (96.9% vs 98.1% at the same false-alarm rate) and its score
saturates: a tree can only cut inside the range of its training data, so a
request 50x slower scored barely higher (0.83 -> 0.87) than one just outside
normal. This distance keeps growing with the slowdown, and explains itself.
"""

import numpy as np

from logad.models.dataset import Session
from logad.models.features import FEATURES, PathScaler


class TimingDetector:
    name = "Timing vs same path (distance)"

    def fit(self, sessions: list[Session]):
        self.scaler = PathScaler().fit(sessions)
        return self

    def score(self, sessions: list[Session]) -> np.ndarray:
        return np.linalg.norm(self.scaler.transform(sessions), axis=1)

    def explain(self, session: Session) -> str:
        """The service furthest above normal for this path (the total is always
        slow too when a service is, so it would not say *where*)."""
        z = self.scaler.transform([session])[0]
        services = [i for i, name in enumerate(FEATURES) if name != "total_ms"]
        worst = max(services, key=lambda i: z[i])
        total = z[FEATURES.index("total_ms")]
        return (
            f"{FEATURES[worst]} is {z[worst]:.1f} spreads slower than normal "
            f"(whole request: {total:.1f})"
        )

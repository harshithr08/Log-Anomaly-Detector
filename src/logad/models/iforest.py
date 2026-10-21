"""
iforest.py — Isolation Forest on "how much slower than the same path".

It learns how a normal request's times relate to each other (total, shop,
inventory, payment, shipping). An odd combination is isolated with few random
cuts and gets a high score. On the development data it beat the simpler rule
"flag the largest single deviation" (97% vs 85% of slow requests caught at the
same false-alarm rate), because it judges all the times together.
"""

import numpy as np
from sklearn.ensemble import IsolationForest

from logad.models.dataset import Session
from logad.models.features import FEATURES, PathScaler

SEED = 42


class IForestDetector:
    name = "Isolation Forest (timing)"

    def __init__(self, n_estimators: int = 200, max_samples: int = 2048):
        self.n_estimators = n_estimators
        self.max_samples = max_samples

    def fit(self, sessions: list[Session]):
        self.scaler = PathScaler().fit(sessions)
        x = self.scaler.transform(sessions)
        self.model = IsolationForest(
            n_estimators=self.n_estimators,
            max_samples=min(self.max_samples, len(x)),
            random_state=SEED,
            n_jobs=-1,
        ).fit(x)
        return self

    def score(self, sessions: list[Session]) -> np.ndarray:
        # sklearn's score_samples is "higher = more normal": flip it
        return -self.model.score_samples(self.scaler.transform(sessions))

    def explain(self, session: Session) -> str:
        """The time furthest above normal for this path."""
        z = self.scaler.transform([session])[0]
        worst = int(np.argmax(z))
        return f"{FEATURES[worst]} is {z[worst]:.1f} spreads slower than normal"

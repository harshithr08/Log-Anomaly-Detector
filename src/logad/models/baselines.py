"""
baselines.py — simple rules every model has to beat.

Every detector has the same two methods:
    fit(sessions)    learn from NORMAL sessions only
    score(sessions)  one number per session; higher = more suspicious
"""

import numpy as np

from logad.models.dataset import Session


class ErrorLine:
    """Score = number of ERROR lines. Catches failures that log errors; blind
    to anything that fails quietly (a skipped step, a slow service)."""

    name = "ERROR line (rule)"

    def fit(self, sessions: list[Session]):
        return self

    def score(self, sessions: list[Session]) -> np.ndarray:
        return np.array([sum(e["level"] == "ERROR" for e in s.events) for s in sessions], float)


class UnseenSequence:
    """1 if this exact sequence of event types never appeared in training."""

    name = "Unseen sequence"

    def fit(self, sessions: list[Session]):
        self.known = {tuple(s.templates) for s in sessions}
        return self

    def score(self, sessions: list[Session]) -> np.ndarray:
        return np.array([float(tuple(s.templates) not in self.known) for s in sessions])


class MarkovChain:
    """Counts how often event B follows event A in training. Score = surprise
    (-log probability) of the session's least likely step. The simplest model
    of event order: it only looks one step back, where the LSTM looks ten."""

    name = "Markov chain (event order)"

    def __init__(self, smoothing: float = 0.01):
        self.smoothing = smoothing

    @staticmethod
    def _steps(session: Session):
        path = ["<start>", *session.templates, "<end>"]
        return zip(path, path[1:], strict=False)

    def fit(self, sessions: list[Session]):
        self.counts: dict[str, dict[str, int]] = {}
        for session in sessions:
            for before, after in self._steps(session):
                row = self.counts.setdefault(before, {})
                row[after] = row.get(after, 0) + 1
        self.vocab_size = len({t for s in sessions for t in s.templates}) + 2
        return self

    def _surprise(self, before: str, after: str) -> float:
        row = self.counts.get(before, {})
        total = sum(row.values())
        p = (row.get(after, 0) + self.smoothing) / (total + self.smoothing * self.vocab_size)
        return -np.log(p)

    def score(self, sessions: list[Session]) -> np.ndarray:
        return np.array([max(self._surprise(a, b) for a, b in self._steps(s)) for s in sessions])


class UnseenTemplate:
    """Number of events whose type never appeared in training."""

    name = "Unseen event type"

    def fit(self, sessions: list[Session]):
        self.known = {t for s in sessions for t in s.templates}
        return self

    def score(self, sessions: list[Session]) -> np.ndarray:
        return np.array([sum(t not in self.known for t in s.templates) for s in sessions], float)

"""
sequences.py — turn sessions into "last few events -> next event" examples.

No PyTorch here: training (PyTorch) and live scoring (ONNX Runtime) share it.

Every session is wrapped as START, e1, e2, ..., en, END, and for every position
the model sees up to WINDOW previous events and must predict the next one.
START/END let it notice a missing first or last step too.
"""

import numpy as np

from logad.models.dataset import Session

PAD, UNK, START, END = 0, 1, 2, 3
SPECIAL = ["<pad>", "<unknown>", "<start>", "<end>"]
WINDOW = 10


class Vocab:
    """Template text <-> number. Text, not database IDs: IDs restart when the
    database is reset; the text stays the same."""

    def __init__(self, templates: list[str]):
        self.templates = SPECIAL + sorted(set(templates))
        self.index = {t: i for i, t in enumerate(self.templates)}

    @classmethod
    def from_list(cls, templates: list[str]) -> "Vocab":
        """Rebuild a saved vocabulary (vocab.json) in exactly its saved order."""
        vocab = cls([])
        vocab.templates = list(templates)
        vocab.index = {t: i for i, t in enumerate(vocab.templates)}
        return vocab

    def __len__(self) -> int:
        return len(self.templates)

    def encode(self, session: Session) -> list[int]:
        return [START, *(self.index.get(t, UNK) for t in session.templates), END]


def windows(vocab: Vocab, sessions: list[Session]):
    """(windows [N, WINDOW], next events [N], session index of each [N])."""
    xs, ys, owners = [], [], []
    for owner, session in enumerate(sessions):
        tokens = vocab.encode(session)
        for i in range(1, len(tokens)):
            history = tokens[max(0, i - WINDOW) : i]
            xs.append([PAD] * (WINDOW - len(history)) + history)
            ys.append(tokens[i])
            owners.append(owner)
    return (
        np.array(xs, dtype=np.int64).reshape(-1, WINDOW),
        np.array(ys, dtype=np.int64),
        np.array(owners, dtype=np.int64),
    )


def log_softmax(logits: np.ndarray) -> np.ndarray:
    shifted = logits - logits.max(axis=1, keepdims=True)
    return shifted - np.log(np.exp(shifted).sum(axis=1, keepdims=True))


def surprise_per_session(log_probs, targets, owners, n_sessions):
    """Score = surprise (-log probability) of each session's most surprising
    step. Returns (scores [n_sessions], index of that step in the windows)."""
    surprise = -log_probs[np.arange(len(targets)), targets]
    scores = np.zeros(n_sessions)
    worst = np.full(n_sessions, -1)
    for i, (owner, value) in enumerate(zip(owners, surprise, strict=True)):
        if value > scores[owner] or worst[owner] == -1:
            scores[owner], worst[owner] = value, i
    return scores, worst

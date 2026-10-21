"""
serving.py — load a saved model version and score sessions, without PyTorch.

The LSTM runs through ONNX Runtime (lstm.onnx), the timing detector is plain
numpy. This is what the live scoring workers use, so their Docker image stays
small: no PyTorch.
"""

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import onnxruntime as ort

from logad.models.dataset import Session
from logad.models.features import FEATURES, PathScaler
from logad.models.sequences import Vocab, log_softmax, surprise_per_session, windows


@dataclass
class Verdict:
    session_id: str
    lstm_score: float
    timing_score: float
    lstm_flag: bool
    timing_flag: bool
    explanation: dict  # only for the detectors that fired

    @property
    def anomalous(self) -> bool:
        return self.lstm_flag or self.timing_flag


def resolve_version(models_dir: Path, version: str = "latest") -> str:
    if version == "latest":
        return (models_dir / "latest").read_text(encoding="utf-8").strip()
    return version


class ServingModels:
    def __init__(self, models_dir: Path, version: str = "latest"):
        self.version = resolve_version(models_dir, version)
        folder = models_dir / self.version

        def read(name):
            return json.loads((folder / name).read_text(encoding="utf-8"))

        self.vocab = Vocab.from_list(read("vocab.json"))
        self.scaler = PathScaler.from_dict(read("scaler.json"))
        self.thresholds = read("thresholds.json")
        self.lstm = ort.InferenceSession(
            str(folder / "lstm.onnx"), providers=["CPUExecutionProvider"]
        )

    def lstm_logits(self, x: np.ndarray) -> np.ndarray:
        if len(x) == 0:
            return np.zeros((0, len(self.vocab)), np.float32)
        return self.lstm.run(None, {"windows": x})[0]

    def score(self, sessions: list[Session]) -> list[Verdict]:
        x, y, owners = windows(self.vocab, sessions)
        log_probs = log_softmax(self.lstm_logits(x))
        lstm_scores, worst_step = surprise_per_session(log_probs, y, owners, len(sessions))
        z = self.scaler.transform(sessions)
        timing_scores = np.linalg.norm(z, axis=1)
        # where each session's windows begin, to map a window back to its event
        first_window = np.searchsorted(owners, np.arange(len(sessions)))

        verdicts = []
        for i, session in enumerate(sessions):
            lstm_flag = bool(lstm_scores[i] > self.thresholds["lstm"])
            timing_flag = bool(timing_scores[i] > self.thresholds["timing"])
            explanation = {}
            if lstm_flag:
                step = worst_step[i]
                # The k-th window of a session predicts event k (k == n: the end).
                # Use the session's real text: an event type the model never saw
                # is "<unknown>" in its vocabulary, which says nothing.
                path = ["<start>", *session.templates, "<end>"]
                k = int(step - first_window[i])
                explanation["lstm"] = {
                    "after": path[k],
                    "expected": self.vocab.templates[int(np.argmax(log_probs[step]))],
                    "got": path[k + 1],
                    "probability": float(np.exp(log_probs[step][y[step]])),
                }
            if timing_flag:
                services = [j for j, name in enumerate(FEATURES) if name != "total_ms"]
                worst = max(services, key=lambda j: z[i][j])
                explanation["timing"] = {
                    "slowest": FEATURES[worst].removesuffix("_ms"),
                    "spreads_slower": round(float(z[i][worst]), 1),
                    "whole_request_spreads": round(float(z[i][0]), 1),
                }
            verdicts.append(
                Verdict(
                    session.session_id,
                    float(lstm_scores[i]),
                    float(timing_scores[i]),
                    lstm_flag,
                    timing_flag,
                    explanation,
                )
            )
        return verdicts

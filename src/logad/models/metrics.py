"""
metrics.py — thresholds from a false-alarm budget, and how good a detector is.

The threshold is never tuned on test data. It comes from normal validation
sessions only: "at most `false_alarm_rate` of normal requests may be flagged".
"""

import numpy as np
from sklearn.metrics import average_precision_score, precision_recall_fscore_support

FALSE_ALARM_BUDGET = 0.001  # at most 1 in 1,000 normal requests flagged


def threshold_for(normal_scores: np.ndarray, false_alarm_rate: float = FALSE_ALARM_BUDGET) -> float:
    """Flag a session when its score is *above* this threshold. At most
    `false_alarm_rate` of these normal scores are above it."""
    ordered = np.sort(np.asarray(normal_scores, dtype=float))
    allowed = int(len(ordered) * false_alarm_rate)  # normal sessions we may flag
    return float(ordered[len(ordered) - 1 - allowed])


def evaluate(scores, labels, faults, threshold: float) -> dict:
    """How well flagging `score > threshold` matches the ground truth.
    `faults` holds e.g. "payment skip" for broken sessions, None otherwise."""
    scores, labels = np.asarray(scores, dtype=float), np.asarray(labels)
    flagged = scores > threshold
    precision, recall, f1, _ = precision_recall_fscore_support(
        labels, flagged, average="binary", zero_division=0
    )
    result = {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "pr_auc": average_precision_score(labels, scores) if labels.any() else 0.0,
        "false_alarm_rate": float(flagged[labels == 0].mean()) if (labels == 0).any() else 0.0,
    }
    kinds = np.array([fault.split()[1] if fault else "" for fault in faults])
    for kind in ("skip", "slow", "error"):
        broken = kinds == kind
        result[f"recall_{kind}"] = float(flagged[broken].mean()) if broken.any() else float("nan")
    return result

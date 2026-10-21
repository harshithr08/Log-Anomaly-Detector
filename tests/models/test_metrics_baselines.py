import numpy as np
import pytest

from logad.models.baselines import ErrorLine, MarkovChain, UnseenSequence, UnseenTemplate
from logad.models.dataset import Session, split
from logad.models.metrics import evaluate, threshold_for


def session(templates, started_at=0.0, label=0, fault=None, levels=None):
    levels = levels or ["INFO"] * len(templates)
    events = [
        {"ts": "", "service": "shop", "level": lvl, "template": t, "message": t, "params": {}}
        for t, lvl in zip(templates, levels, strict=True)
    ]
    return Session("id", started_at, started_at, "end", events, label, fault)


def test_threshold_respects_the_false_alarm_budget():
    normal = np.random.default_rng(0).normal(size=10_000)
    threshold = threshold_for(normal, false_alarm_rate=0.001)
    assert (normal > threshold).sum() <= 10


def test_threshold_with_many_ties_flags_nothing_rather_than_too_much():
    normal = np.array([0.0] * 990 + [1.0] * 10)  # 1% of normal sessions score 1
    assert threshold_for(normal, false_alarm_rate=0.001) == 1.0  # so 1 is not "above"


def test_evaluate_reports_recall_per_fault_kind():
    scores = [0, 0, 5, 5, 0]
    labels = [0, 0, 1, 1, 1]
    faults = [None, None, "payment skip", "shop slow", "shop slow"]
    result = evaluate(scores, labels, faults, threshold=1)
    assert result["precision"] == 1.0
    assert result["recall"] == pytest.approx(2 / 3)
    assert result["recall_skip"] == 1.0 and result["recall_slow"] == 0.5
    assert np.isnan(result["recall_error"])
    assert result["false_alarm_rate"] == 0.0


def test_baselines():
    train = [session(["a", "b", "c"]), session(["a", "c"])]
    test = [
        session(["a", "b", "c"]),
        session(["a", "b"]),  # new order, known types
        session(["a", "x", "c"], levels=["INFO", "ERROR", "INFO"]),  # new type, an error
    ]
    assert list(ErrorLine().fit(train).score(test)) == [0, 0, 1]
    assert list(UnseenSequence().fit(train).score(test)) == [0, 1, 1]
    assert list(UnseenTemplate().fit(train).score(test)) == [0, 0, 1]


def test_markov_chain_flags_an_unusual_step():
    train = [session(["a", "b", "c"])] * 50 + [session(["a", "c"])] * 50
    scores = (
        MarkovChain()
        .fit(train)
        .score([session(["a", "b", "c"]), session(["a", "c"]), session(["a", "c", "b"])])
    )
    assert scores[0] < 1 and scores[1] < 1  # both normal paths
    assert scores[2] > 5  # "c" then "b" never happened


def test_split_is_by_time():
    sessions = [session(["a"], started_at=t) for t in range(10)] + [
        session(["a"], started_at=10 + t, label=t % 2, fault="shop skip" if t % 2 else None)
        for t in range(4)
    ]
    splits = split(sessions, {"clean": [0, 10]})
    assert [s.started_at for s in splits.train] == [0, 1, 2, 3, 4, 5, 6]
    assert [s.started_at for s in splits.val] == [7, 8, 9]
    assert [s.started_at for s in splits.test] == [10, 11, 12, 13]
    assert splits.info["test_broken"] == 2


def test_split_refuses_faults_in_the_clean_period():
    with pytest.raises(AssertionError):
        split([session(["a"], started_at=1, label=1)], {"clean": [0, 10]})

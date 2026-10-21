import json
import random

import numpy as np

from logad.models.dataset import Session
from logad.models.features import FEATURES, PathScaler
from logad.models.lstm import LSTMDetector
from logad.models.metrics import threshold_for
from logad.models.timing import TimingDetector
from logad.models.train import save


def request(path, shop_ms, inventory_ms=0.0, start=0.0):
    """A synthetic session: steps of `path`, with Completed lines carrying times."""
    events = [
        {
            "ts": "",
            "service": "shop",
            "level": "INFO",
            "template": step,
            "message": step,
            "params": {},
        }
        for step in path
    ]
    if inventory_ms:
        events.insert(
            1,
            {
                "ts": "",
                "service": "inventory",
                "level": "INFO",
                "template": "inventory Completed",
                "message": "Completed GET /stock",
                "params": {"duration_ms": inventory_ms},
            },
        )
    events.append(
        {
            "ts": "",
            "service": "shop",
            "level": "INFO",
            "template": "shop Completed",
            "message": "Completed GET /p",
            "params": {"duration_ms": shop_ms},
        }
    )
    return Session("id", start, start + shop_ms / 1000, "end", events)


HIT = ["shop Handling GET /p", "shop Cache hit"]
MISS = ["shop Handling GET /p", "shop Cache miss"]


def normal_traffic(n, rng):
    """Cache hits take ~2 ms; misses call inventory and take ~10 ms."""
    sessions = []
    for _ in range(n):
        if rng.random() < 0.5:
            sessions.append(request(HIT, shop_ms=rng.uniform(1.5, 2.5)))
        else:
            inv = rng.uniform(6, 9)
            sessions.append(request(MISS, shop_ms=inv + rng.uniform(1.5, 2.5), inventory_ms=inv))
    return sessions


def test_times_are_compared_with_the_same_path():
    scaler = PathScaler().fit(normal_traffic(400, random.Random(1)))
    slow_hit = request(HIT, shop_ms=12)  # normal for a miss, very slow for a hit
    fast_miss = request(MISS, shop_ms=3, inventory_ms=1)  # faster than normal: not our concern
    z = scaler.transform([slow_hit, fast_miss])
    assert z[0][FEATURES.index("shop_ms")] > 5
    assert z[1].max() == 0


def test_timing_flags_slow_requests_not_normal_ones():
    rng = random.Random(2)
    detector = TimingDetector().fit(normal_traffic(2000, rng))
    threshold = threshold_for(detector.score(normal_traffic(2000, rng)))
    normal = detector.score(normal_traffic(500, rng))
    slow = detector.score(
        [request(MISS, shop_ms=600, inventory_ms=590) for _ in range(20)]
        + [request(HIT, shop_ms=400) for _ in range(20)]
    )
    assert (normal > threshold).mean() < 0.01
    assert (slow > threshold).all()
    assert "shop_ms" in detector.explain(request(HIT, shop_ms=400))


def test_timing_score_keeps_growing_with_the_slowdown():
    """No ceiling: a worse outage always scores higher (unlike Isolation Forest)."""
    detector = TimingDetector().fit(normal_traffic(1000, random.Random(4)))
    scores = detector.score([request(HIT, shop_ms=ms) for ms in (20, 200, 2000, 20000)])
    assert all(a < b for a, b in zip(scores, scores[1:], strict=False))


def test_lstm_learns_the_workflow_and_flags_a_skipped_step():
    flow = ["start checkout", "reserve stock", "charge card", "fraud check", "authorized", "ship"]
    no_stock = ["start checkout", "reserve stock", "out of stock"]
    train = [request(flow, 1)] * 300 + [request(no_stock, 1)] * 100
    lstm = LSTMDetector(epochs=30, batch_size=64).fit(train, log=lambda _: None)

    skipped = [s for s in flow if s != "fraud check"]
    normal_scores = lstm.score([request(flow, 1), request(no_stock, 1)])
    [skip_score] = lstm.score([request(skipped, 1)])
    assert skip_score > 3 * normal_scores.max()
    assert "expected 'fraud check'" in lstm.explain(request(skipped, 1))


def test_saved_model_folder_has_everything(tmp_path):
    rng = random.Random(3)
    traffic = normal_traffic(300, rng)
    lstm = LSTMDetector(epochs=1).fit(traffic, log=lambda _: None)
    timing = TimingDetector().fit(traffic)
    save(tmp_path / "models" / "v1", lstm, timing, {"lstm": 1.0}, [], {"version": "v1"})

    folder = tmp_path / "models" / "v1"
    for name in (
        "lstm.pt",
        "lstm.onnx",
        "vocab.json",
        "scaler.json",
        "thresholds.json",
        "metrics.json",
        "meta.json",
    ):
        assert (folder / name).exists(), name
    assert (tmp_path / "models" / "latest").read_text() == "v1"
    scaler = PathScaler.from_dict(json.loads((folder / "scaler.json").read_text()))
    sample = traffic[:5]
    assert np.allclose(scaler.transform(sample), timing.scaler.transform(sample))

import random

import numpy as np
from test_models import HIT, normal_traffic, request

from logad.models.lstm import LSTMDetector
from logad.models.metrics import threshold_for
from logad.models.serving import ServingModels
from logad.models.timing import TimingDetector
from logad.models.train import save


def trained_models(folder):
    rng = random.Random(5)
    train, val = normal_traffic(1500, rng), normal_traffic(1500, rng)
    lstm = LSTMDetector(epochs=3).fit(train, log=lambda _: None)
    timing = TimingDetector().fit(train)
    thresholds = {
        "lstm": threshold_for(lstm.score(val)),
        "timing": threshold_for(timing.score(val)),
    }
    save(folder / "v1", lstm, timing, thresholds, [], {"version": "v1"})
    return lstm, timing


def test_onnx_serving_matches_pytorch(tmp_path):
    lstm, timing = trained_models(tmp_path)
    served = ServingModels(tmp_path)  # "latest" -> v1, no PyTorch involved
    assert served.version == "v1"

    sessions = normal_traffic(50, random.Random(6))
    verdicts = served.score(sessions)
    assert np.allclose([v.lstm_score for v in verdicts], lstm.score(sessions), atol=1e-4)
    assert np.allclose([v.timing_score for v in verdicts], timing.score(sessions))


def test_verdicts_flag_and_explain(tmp_path):
    trained_models(tmp_path)
    served = ServingModels(tmp_path)
    skipped = request(["shop Handling GET /p"], shop_ms=2)  # cache step missing
    slow = request(HIT, shop_ms=900)
    normal, skip, slow = served.score([request(HIT, shop_ms=2), skipped, slow])

    assert not normal.anomalous and normal.explanation == {}
    [new_error] = served.score(
        [request(["shop Handling GET /p", "shipping ERROR Printer on fire"], shop_ms=2)]
    )
    assert new_error.explanation["lstm"]["got"] == "shipping ERROR Printer on fire"  # not <unknown>
    assert skip.lstm_flag and skip.explanation["lstm"]["got"] == "shop Completed"
    assert slow.timing_flag and slow.explanation["timing"]["slowest"] == "shop"

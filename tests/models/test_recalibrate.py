import json
import random

from test_models import HIT, MISS, request
from test_serving import trained_models

from logad.models.recalibrate import new_thresholds, save_version
from logad.models.serving import ServingModels


def drifted_traffic(n, rng):
    """Everything got ~3x slower for good (say, a new release): new normal."""
    return [
        request(HIT, shop_ms=rng.uniform(5, 7))
        if rng.random() < 0.5
        else request(MISS, shop_ms=rng.uniform(28, 33), inventory_ms=rng.uniform(20, 25))
        for _ in range(n)
    ]


def test_recalibration_adapts_thresholds_to_drift(tmp_path):
    trained_models(tmp_path)  # v1, trained on the old normal
    old = ServingModels(tmp_path)
    rng = random.Random(9)
    before = sum(v.anomalous for v in old.score(drifted_traffic(1000, rng)))
    assert before > 900, "the old thresholds flag the new normal almost everywhere"

    thresholds = new_thresholds(old, drifted_traffic(3000, rng))
    version = save_version(tmp_path, old, thresholds, {"from": "v1"})

    new = ServingModels(tmp_path)  # latest -> the recalibrated copy
    assert new.version == version != "v1"
    after = sum(v.anomalous for v in new.score(drifted_traffic(1000, rng)))
    assert after <= 10, "about 1 in 1,000 of the new normal flagged"
    assert new.score([request(HIT, shop_ms=400)])[0].timing_flag, "real slowness still flagged"
    meta = json.loads((tmp_path / version / "meta.json").read_text())
    assert meta["recalibrated"] == {"from": "v1"}

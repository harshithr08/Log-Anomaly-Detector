"""
train.py — train the models, set their thresholds, grade them, save a version.

    python -m logad.models.train                    # data/shop
    python -m logad.models.train --data data/shop_dev

Every detector (baselines included) is fitted on clean training sessions,
gets its threshold from clean validation sessions (at most 1 in 1,000 normal
requests flagged) and is graded on the fault period. The two production
models are the LSTM (event order) and the timing detector; the Isolation
Forest is graded too, as the alternative that was rejected. Results go to
eval/results/models.md, and the trained models to a new folder:

    models/<version>/
        lstm.pt           LSTM weights (PyTorch): event order
        lstm.onnx         the same network for ONNX Runtime (live scoring)
        vocab.json        template text <-> number
        scaler.json       timing: per path, median and spread of each time
        thresholds.json   one threshold per model
        metrics.json      test results
        meta.json         when, from what data, with which settings
    models/latest         name of the newest version
"""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import torch

from logad.models.dataset import DATA_DIR, load_splits
from logad.models.evaluate import combine, detectors, grade, write_report
from logad.models.iforest import IForestDetector
from logad.models.lstm import LSTMDetector
from logad.models.metrics import FALSE_ALARM_BUDGET
from logad.models.timing import TimingDetector

MODELS_DIR = Path("models")


def save(version_dir: Path, lstm, timing, thresholds: dict, metrics: list, meta: dict):
    version_dir.mkdir(parents=True)
    torch.save(lstm.model.state_dict(), version_dir / "lstm.pt")
    lstm.export_onnx(version_dir / "lstm.onnx")  # for live scoring without PyTorch

    def dump(name, data):
        (version_dir / name).write_text(json.dumps(data, indent=2), encoding="utf-8")

    dump("vocab.json", lstm.vocab.templates)
    dump("scaler.json", timing.scaler.to_dict())
    dump("thresholds.json", thresholds)
    dump("metrics.json", metrics)
    dump("meta.json", meta)
    (version_dir.parent / "latest").write_text(version_dir.name, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Train, grade and save the models.")
    parser.add_argument("--data", type=Path, default=DATA_DIR)
    args = parser.parse_args()
    splits = load_splits(args.data)
    print(f"train {len(splits.train):,}  val {len(splits.val):,}  test {len(splits.test):,}")

    rows = []
    for detector in [*detectors(), IForestDetector()]:  # baselines + rejected alternative
        print(f"  {detector.name}...", flush=True)
        rows.append(grade(detector, splits).row)

    lstm, timing = LSTMDetector(), TimingDetector()
    graded = {}
    for name, detector in (("lstm", lstm), ("timing", timing)):
        print(f"  {detector.name}...", flush=True)
        graded[name] = grade(detector, splits)
        rows.append(graded[name].row)
    rows.append(combine("LSTM + timing (either)", list(graded.values()), splits))
    print(write_report(rows, splits))

    version = datetime.now(UTC).strftime("v%Y%m%d-%H%M%S")
    save(
        MODELS_DIR / version,
        lstm,
        timing,
        thresholds={
            "false_alarm_budget": FALSE_ALARM_BUDGET,
            "lstm": graded["lstm"].threshold,
            "timing": graded["timing"].threshold,
        },
        metrics=rows,
        meta={
            "version": version,
            "created": datetime.now(UTC).isoformat(),
            "data": {
                "dir": str(args.data),
                "train": len(splits.train),
                "val": len(splits.val),
                "test": len(splits.test),
                "test_broken": splits.info["test_broken"],
            },
            "lstm": {"epochs": lstm.epochs, "vocab": len(lstm.vocab)},
            "timing": {"paths": len(timing.scaler.paths)},
        },
    )
    print(f"Saved models/{version}")


if __name__ == "__main__":
    main()

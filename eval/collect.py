"""
collect.py — record a labelled dataset from the live shop.

    1. clean period: normal traffic only        -> training + threshold data
    2. fault period: a fixed schedule of faults -> test data
The fault injector records every request it breaks (runtime/ground_truth/),
so the test data has exact labels. The schedule and the time of each period
are saved to runtime/collection.json.

Then export everything to data/shop/ (sessions + labels):
    python -m logad.models.dataset export

Usage:
    python eval/collect.py --fresh                 # ~22 minutes
    python eval/collect.py --clean-minutes 2 --fault-minutes 2 --fresh   # quick try
"""

import argparse
import asyncio
import json
import time
from pathlib import Path

import httpx
from stack import compose, start_stack, wait_until_drained

from shop.settings import ShopSettings

# (service, fault kind, share of requests broken, extra delay for 'slow')
SCHEDULE = [
    ("payment", "skip", 0.5, 0),
    ("inventory", "slow", 1.0, 600),
    ("shipping", "error", 0.5, 0),
    ("shop", "skip", 0.5, 0),
    ("inventory", "skip", 0.5, 0),
    ("payment", "slow", 1.0, 800),
    ("payment", "error", 0.3, 0),
    ("shipping", "slow", 1.0, 500),
    ("shipping", "skip", 0.5, 0),
    ("shop", "slow", 1.0, 400),
]
FAULT_ON_S = 40
FAULT_GAP_S = 20
OUT = Path("runtime/collection.json")


def run_faults(minutes: float) -> list[dict]:
    """Go through the schedule (repeating it if there is time) and record when
    each fault was on."""
    shop = ShopSettings()
    end = time.time() + minutes * 60
    applied = []
    with httpx.Client(timeout=5) as client:
        step = 0
        while time.time() + FAULT_ON_S <= end:
            service, kind, probability, delay_ms = SCHEDULE[step % len(SCHEDULE)]
            url = shop.url_for(service) + "/admin/fault"
            spec = {"kind": kind, "probability": probability, "delay_ms": delay_ms}
            fault_id = client.post(url, json=spec).raise_for_status().json()["fault_id"]
            start = time.time()
            print(f"  {service:<9} {kind:<5} p={probability} for {FAULT_ON_S}s", flush=True)
            time.sleep(FAULT_ON_S)
            client.delete(url).raise_for_status()
            applied.append(
                {
                    "fault_id": fault_id,
                    "service": service,
                    **spec,
                    "start": start,
                    "end": time.time(),
                }
            )
            time.sleep(FAULT_GAP_S)
            step += 1
    return applied


def main():
    parser = argparse.ArgumentParser(description="Record a labelled dataset from the shop.")
    parser.add_argument("--clean-minutes", type=float, default=12)
    parser.add_argument("--fault-minutes", type=float, default=10)
    parser.add_argument("--rate", type=float, default=20, help="shop requests per second")
    parser.add_argument("--fresh", action="store_true", help="start from empty data")
    args = parser.parse_args()

    start_stack(args.rate, args.fresh)
    clean_start = time.time()
    print(f"Clean traffic for {args.clean_minutes} min...", flush=True)
    time.sleep(args.clean_minutes * 60)
    clean_end = time.time()

    print(f"Fault schedule for {args.fault_minutes} min:", flush=True)
    faults = run_faults(args.fault_minutes)
    fault_end = time.time()
    compose("stop", "traffic")

    print("Traffic stopped; waiting for the pipeline to drain...", flush=True)
    asyncio.run(wait_until_drained(timeout=300))
    OUT.write_text(
        json.dumps(
            {
                "rate": args.rate,
                "clean": [clean_start, clean_end],
                "faults_period": [clean_end, fault_end],
                "faults": faults,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Done. Schedule saved to {OUT}. Next: python -m logad.models.dataset export")


if __name__ == "__main__":
    main()

"""
faults.py — break the live shop again and again; did an alert follow?

    1. clean traffic for a while: every alert raised there is a false alarm
    2. 22 faults, one at a time (every service x slow/error/skip/crash, plus
       weaker versions that are harder to notice), each on for 45 s, then a
       gap until the alerts have resolved
    3. per fault: detected? the right service? time to detect? which
       detectors fired? And per detector alone (the ablation): would it have
       been enough, by the same rule the alert manager uses?

Writes eval/results/faults.md.

Usage:
    python eval/faults.py --fresh           # ~70 minutes
"""

import argparse
import asyncio
import random
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
from sqlalchemy import text
from stack import compose, start_stack

from logad.common.config import get_settings
from logad.common.db import make_engine
from shop.settings import ShopSettings

RESULTS = Path("eval/results/faults.md")
ON_S = 45
DETECT_AFTER_END_S = 60  # an alert raised this soon after the fault still counts
MIN_GAP_S = DETECT_AFTER_END_S  # so one fault's detection window never overlaps the next
MAX_GAP_S = 150
# Strong faults: every service x kind. Weak ones: harder to notice.
FAULTS = [
    (service, kind, p, delay)
    for service in ("shop", "inventory", "payment", "shipping")
    for kind, p, delay in (
        ("slow", 1.0, 1500),
        ("error", 0.5, 0),
        ("skip", 0.5, 0),
        ("crash", 1.0, 0),
    )
] + [
    ("payment", "skip", 0.2, 0),
    ("inventory", "error", 0.2, 0),
    ("shop", "slow", 1.0, 300),
    ("payment", "slow", 1.0, 300),
    ("shipping", "skip", 0.3, 0),
    ("inventory", "slow", 1.0, 300),
]


def _now() -> datetime:
    return datetime.now(UTC)


async def open_alerts() -> int:
    async with httpx.AsyncClient(timeout=10) as client:
        return len(
            (await client.get("http://127.0.0.1:8200/api/alerts?status=open")).json()["items"]
        )


def run_schedule(clean_s: float, seed: int) -> tuple[datetime, datetime, list[dict]]:
    shop = ShopSettings()
    order = FAULTS[:]
    random.Random(seed).shuffle(order)
    clean_start = _now()
    print(f"Clean traffic for {clean_s / 60:.0f} min...", flush=True)
    time.sleep(clean_s)
    clean_end = _now()
    done = []
    with httpx.Client(timeout=10) as client:
        for i, (service, kind, p, delay) in enumerate(order, 1):
            url = shop.url_for(service) + "/admin/fault"
            start = _now()
            client.post(url, json={"kind": kind, "probability": p, "delay_ms": delay})
            time.sleep(ON_S)
            if kind != "crash":  # a crash ends by itself (Docker restarts it)
                client.delete(url)
            end = _now()
            done.append(
                {
                    "service": service,
                    "kind": kind,
                    "p": p,
                    "delay": delay,
                    "start": start,
                    "end": end,
                }
            )
            print(
                f"  {i:2d}/{len(order)} {service:<9} {kind:<5} p={p} "
                f"{f'+{delay}ms' if kind == 'slow' else ''}",
                flush=True,
            )
            waited = 0
            while waited < MAX_GAP_S and (waited < MIN_GAP_S or asyncio.run(open_alerts())):
                time.sleep(5)
                waited += 5
    return clean_start, clean_end, done


async def analyse(clean_start, clean_end, faults) -> dict:
    engine = make_engine(get_settings().database_url)
    async with engine.connect() as conn:
        alerts = (
            await conn.execute(
                text("select service, raised_at, detectors from alerts where raised_at >= :t"),
                {"t": clean_start},
            )
        ).all()
        for i, f in enumerate(faults):
            window = {"a": f["start"], "b": f["end"]}
            # up to DETECT_AFTER_END_S after the fault, but never into the next one
            until = f["end"].timestamp() + DETECT_AFTER_END_S
            if i + 1 < len(faults):
                until = min(until, faults[i + 1]["start"].timestamp())
            f["until"] = until
            found = [
                a for a in alerts if f["start"].timestamp() <= a.raised_at.timestamp() <= until
            ]
            f["detected"] = bool(found)
            f["right_service"] = any(a.service == f["service"] for a in found)
            f["ttd"] = (
                (min(a.raised_at for a in found) - f["start"]).total_seconds() if found else None
            )
            f["alert_detectors"] = sorted({d for a in found for d in a.detectors})
            # Ablation: would each detector alone have been enough (same rule as
            # the alert manager: 5 flagged requests, or any rate anomaly)?
            for detector in ("lstm", "timing"):
                n = (
                    await conn.execute(
                        text(
                            "select count(*) from scores sc join sessions s using (session_id)"
                            " where :d = any(sc.detectors) and s.started_at between :a and :b"
                        ),
                        {"d": detector, **window},
                    )
                ).scalar()
                f[detector] = n >= 5
            n = (
                await conn.execute(
                    text(
                        "select count(*) from rate_anomalies where window_start between :a and :b"
                    ),
                    {"a": f["start"], "b": f["end"]},
                )
            ).scalar()
            f["rate"] = n >= 1
    await engine.dispose()

    def in_fault(t):
        return any(f["start"].timestamp() <= t.timestamp() <= f["until"] for f in faults)

    false = [a for a in alerts if not in_fault(a.raised_at)]
    # fault-free time: the clean period, plus the gaps after each detection window
    quiet_s = (clean_end - clean_start).total_seconds() + sum(
        max(0.0, b["start"].timestamp() - a["until"])
        for a, b in zip(faults, faults[1:], strict=False)
    )
    return {"faults": faults, "false_alarms": len(false), "quiet_hours": quiet_s / 3600}


def report(result: dict, rate: float) -> str:
    faults = result["faults"]
    detected = [f for f in faults if f["detected"]]
    ttds = [f["ttd"] for f in detected]
    kinds = ("slow", "error", "skip", "crash")
    lines = [
        "# Fault evaluation",
        "",
        "Generated by `python eval/faults.py`. Do not edit by hand.",
        "",
        f"Run at {_now():%Y-%m-%d %H:%M} UTC: live shop at ~{rate:.0f} requests/s, "
        f"{len(faults)} faults one at a time, each on for {ON_S} s (a crash: the process is",
        "killed once and Docker restarts it). Detected = an alert raised during the fault",
        f"or up to {DETECT_AFTER_END_S} s after it.",
        "",
        "## Summary",
        "",
        "| | |",
        "|---|---|",
        f"| Faults detected | **{len(detected)} / {len(faults)}** |",
        f"| ... blaming the right service | {sum(f['right_service'] for f in faults)} / "
        f"{len(faults)} |",
        f"| Time to detect (median / worst) | **{statistics.median(ttds):.0f} s** / "
        f"{max(ttds):.0f} s |"
        if ttds
        else "| Time to detect | – |",
        f"| False alarms | {result['false_alarms']} in {result['quiet_hours']:.2f} h of fault-free "
        f"traffic ({result['false_alarms'] / max(result['quiet_hours'], 1e-9):.1f} per hour) |",
        "",
        "## Which detector catches what (ablation)",
        "",
        "Would one detector alone have raised the alert (same rule: 5 flagged requests",
        "within the fault, or any rate anomaly)?",
        "",
        "| Detectors | " + " | ".join(kinds) + " | All |",
        "|---|" + "---|" * (len(kinds) + 1),
    ]
    subsets = [
        ("LSTM only", ["lstm"]),
        ("Timing only", ["timing"]),
        ("Rate only", ["rate"]),
        ("LSTM + timing", ["lstm", "timing"]),
        ("All three", ["lstm", "timing", "rate"]),
    ]
    for name, members in subsets:
        cells = []
        for kind in (*kinds, None):
            group = [f for f in faults if kind is None or f["kind"] == kind]
            caught = sum(any(f[m] for m in members) for f in group)
            cells.append(f"{caught}/{len(group)}")
        lines.append(f"| {name} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "## Every fault",
        "",
        "| # | Fault | Detected | Right service | Time to detect | Alert detectors |",
        "|---|---|---|---|---|---|",
    ]
    for i, f in enumerate(faults, 1):
        what = f"{f['service']} {f['kind']}"
        if f["kind"] == "slow":
            what += f" +{f['delay']} ms"
        elif f["kind"] in ("error", "skip"):
            what += f" ({f['p']:.0%} of requests)"
        ttd = "–" if f["ttd"] is None else f"{f['ttd']:.0f} s"
        detected = "yes" if f["detected"] else "**no**"
        right = "yes" if f["right_service"] else "no"
        used = ", ".join(f["alert_detectors"]) or "–"
        lines.append(f"| {i} | {what} | {detected} | {right} | {ttd} | {used} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Break the live shop; measure the alerts.")
    parser.add_argument("--clean-minutes", type=float, default=20)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--fresh", action="store_true")
    args = parser.parse_args()

    start_stack(args.rate, args.fresh)
    time.sleep(90)  # the rate detector warms up for a minute
    clean_start, clean_end, faults = run_schedule(args.clean_minutes * 60, args.seed)
    compose("stop", "traffic")
    result = asyncio.run(analyse(clean_start, clean_end, faults))
    RESULTS.write_text(report(result, args.rate), encoding="utf-8", newline="\n")
    print(RESULTS.read_text(encoding="utf-8").encode("ascii", "replace").decode())


if __name__ == "__main__":
    main()

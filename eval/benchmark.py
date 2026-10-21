"""
benchmark.py — how fast is the pipeline, how quick, and where is the bottleneck?

The shop's own traffic is paused; loadgen.py replays real recorded sessions
straight into the ingest API (shipper not included).

    latency     a steady realistic load: p50/p99 of "last line of a request
                written -> request scored" (includes the 2 s grace period a
                session waits for late lines)
    throughput  as fast as the pipeline accepts, per worker configuration:
                log lines fully processed per second (inside saved sessions),
                and which stage's queue keeps growing: the bottleneck
    onnx        scoring with ONNX Runtime vs PyTorch, one request at a time
                and in batches

Writes eval/results/benchmark.md and eval/results/scaling.svg.

Usage:
    python eval/benchmark.py --configs "parser=2,scoring=1" "parser=4,scoring=2"
"""

import argparse
import asyncio
import json
import os
import platform
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
from loadgen import blast, load_sessions
from sqlalchemy import text
from stack import compose, start_stack

from logad.common.config import get_settings
from logad.common.db import make_engine

INGEST = "http://127.0.0.1:8100"
API = "http://127.0.0.1:8200"
RESULTS = Path("eval/results/benchmark.md")
CHART = Path("eval/results/scaling.svg")


def hardware() -> str:
    cpu = platform.processor()
    if platform.system() == "Windows":
        cpu = (
            subprocess.run(
                ["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"],
                capture_output=True,
                text=True,
                check=False,
            ).stdout.strip()
            or cpu
        )
    info = subprocess.run(
        ["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.split()
    docker = f"{info[0]} CPUs, {int(info[1]) / 2**30:.1f} GiB" if len(info) == 2 else "?"
    return f"{cpu}; {os.cpu_count()} logical cores; Docker: {docker}; {platform.system()}"


async def _query(sql: str, **params):
    engine = make_engine(get_settings().database_url)
    async with engine.connect() as conn:
        row = (await conn.execute(text(sql), params)).one()
    await engine.dispose()
    return row


async def _stats() -> dict:
    import httpx

    async with httpx.AsyncClient(timeout=10) as client:
        return (await client.get(f"{API}/api/stats")).json()


async def run_latency(sessions, rate: float, seconds: float) -> dict:
    start = datetime.now(UTC)
    sent = await blast(INGEST, rate, seconds, sessions)
    await asyncio.sleep(15)  # let the last sessions close (grace) and be scored
    row = await _query(
        "select count(*) as n, percentile_cont(array[0.5, 0.99]) within group"
        " (order by extract(epoch from sc.scored_at - s.ended_at)) as p"
        " from scores sc join sessions s using (session_id) where s.created_at >= :start",
        start=start,
    )
    return {
        "rate": rate,
        "lines": sent["lines"],
        "sessions": row.n,
        "p50_s": row.p[0],
        "p99_s": row.p[1],
    }


async def run_throughput(sessions, seconds: float, warmup: float = 30) -> dict:
    """Blast for warmup + seconds; measure processed lines/s over the last part."""
    samples = []

    async def sample():
        while True:
            samples.append((time.monotonic(), (await _stats())["backlog"]))
            await asyncio.sleep(2)

    sampler = asyncio.create_task(sample())
    blaster = asyncio.create_task(blast(INGEST, None, warmup + seconds, sessions))
    await asyncio.sleep(warmup)
    t0 = datetime.now(UTC)
    await asyncio.sleep(seconds)
    t1 = datetime.now(UTC)
    sent = await blaster
    sampler.cancel()
    row = await _query(
        "select coalesce(sum(event_count), 0) as lines, count(*) as n from sessions"
        " where created_at >= :a and created_at < :b",
        a=t0,
        b=t1,
    )
    measured = [b for t, b in samples if t >= samples[0][0] + warmup]
    growth = {k: measured[-1][k] - measured[0][k] for k in measured[0]} if measured else {}
    return {
        "lines_per_s": row.lines / seconds,
        "sessions_per_s": row.n / seconds,
        "accepted_per_s": sent["lines"] / sent["seconds"],
        "backlog_end": measured[-1] if measured else {},
        "backlog_growth": growth,
        "bottleneck": max(growth, key=growth.get)
        if growth and max(growth.values()) > 1000
        else "none (kept up)",
    }


def onnx_vs_torch(n: int = 2000) -> list[dict]:
    """Score the same recorded sessions with PyTorch and with ONNX Runtime."""
    import torch

    from logad.models.dataset import load
    from logad.models.lstm import LSTMDetector, NextEventLSTM
    from logad.models.serving import ServingModels

    sessions = load()[:n]
    served = ServingModels(Path("models"))
    folder = Path("models") / served.version
    lstm = LSTMDetector()
    lstm.vocab = served.vocab
    lstm.model = NextEventLSTM(len(served.vocab))
    lstm.model.load_state_dict(torch.load(folder / "lstm.pt"))
    lstm.model.eval()
    torch.set_num_threads(1)  # like a single scoring worker

    def lstm_onnx(batch):
        from logad.models.sequences import windows

        x, _, _ = windows(served.vocab, batch)
        served.lstm_logits(x)

    def lstm_torch(batch):
        from logad.models.sequences import windows

        x, _, _ = windows(served.vocab, batch)
        lstm.logits(x)

    results = []
    for name, fn in (("PyTorch", lstm_torch), ("ONNX Runtime", lstm_onnx)):
        fn(sessions[:50])  # warm up
        single = []
        for s in sessions[:300]:
            t = time.perf_counter()
            fn([s])
            single.append((time.perf_counter() - t) * 1000)
        t = time.perf_counter()
        for i in range(0, len(sessions), 200):
            fn(sessions[i : i + 200])
        batched = len(sessions) / (time.perf_counter() - t)
        results.append(
            {
                "engine": name,
                "p50_ms": float(np.percentile(single, 50)),
                "p99_ms": float(np.percentile(single, 99)),
                "batched_per_s": batched,
            }
        )
    return results


def chart(rows: list[dict]) -> str:
    """A small bar chart (SVG) of throughput per configuration."""
    w, bar_h, gap, left = 640, 28, 14, 190
    h = 50 + len(rows) * (bar_h + gap)
    top = max(r["lines_per_s"] for r in rows) or 1
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        'font-family="system-ui, sans-serif" font-size="13">',
        '<rect width="100%" height="100%" fill="#fcfcfb"/>',
        '<text x="16" y="24" font-weight="600" fill="#0b0b0b">'
        "Log lines fully processed per second</text>",
    ]
    for i, r in enumerate(rows):
        y = 40 + i * (bar_h + gap)
        length = (w - left - 90) * r["lines_per_s"] / top
        parts += [
            f'<text x="16" y="{y + 19}" fill="#52514e">{r["config"]}</text>',
            f'<rect x="{left}" y="{y}" width="{length:.0f}" height="{bar_h}" '
            'rx="4" fill="#2a78d6"/>',
            f'<text x="{left + length + 8:.0f}" y="{y + 19}" fill="#0b0b0b">'
            f"{r['lines_per_s']:,.0f}</text>",
        ]
    return "\n".join(parts + ["</svg>"]) + "\n"


def report(hw, latency, throughput, onnx) -> str:
    lines = [
        "# Benchmark",
        "",
        "Generated by `python eval/benchmark.py`. Do not edit by hand.",
        "",
        f"Run at {datetime.now(UTC):%Y-%m-%d %H:%M} UTC on: {hw}.",
        "Everything (shop, pipeline, Redis, Postgres) runs on this one machine.",
        "",
        "## Latency at a steady load",
        "",
        "Last line of a request written → request scored. Includes the 2 s a session",
        "waits for late lines from other services.",
        "",
        "| Load (lines/s) | Requests scored | p50 | p99 |",
        "|---|---|---|---|",
        f"| {latency['rate']:.0f} | {latency['sessions']:,} | {latency['p50_s']:.2f} s | "
        f"{latency['p99_s']:.2f} s |",
        "",
        "## Throughput",
        "",
        "As fast as the pipeline accepts (backpressure on), after a 30 s warm-up.",
        "",
        "| Workers | Lines/s processed | Requests/s | Growing queue (bottleneck) |",
        "|---|---|---|---|",
    ]
    for r in throughput:
        lines.append(
            f"| {r['config']} | {r['lines_per_s']:,.0f} | {r['sessions_per_s']:,.1f} | "
            f"{r['bottleneck']} |"
        )
    lines += [
        "",
        "![throughput](scaling.svg)",
        "",
        "## Scoring: ONNX Runtime vs PyTorch",
        "",
        "LSTM on recorded requests, one CPU thread.",
        "",
        "| Engine | One request p50 | One request p99 | Batches of 200 |",
        "|---|---|---|---|",
    ]
    for r in onnx:
        lines.append(
            f"| {r['engine']} | {r['p50_ms']:.2f} ms | {r['p99_ms']:.2f} ms | "
            f"{r['batched_per_s']:,.0f} requests/s |"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Benchmark the live pipeline.")
    parser.add_argument("--configs", nargs="+", default=["parser=2,scoring=1"])
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--latency-rate", type=float, default=300)
    parser.add_argument("--fresh", action="store_true")
    args = parser.parse_args()

    hw = hardware()
    sessions = load_sessions()
    start_stack(10, args.fresh)
    compose("stop", "traffic")
    time.sleep(10)

    print("Latency...", flush=True)
    latency = asyncio.run(run_latency(sessions, args.latency_rate, args.seconds))
    print(f"  {latency}", flush=True)
    throughput = []
    for config in args.configs:
        scales = dict(item.split("=") for item in config.split(","))
        flags = [f for k, v in scales.items() for f in ("--scale", f"{k}={v}")]
        compose("up", "-d", "--no-deps", *flags, *scales)
        time.sleep(15)
        print(f"Throughput with {config}...", flush=True)
        result = asyncio.run(run_throughput(sessions, args.seconds))
        result["config"] = ", ".join(f"{v} {k}" for k, v in scales.items())
        print(f"  {json.dumps(result)}", flush=True)
        throughput.append(result)
        time.sleep(30)  # drain before the next configuration
    print("ONNX vs PyTorch...", flush=True)
    onnx = onnx_vs_torch()

    RESULTS.write_text(report(hw, latency, throughput, onnx), encoding="utf-8", newline="\n")
    CHART.write_text(chart(throughput), encoding="utf-8", newline="\n")
    print(RESULTS.read_text(encoding="utf-8").encode("ascii", "replace").decode())


if __name__ == "__main__":
    main()

"""
faults.py — break a service on purpose, and record exactly what we broke.

Fault kinds:
    slow   add a delay to requests
    error  fail requests with a 500 error
    skip   silently skip one step of the service's work (each service decides which)
    crash  kill the process; Docker restarts it

Ground truth is an append-only JSON-lines file per service:
    {"event": "fault_start", "fault_id": ..., "kind": "slow", ...}
    {"event": "affected", "fault_id": ..., "request_id": ...}   # one per broken request
    {"event": "fault_end", "fault_id": ..., "reason": "fixed" | "restarted"}

Only the evaluation reads these files — never the pipeline. They are the answer key.
"""

import asyncio
import json
import os
import random
import time
import uuid
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel, Field

from shop.logs import request_id_var

FaultKind = Literal["slow", "error", "skip", "crash"]


class FaultSpec(BaseModel):
    kind: FaultKind
    probability: float = Field(1.0, ge=0.0, le=1.0, description="share of requests affected")
    delay_ms: int = Field(2000, ge=0, description="extra delay for 'slow'")


class GroundTruth:
    """Append-only record of what we broke."""

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event: str, **fields) -> None:
        line = json.dumps({"ts": time.time(), "event": event, **fields})
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    def events(self) -> list[dict]:
        if not self.path.exists():
            return []
        with self.path.open(encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]


class FaultInjector:
    """Holds the service's active fault (at most one) and records its effects."""

    def __init__(self, service: str, ground_truth: GroundTruth, rng: random.Random | None = None):
        self.service = service
        self.ground_truth = ground_truth
        self.rng = rng or random.Random()
        self.active: FaultSpec | None = None
        self.fault_id: str | None = None

    def start(self, spec: FaultSpec) -> str:
        if self.active:
            self.stop()
        self.active, self.fault_id = spec, uuid.uuid4().hex[:12]
        self.ground_truth.write(
            "fault_start", fault_id=self.fault_id, service=self.service, **spec.model_dump()
        )
        return self.fault_id

    def stop(self, reason: str = "fixed") -> None:
        if self.active:
            self.ground_truth.write(
                "fault_end", fault_id=self.fault_id, service=self.service, reason=reason
            )
        self.active, self.fault_id = None, None

    def hits(self, kind: FaultKind) -> bool:
        """Should the current request be broken by an active fault of this kind?
        If yes, the request ID is recorded as affected."""
        if self.active is None or self.active.kind != kind:
            return False
        if self.rng.random() >= self.active.probability:
            return False
        self.ground_truth.write(
            "affected",
            fault_id=self.fault_id,
            service=self.service,
            kind=kind,
            request_id=request_id_var.get(),
        )
        return True

    async def maybe_delay(self) -> None:
        if self.hits("slow"):
            await asyncio.sleep(self.active.delay_ms / 1000 * self.rng.uniform(0.8, 1.2))

    def close_open_faults(self) -> None:
        """Faults live in memory, so a restart ends them. Called at startup to
        record the end of any fault (e.g. a crash) that was still open."""
        open_faults = {}
        for event in self.ground_truth.events():
            if event["event"] == "fault_start":
                open_faults[event["fault_id"]] = event
            elif event["event"] == "fault_end":
                open_faults.pop(event["fault_id"], None)
        for fault_id in open_faults:
            self.ground_truth.write(
                "fault_end", fault_id=fault_id, service=self.service, reason="restarted"
            )


async def _crash_soon() -> None:
    await asyncio.sleep(0.2)  # let the HTTP response go out first
    os._exit(1)


def admin_router(injector: FaultInjector) -> APIRouter:
    router = APIRouter(prefix="/admin")

    def status():
        return {
            "service": injector.service,
            "fault_id": injector.fault_id,
            "fault": injector.active.model_dump() if injector.active else None,
        }

    @router.get("/fault")
    def get_fault():
        return status()

    @router.post("/fault")
    def start_fault(spec: FaultSpec, background_tasks: BackgroundTasks):
        injector.start(spec)
        if spec.kind == "crash":
            background_tasks.add_task(_crash_soon)
        return status()

    @router.delete("/fault")
    def stop_fault():
        injector.stop()
        return status()

    return router

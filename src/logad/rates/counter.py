"""
counter.py — count each event type per time window and compare with its usual
level. Pure logic (no Redis, no database), so it is easy to test.

Windows follow the logs' own timestamps (event time). A window closes once
events more than `lateness_s` past its end have arrived, so a stalled pipeline
just pauses the counting instead of looking like a traffic drop.

Per event type the usual level is a moving average and spread (EWMA) of past
windows. A window is anomalous for an event type when
    spike: count is far above usual  (z > z_limit and at least min_count), or
    drop:  a busy event type is far below usual (z < -z_limit).
A brand-new event type (e.g. a new error) has usual level 0, so a burst of it
is a spike. Nothing is reported during the first `warmup_windows`.

While an event type is anomalous its usual level is frozen, so an incident
stays flagged for as long as it lasts. If it stays anomalous for
`accept_after_windows` (3 minutes), the new level becomes the new normal, so a
lasting change (e.g. a new release logging more) doesn't alert forever.
"""

import math
from dataclasses import dataclass, field


@dataclass
class Baseline:
    mean: float = 0.0
    var: float = 0.0
    anomalous_streak: int = 0  # consecutive anomalous windows

    def update(self, count: float, alpha: float) -> None:
        diff = count - self.mean
        self.mean += alpha * diff
        self.var = (1 - alpha) * (self.var + alpha * diff * diff)


@dataclass
class RateAnomaly:
    window_start: float
    template_id: int
    count: int
    expected: float
    z: float
    direction: str  # "spike" or "drop"


@dataclass
class Window:
    start: float
    counts: dict[int, int] = field(default_factory=dict)
    line_ids: set[str] = field(default_factory=set)
    entry_ids: list[tuple[str, str]] = field(default_factory=list)  # to ack once saved


class RateCounter:
    def __init__(
        self,
        window_s: float = 10.0,
        lateness_s: float = 5.0,
        alpha: float = 0.1,
        z_limit: float = 6.0,
        min_count: int = 5,
        min_busy: float = 20.0,
        warmup_windows: int = 6,
        accept_after_windows: int = 18,
    ):
        self.window_s = window_s
        self.lateness_s = lateness_s
        self.alpha = alpha
        self.z_limit = z_limit
        self.min_count = min_count
        self.min_busy = min_busy  # a "drop" only counts for types usually this busy
        self.warmup_windows = warmup_windows
        self.accept_after_windows = accept_after_windows
        self.windows: dict[float, Window] = {}
        self.baselines: dict[int, Baseline] = {}
        self.watermark = 0.0
        self.closed_until = 0.0  # windows starting before this are closed
        self.closed_count = 0
        self.late = 0

    def add(self, ts: float, template_id: int, line_id: str, entry: tuple[str, str]) -> str:
        """Count one event. Returns "counted", "duplicate" or "late"."""
        start = ts - ts % self.window_s
        self.watermark = max(self.watermark, ts)
        if start < self.closed_until:
            self.late += 1
            return "late"
        window = self.windows.setdefault(start, Window(start))
        window.entry_ids.append(entry)
        if line_id in window.line_ids:
            return "duplicate"
        window.line_ids.add(line_id)
        window.counts[template_id] = window.counts.get(template_id, 0) + 1
        return "counted"

    def close_ready(self) -> list[tuple[Window, list[RateAnomaly]]]:
        """Close every window the watermark has moved past (plus lateness), in
        order, judging each against the baselines and then updating them."""
        closed = []
        for start in sorted(self.windows):
            if start + self.window_s + self.lateness_s > self.watermark:
                break
            window = self.windows.pop(start)
            closed.append((window, self._judge(window)))
            self.closed_until = start + self.window_s
        return closed

    def _judge(self, window: Window) -> list[RateAnomaly]:
        anomalies = []
        warm = self.closed_count >= self.warmup_windows
        for template_id in set(window.counts) | set(self.baselines):
            count = window.counts.get(template_id, 0)
            if template_id not in self.baselines:
                # Seen while warming up: start from its first count. Seen only
                # later: start from 0, so a brand-new event type (e.g. a new
                # error) stands out.
                self.baselines[template_id] = Baseline(mean=0.0 if warm else float(count))
            base = self.baselines[template_id]
            spread = math.sqrt(base.var + max(base.mean, 1.0))  # Poisson-like floor
            z = (count - base.mean) / spread
            direction = None
            if warm and z > self.z_limit and count >= self.min_count:
                direction = "spike"
            elif warm and z < -self.z_limit and base.mean >= self.min_busy:
                direction = "drop"
            if direction:
                anomalies.append(
                    RateAnomaly(window.start, template_id, count, base.mean, z, direction)
                )
            if not direction:
                base.anomalous_streak = 0
                base.update(count, self.alpha)
            else:
                base.anomalous_streak += 1  # usual level frozen during the incident
                if base.anomalous_streak >= self.accept_after_windows:
                    self.baselines[template_id] = Baseline(mean=float(count))  # new normal
        self.closed_count += 1
        return anomalies

"""Timing primitives and summary statistics.

Latency distributions here are long-tailed, so the mean is close to useless on
its own; percentiles are what get reported. Sample counts are always carried
alongside so a p99 computed from 5 samples can be recognized as noise.
"""

from __future__ import annotations

import math
import statistics
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator


@contextmanager
def timer() -> Iterator["Elapsed"]:
    e = Elapsed()
    start = time.perf_counter()
    try:
        yield e
    finally:
        e.seconds = time.perf_counter() - start


@dataclass
class Elapsed:
    seconds: float = 0.0

    @property
    def ms(self) -> float:
        return self.seconds * 1000.0


@dataclass
class Samples:
    """A collection of per-operation latencies (milliseconds)."""

    name: str
    values_ms: list[float] = field(default_factory=list)

    def add(self, ms: float) -> None:
        self.values_ms.append(ms)

    def __len__(self) -> int:
        return len(self.values_ms)

    def summary(self) -> dict[str, float]:
        return summarize(self.values_ms)


def percentile(sorted_values: list[float], p: float) -> float:
    """Linear-interpolated percentile. `p` in [0, 100]. Input must be sorted."""
    if not sorted_values:
        return math.nan
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (p / 100.0)
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return sorted_values[int(k)]
    return sorted_values[lo] * (hi - k) + sorted_values[hi] * (k - lo)


def summarize(values_ms: list[float]) -> dict[str, float]:
    if not values_ms:
        return {"count": 0}
    s = sorted(values_ms)
    out = {
        "count": len(s),
        "min_ms": s[0],
        "max_ms": s[-1],
        "mean_ms": statistics.fmean(s),
        "median_ms": percentile(s, 50),
        "p90_ms": percentile(s, 90),
        "p95_ms": percentile(s, 95),
        "p99_ms": percentile(s, 99),
        "total_ms": sum(s),
    }
    if len(s) > 1:
        out["stdev_ms"] = statistics.stdev(s)
    return out


def throughput(count: int, seconds: float) -> float:
    """Items per second. Returns inf for a zero-duration measurement rather
    than dividing by zero, so it shows up as obviously wrong instead of crashing."""
    if seconds <= 0:
        return math.inf
    return count / seconds

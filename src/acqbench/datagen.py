"""Deterministic synthetic timeseries generation.

Two properties matter here, and both are correctness issues rather than
niceties:

1. **Determinism.** Every ref and every repetition must see byte-identical
   input, or ref-to-ref deltas measure the data, not the code. Values are
   derived from a seeded generator with a fixed sequence.

2. **Disjoint time windows.** Storage dedups on (ref_uri, ts) with a
   DELETE-then-INSERT. Re-sending the same timestamps therefore measures the
   dedup path and a delete of an ever-growing table, which is not what a write
   benchmark is supposed to report. Each repetition is handed its own window so
   every row is a genuine insert.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

# Fixed origin so runs are comparable across machines and days.
EPOCH = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

# One second between samples within a stream.
SAMPLE_INTERVAL = timedelta(seconds=1)


@dataclass(frozen=True)
class Window:
    """The time slice assigned to one repetition of one workload."""

    start: datetime
    rows_per_stream: int

    @property
    def end(self) -> datetime:
        return self.start + SAMPLE_INTERVAL * self.rows_per_stream


def window_for(repetition: int, rows_per_stream: int, *, slot: int = 0) -> Window:
    """A window that cannot overlap any other (repetition, slot) pair.

    `slot` separates workloads that write to the same streams within a cell
    (e.g. a json and an arrow variant), so neither ever collides with the
    other's timestamps.
    """
    # Stride generously: overlapping windows would silently turn inserts into
    # upserts, so the cost of wasted timestamp space is worth the safety.
    stride = timedelta(days=1)
    offset = stride * (repetition + slot * 1000)
    return Window(start=EPOCH + offset, rows_per_stream=rows_per_stream)


def stream_names(count: int, prefix: str = "s") -> list[str]:
    width = max(4, len(str(count - 1)))
    return [f"{prefix}{i:0{width}d}" for i in range(count)]


def numeric_value(stream_index: int, row_index: int) -> float:
    """A smooth, bounded, deterministic signal.

    Sinusoidal rather than random: compresses realistically (TimescaleDB applies
    compression policies), which random noise would not, and stays reproducible
    without carrying a PRNG's state.
    """
    phase = (stream_index * 0.37) + (row_index * 0.01)
    return round(50.0 + 25.0 * math.sin(phase) + (stream_index % 7), 4)


def text_value(stream_index: int, row_index: int) -> str:
    states = ("ok", "warn", "alarm", "offline")
    return states[(stream_index + row_index) % len(states)]


def generate(
    source_id: str,
    ref_names: list[str],
    window: Window,
    *,
    value_kind: str = "numeric",
) -> dict[tuple[str, str], list[tuple[datetime, Any]]]:
    """Build {(source_id, ref_name): [(ts, value), ...]} for a whole window."""
    out: dict[tuple[str, str], list[tuple[datetime, Any]]] = {}
    make = numeric_value if value_kind == "numeric" else text_value
    for si, ref_name in enumerate(ref_names):
        rows = [
            (window.start + SAMPLE_INTERVAL * ri, make(si, ri))
            for ri in range(window.rows_per_stream)
        ]
        out[(source_id, ref_name)] = rows
    return out


def total_rows(streams: dict[tuple[str, str], list[tuple[datetime, Any]]]) -> int:
    return sum(len(v) for v in streams.values())

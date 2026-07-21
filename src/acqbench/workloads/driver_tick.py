"""Observe benchmark drivers ticking, and report their timing.

This is the workload that exercises what `ums-ray-backend` changes: the same
`[[drivers]]` config starts in-process threads on `main` and Ray actors on the
branch. The benchmark driver (bench_driver.py) is rendered into the cell's
config by the driver count / rows-per-tick / period the cell encodes, so each
grid point is a *cell* (its own server boot) — necessary because `main` has no
dynamic-driver HTTP API; drivers only start from config at boot, which both
branches support.

The drivers self-report each tick to a file. After they have produced enough
ticks, this workload reads those files and computes, for this grid point:

* **tick latency** — how long one tick (generate + insert) takes; warmup
  discarded.
* **scheduling jitter** — gap between consecutive ticks vs the configured
  period.
* **driver-online spread** — the wall-clock span between the first tick of the
  earliest driver and the first tick of the last driver. On the Ray backend,
  actors are created and set up serially, so this is the actor-creation cost;
  on `main` (threads) it is small.
* **in-tick ingest throughput** and **period overrun**.

The grid is assembled across cells by the report layer.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from ..metrics import percentile, summarize
from ..render import bench_tick_dir
from .base import Context, Workload, register


@register("driver_tick")
class DriverTick(Workload):
    """Observe one grid point's benchmark drivers (count/rows/period come from
    the cell config) and summarize their tick timing."""

    def __init__(self, *, n_ticks: dict[str, int] | None = None, **params: Any):
        super().__init__(n_ticks=n_ticks, **params)
        self.n_ticks = {float(k): int(v) for k, v in (n_ticks or {30: 5, 60: 4, 600: 2}).items()}

    def _ticks_for(self, period: float) -> int:
        if period in self.n_ticks:
            return self.n_ticks[period]
        return min(self.n_ticks.values()) if self.n_ticks else 3

    def run(self, ctx: Context) -> dict[str, Any]:
        cfg = ctx.cell.effective_config()
        count = cfg.driver_count
        rows = cfg.bench_rows_per_tick
        period = cfg.driver_interval
        if rows is None or count == 0:
            raise RuntimeError(
                "driver_tick expects a bench-driver cell "
                "(driver_count > 0 and bench_rows_per_tick set); "
                "use matrices/driver-tick.toml"
            )

        n_ticks = self._ticks_for(period)
        out_dir = bench_tick_dir(ctx.workdir)
        names = [f"bench_{i}" for i in range(count)]

        # Drivers autostart at server boot; wait until each has produced n_ticks.
        timeout = (n_ticks + 2) * period + 120.0 + count * 3.0
        self._await_ticks(out_dir, names, n_ticks, timeout)

        return self._summarize(out_dir, names, count, rows, period, n_ticks, ctx)

    def _await_ticks(
        self, out_dir: Path, names: list[str], n_ticks: int, timeout: float
    ) -> None:
        deadline = time.monotonic() + timeout
        poll = min(3.0, max(0.5, timeout / 120))
        while time.monotonic() < deadline:
            if all(_count_lines(out_dir / f"{n}.jsonl") >= n_ticks for n in names):
                return
            time.sleep(poll)

    def _summarize(
        self,
        out_dir: Path,
        names: list[str],
        count: int,
        rows: int,
        period: float,
        n_ticks: int,
        ctx: Context,
    ) -> dict[str, Any]:
        durations: list[float] = []
        gaps: list[float] = []
        jitters: list[float] = []
        tick_ingest_rps: list[float] = []
        first_starts: list[float] = []
        first_ends: list[float] = []
        total_rows = 0
        insert_errors = 0
        ticks_per_driver: list[int] = []

        for name in names:
            recs = _read_jsonl(out_dir / f"{name}.jsonl")
            # Only the first n_ticks per driver, so a slow-polled extra tick on
            # one driver doesn't bias the aggregate toward fast drivers.
            recs = recs[:n_ticks]
            ticks_per_driver.append(len(recs))
            for rec in recs:
                if rec.get("error"):
                    insert_errors += 1
                total_rows += int(rec.get("rows", 0) or 0)
                if rec.get("seq", 0) == 0 and rec.get("wall_start") is not None:
                    first_starts.append(rec["wall_start"])
                    first_ends.append(rec["wall_start"] + rec.get("duration_ms", 0) / 1000.0)
                g = rec.get("gap_ms")
                if g is not None:
                    gaps.append(g)
                    jitters.append(abs(g - period * 1000.0))
                if rec.get("seq", 0) >= 1:  # discard warmup tick
                    d = rec["duration_ms"]
                    durations.append(d)
                    r = int(rec.get("rows", 0) or 0)
                    if d > 0 and r > 0:
                        tick_ingest_rps.append(r / (d / 1000.0))

        tl = summarize(durations)
        # Two "online" views. `spread` is the stagger between drivers *starting*
        # their first tick — pure creation cost (serial actor creation on the
        # Ray backend). `complete` is the honest "whole fleet is productive"
        # time: from the first driver starting to the LAST driver finishing its
        # first tick. They diverge sharply on `main` under load, where all
        # threads start together (~small spread) but the first tick round is a
        # thundering herd on the single insert path (large completion time).
        online_spread = (max(first_starts) - min(first_starts)) if len(first_starts) > 1 else 0.0
        online_complete = (
            (max(first_ends) - min(first_starts)) if first_starts and first_ends else 0.0
        )
        return {
            "drivers": count,
            "rows_per_tick": rows,
            "period_s": period,
            "n_ticks": n_ticks,
            "drivers_reporting": sum(1 for t in ticks_per_driver if t > 0),
            "min_ticks_observed": min(ticks_per_driver) if ticks_per_driver else 0,
            "tick_latency_ms": tl,
            "gap_ms": summarize(gaps),
            "jitter_ms": summarize(jitters),
            "period_overrun": (tl.get("median_ms", 0) / (period * 1000.0)) if durations else 0.0,
            "driver_online_spread_s": online_spread,
            "driver_online_complete_s": online_complete,
            "tick_ingest_rps_median": _median(tick_ingest_rps),
            "tick_ingest_rps_p95": _p95(tick_ingest_rps),
            "total_rows": total_rows,
            "insert_errors": insert_errors,
            **{f"server_{k}": v for k, v in ctx.server.resources().items()},
        }


def _median(vals: list[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    return s[len(s) // 2] if len(s) % 2 else (s[len(s) // 2 - 1] + s[len(s) // 2]) / 2


def _p95(vals: list[float]) -> float:
    return percentile(sorted(vals), 95) if vals else 0.0


def _count_lines(path: Path) -> int:
    try:
        with path.open() as f:
            return sum(1 for _ in f)
    except OSError:
        return 0


def _read_jsonl(path: Path) -> list[dict]:
    out: list[dict] = []
    try:
        with path.open() as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except OSError:
        pass
    return out

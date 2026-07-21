"""A benchmark driver: ingests a controlled number of rows per tick and
self-reports its timing to a file.

This runs *inside acquirium* — in the server process on `main`, in a Ray worker
on `ums-ray-backend` — so it imports acquirium and must not touch the harness's
own packages. The harness references it by file path (never imports it) and
reads the timing files it writes.

Self-reporting to a file (rather than relying on the harness to observe over
HTTP) is what makes the measurement work identically across execution models: a
Ray worker and an in-process loop both just append JSON lines to a path on the
shared filesystem. Each line records one tick's wall-clock start, duration, row
count, and the gap since the previous tick — enough to recover tick latency,
scheduling jitter, and throughput after the fact.

Per-driver config arrives under ``config["driver"]`` — acquirium merges each
``[[drivers]]`` entry's keys there at boot (the same place built-in drivers read
their options). Keys: {source_id, rows_per_tick, streams, out_dir, driver_id,
base_epoch_s}.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone

import pyarrow as pa

from acquirium.Driver import Driver
from acquirium.internals.models import compute_ref_uri

_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


class BenchDriver(Driver):
    """Each tick: generate `rows_per_tick` numeric rows across `streams`
    streams in a fresh time window, insert them, and log the tick's timing."""

    source_id = "acqbench_driver"

    def setup(self) -> None:
        # acquirium merges this driver's [[drivers]] entry keys into
        # config["driver"]; fall back to a top-level block for API-started use.
        cfg = {**self.config.get("acqbench_driver", {}), **self.config.get("driver", {})}
        self.source_id = cfg.get("source_id", "acqbench_driver")
        self.rows_per_tick = int(cfg.get("rows_per_tick", 1000))
        self.n_streams = max(1, min(int(cfg.get("streams", 4)), self.rows_per_tick))
        self.driver_id = cfg.get("driver_id", self.source_id)
        self.out_dir = cfg.get("out_dir", ".")
        # A per-driver base offset so drivers never share timestamps even if two
        # were misconfigured with the same source_id.
        self.base = _EPOCH + timedelta(days=int(cfg.get("base_epoch_s", 0)))

        os.makedirs(self.out_dir, exist_ok=True)
        self.out_path = os.path.join(self.out_dir, f"{self.driver_id}.jsonl")

        self._names = [f"s{i:03d}" for i in range(self.n_streams)]
        self._per_stream = self.rows_per_tick // self.n_streams
        self._row_offset = 0
        self._seq = 0
        self._prev_start: float | None = None

        # Register streams as numeric via an explicit graph insert — the same
        # minimal (sourceId, refName, valueKind) shape the server requires, so
        # values land in the numeric column rather than being coerced to text.
        self.aq.insert_graph(self._registration_ttl(), format="turtle", replace=False)

    def _registration_ttl(self) -> str:
        lines = [
            "@prefix acq: <urn:acquirium#> .",
            "@prefix ref: <https://brickschema.org/schema/Brick/ref#> .",
            "",
        ]
        for name in self._names:
            ref_uri = str(compute_ref_uri(self.source_id, name))
            lines.append(
                f"<{ref_uri}> a ref:TimeseriesReference ;\n"
                f'    acq:sourceId "{self.source_id}" ;\n'
                f'    acq:refName "{name}" ;\n'
                f'    acq:valueKind "numeric" .'
            )
        return "\n".join(lines)

    def tick(self) -> None:
        wall_start = time.time()
        perf0 = time.perf_counter()

        table = self._build_table()
        try:
            resp = self.aq.insert_timeseries_arrow(self.source_id, table)
            rows = int(resp.get("rows_inserted", table.num_rows)) if isinstance(resp, dict) else table.num_rows
            err = None
        except Exception as e:  # noqa: BLE001 — record, keep ticking
            rows = 0
            err = f"{type(e).__name__}: {e}"[:200]

        duration_ms = (time.perf_counter() - perf0) * 1000.0
        gap_ms = None if self._prev_start is None else (wall_start - self._prev_start) * 1000.0
        self._prev_start = wall_start

        record = {
            "seq": self._seq,
            "wall_start": wall_start,
            "duration_ms": round(duration_ms, 3),
            "rows": rows,
            "gap_ms": round(gap_ms, 3) if gap_ms is not None else None,
            "error": err,
        }
        with open(self.out_path, "a") as f:
            f.write(json.dumps(record) + "\n")

        self._row_offset += self._per_stream
        self._seq += 1

    def _build_table(self) -> "pa.Table":
        ref_names: list[str] = []
        tss: list[datetime] = []
        values: list[float] = []
        for si, name in enumerate(self._names):
            for i in range(self._per_stream):
                ref_names.append(name)
                tss.append(self.base + timedelta(seconds=self._row_offset + i))
                values.append(50.0 + si + (i % 100) * 0.1)
        return pa.table(
            {
                "ref_name": pa.array(ref_names, pa.string()),
                "ts": pa.array(tss, pa.timestamp("us", tz="UTC")),
                "value": pa.array(values, pa.float64()),
            }
        )

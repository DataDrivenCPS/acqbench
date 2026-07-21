"""Timeseries read path.

GET /timeseries streams Arrow IPC back to the caller. The interesting knobs are
`read_batch_size` (server-side RecordBatch sizing, swept via the config axis)
and the query shape: full scan vs bounded window vs limit.

An unknown URI returns an empty table rather than an error, so every read here
asserts on the row count. Without that a "fast" read of nothing looks like a
win.
"""

from __future__ import annotations

import time
from typing import Any

from .. import datagen
from ..metrics import Samples, throughput
from ..protocol import Client, compute_ref_uri
from .base import Context, Workload, register

SOURCE_PREFIX = "acqbench_read"


class _ReadBase(Workload):
    """Seed a fixed corpus in setup(), then read it back in run()."""

    def __init__(
        self,
        *,
        streams: int = 10,
        rows_per_stream: int = 10_000,
        reads: int = 20,
        **params: Any,
    ):
        super().__init__(
            streams=streams, rows_per_stream=rows_per_stream, reads=reads, **params
        )
        self.streams = streams
        self.rows_per_stream = rows_per_stream
        self.reads = reads
        self._client: Client | None = None
        self._source_id = ""
        self._names: list[str] = []
        self._window: datagen.Window | None = None

    def setup(self, ctx: Context) -> None:
        self._client = Client(ctx.base_url)
        self._source_id = f"{SOURCE_PREFIX}_{ctx.cell.cell_id}_{ctx.repetition}"
        self._names = datagen.stream_names(self.streams)
        self._client.register_streams(self._source_id, self._names, value_kind="numeric")

        # Seed via Arrow: it is the fastest ingest path, and seeding is not the
        # thing under measurement here.
        self._window = datagen.window_for(
            repetition=ctx.repetition, rows_per_stream=self.rows_per_stream, slot=9
        )
        data = datagen.generate(self._source_id, self._names, self._window)
        written = self._client.insert_arrow(data)
        expected = self.streams * self.rows_per_stream
        if written != expected:
            raise RuntimeError(f"seed wrote {written} rows, expected {expected}")

    def teardown(self, ctx: Context) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def _read_once(self, i: int) -> tuple[int, int]:
        raise NotImplementedError

    def _expected_rows(self) -> int:
        raise NotImplementedError

    def run(self, ctx: Context) -> dict[str, Any]:
        samples = Samples("read")
        total_rows = 0
        total_bytes = 0

        wall_start = time.perf_counter()
        for i in range(self.reads):
            t0 = time.perf_counter()
            rows, nbytes = self._read_once(i)
            samples.add((time.perf_counter() - t0) * 1000.0)
            total_rows += rows
            total_bytes += nbytes
        wall = time.perf_counter() - wall_start

        expected = self._expected_rows() * self.reads
        if total_rows != expected:
            raise RuntimeError(
                f"read {total_rows} rows, expected {expected}; "
                "an unknown URI returns an empty table silently"
            )

        return {
            "latency": samples.summary(),
            "reads": self.reads,
            "rows_read": total_rows,
            "rows_per_read": self._expected_rows(),
            "arrow_decoded_bytes_total": total_bytes,
            "wall_seconds": wall,
            "rows_per_second": throughput(total_rows, wall),
            **{f"server_{k}": v for k, v in ctx.server.resources().items()},
        }


@register("read_full")
class ReadFull(_ReadBase):
    """Full scan of one stream — the read_batch_size axis bites hardest here."""

    def _read_once(self, i: int) -> tuple[int, int]:
        assert self._client is not None
        uri = compute_ref_uri(self._source_id, self._names[i % len(self._names)])
        return self._client.read_timeseries_bytes(uri)

    def _expected_rows(self) -> int:
        return self.rows_per_stream


@register("read_window")
class ReadWindow(_ReadBase):
    """Bounded time window — exercises the index rather than a full scan."""

    def __init__(self, *, window_rows: int = 1_000, **params: Any):
        super().__init__(window_rows=window_rows, **params)
        self.window_rows = window_rows

    def _read_once(self, i: int) -> tuple[int, int]:
        assert self._client is not None and self._window is not None
        uri = compute_ref_uri(self._source_id, self._names[i % len(self._names)])
        start = self._window.start
        end = start + datagen.SAMPLE_INTERVAL * self.window_rows
        # `end` is inclusive of the row at that instant, so the half-open
        # arithmetic here would be off by one; step back one interval.
        return self._client.read_timeseries_bytes(
            uri, start=start, end=end - datagen.SAMPLE_INTERVAL
        )

    def _expected_rows(self) -> int:
        return min(self.window_rows, self.rows_per_stream)


@register("read_limit")
class ReadLimit(_ReadBase):
    """Most-recent-N — the shape a dashboard actually issues."""

    def __init__(self, *, limit: int = 100, **params: Any):
        super().__init__(limit=limit, **params)
        self.limit = limit

    def _read_once(self, i: int) -> tuple[int, int]:
        assert self._client is not None
        uri = compute_ref_uri(self._source_id, self._names[i % len(self._names)])
        return self._client.read_timeseries_bytes(uri, limit=self.limit, order="desc")

    def _expected_rows(self) -> int:
        return min(self.limit, self.rows_per_stream)

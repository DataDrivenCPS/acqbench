"""Plain data ingestion at scale, then a battery of timeseries reads over it.

One coherent measurement: each repetition ingests M+ rows into a *fresh* time
window (so every rep is a true insert, never a dedup), then queries exactly that
data with a spread of selectivities — from a single latest point to a full scan
of every row. Ingesting and querying the same data in one unit is what makes the
read numbers trustworthy: the rows are known to be present and their bounds are
known exactly, so a "fast" read can be told apart from a read of nothing.

Ingestion is the headline metric (rows/second). The query battery is emitted in
the same shape as the graph-query workload, so `acqbench queries` and
`queries-compare` report it without new plumbing.

Selectivity axis, high to low: 1 row -> 100 -> 1k -> 10k -> half a stream ->
one full stream -> several streams -> every stream. That spans a single indexed
lookup to a multi-million-row scan against one ingested dataset.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Callable

from .. import datagen
from ..metrics import Samples, summarize, throughput
from ..protocol import ARROW_STREAM_MIME, Client, arrow_ipc_bytes, compute_ref_uri
from .base import Context, Workload, register

SOURCE_PREFIX = "acqbench_scale"
DATASET_KEY = "ingested"  # the synthetic "graph" name the query battery reports under


@register("ingest_query")
class IngestQuery(Workload):
    """Ingest M+ rows, then read them back across a range of selectivities."""

    def __init__(
        self,
        *,
        streams: int = 40,
        rows_per_stream: int = 50_000,
        chunk_rows: int = 10_000,
        **params: Any,
    ):
        super().__init__(
            streams=streams,
            rows_per_stream=rows_per_stream,
            chunk_rows=chunk_rows,
            **params,
        )
        self.streams = streams
        self.rows_per_stream = rows_per_stream
        self.chunk_rows = chunk_rows
        self._client: Client | None = None
        self._source_id = ""
        self._names: list[str] = []

    @property
    def total_rows(self) -> int:
        return self.streams * self.rows_per_stream

    def setup(self, ctx: Context) -> None:
        self._client = Client(ctx.base_url)
        # Per (cell, repetition): a fresh source so no rep reads another's data.
        self._source_id = f"{SOURCE_PREFIX}_{ctx.cell.cell_id}_{ctx.repetition}"
        self._names = datagen.stream_names(self.streams)
        self._client.register_streams(self._source_id, self._names, value_kind="numeric")

    def teardown(self, ctx: Context) -> None:
        if self._client:
            self._client.close()
            self._client = None

    # -- ingestion --------------------------------------------------------

    def _base_time(self, ctx: Context) -> datetime:
        # A day per repetition; the dataset spans rows_per_stream seconds
        # (< 1 day for any realistic size), so windows never overlap.
        return datagen.EPOCH + timedelta(days=ctx.repetition)

    def _ingest(self, ctx: Context) -> dict[str, Any]:
        assert self._client is not None
        http = self._client._http
        base = self._base_time(ctx)
        per_chunk = Samples("chunk")
        rows_written = 0

        n_chunks = (self.rows_per_stream + self.chunk_rows - 1) // self.chunk_rows
        wall_start = time.perf_counter()
        for c in range(n_chunks):
            start_row = c * self.chunk_rows
            n = min(self.chunk_rows, self.rows_per_stream - start_row)
            body = self._chunk_body(base, start_row, n)
            t0 = time.perf_counter()
            r = http.post(
                "/insert_timeseries_arrow",
                content=body,
                headers={"Content-Type": ARROW_STREAM_MIME},
            )
            r.raise_for_status()
            per_chunk.add((time.perf_counter() - t0) * 1000.0)
            rows_written += int(r.json().get("rows_inserted", 0))
        wall = time.perf_counter() - wall_start

        if rows_written != self.total_rows:
            raise RuntimeError(
                f"ingested {rows_written} rows, expected {self.total_rows}; "
                "windows may overlap (dedup) or rows were dropped"
            )

        return {
            "rows_written": rows_written,
            "rows_per_second": throughput(rows_written, wall),
            "wall_seconds": wall,
            "chunks": n_chunks,
            "rows_per_chunk": self.streams * self.chunk_rows,
            "chunk_latency": per_chunk.summary(),
            "transport": "arrow",
        }

    def _chunk_body(self, base: datetime, start_row: int, n_rows: int) -> bytes:
        """Build the Arrow IPC body for one time-chunk across all streams."""
        streams: dict[tuple[str, str], list[tuple[datetime, Any]]] = {}
        for si, name in enumerate(self._names):
            rows = [
                (
                    base + datagen.SAMPLE_INTERVAL * (start_row + i),
                    datagen.numeric_value(si, start_row + i),
                )
                for i in range(n_rows)
            ]
            streams[(self._source_id, name)] = rows
        table = Client.build_arrow_table(streams, value_kind="numeric")
        return arrow_ipc_bytes(table)

    # -- query battery ----------------------------------------------------

    def _shapes(self, ctx: Context) -> list[tuple[str, str, int, int, Callable[[], int]]]:
        """(name, description, repeat, expected_rows, run_once) tuples.

        run_once performs one execution and returns the row count it saw, so the
        caller can both time it and assert on it.
        """
        assert self._client is not None
        c = self._client
        base = self._base_time(ctx)
        rps = self.rows_per_stream
        uri0 = compute_ref_uri(self._source_id, self._names[0])
        all_uris = [compute_ref_uri(self._source_id, n) for n in self._names]

        def window(start_off: int, n: int) -> Callable[[], int]:
            # Inclusive both ends server-side, so n rows means +(n-1) seconds.
            start = base + datagen.SAMPLE_INTERVAL * start_off
            end = start + datagen.SAMPLE_INTERVAL * (n - 1)
            return lambda: c.read_timeseries_bytes(uri0, start=start, end=end)[0]

        def limit(n: int) -> Callable[[], int]:
            return lambda: c.read_timeseries_bytes(uri0, limit=n, order="desc")[0]

        def full_streams(k: int) -> Callable[[], int]:
            def _run() -> int:
                return sum(c.read_timeseries_bytes(u)[0] for u in all_uris[:k])
            return _run

        def info(uris: list[str]) -> Callable[[], int]:
            return lambda: _count_info(c.timeseries_info(uris))

        half = rps // 2
        k_streams = min(5, self.streams)
        cap = lambda n: min(n, rps)  # a single stream can't return more than it holds
        # expected=None means "don't assert on the row count" — used for the
        # timeseries_info shapes, whose response layout is not pinned here.
        return [
            ("latest_1",        "single latest point (limit=1, desc)",      8, cap(1),        limit(1)),
            ("latest_100",      "dashboard tail (limit=100, desc)",         8, cap(100),      limit(100)),
            ("latest_1k",       "limit=1000, desc",                         6, cap(1_000),    limit(1_000)),
            ("window_100",      "100-row time window",                      8, cap(100),      window(0, cap(100))),
            ("window_1k",       "1k-row time window",                       6, cap(1_000),    window(0, cap(1_000))),
            ("window_10k",      "10k-row time window",                      4, cap(10_000),   window(0, cap(10_000))),
            ("window_half",     "half a stream",                           3, half,          window(0, half)),
            ("full_stream",     "one full stream (all rows)",              3, rps,           window(0, rps)),
            ("info_single",     "timeseries_info for one stream",          8, None,           info([uri0])),
            ("info_all",        "timeseries_info for every stream",        6, None,           info(all_uris)),
            (f"full_{k_streams}_streams", f"full scan of {k_streams} streams", 2, k_streams * rps, full_streams(k_streams)),
            ("full_all_streams", "full scan of every stream",              2, self.total_rows, full_streams(self.streams)),
        ]

    def _run_battery(self, ctx: Context) -> tuple[dict[str, Any], dict[str, str]]:
        battery: dict[str, Any] = {}
        errors: dict[str, str] = {}
        for name, desc, repeat, expected, run_once in self._shapes(ctx):
            times: list[float] = []
            rows_seen = None
            try:
                for _ in range(repeat):
                    t0 = time.perf_counter()
                    rows = run_once()
                    times.append((time.perf_counter() - t0) * 1000.0)
                    rows_seen = rows
                if expected is not None and rows_seen != expected:
                    raise RuntimeError(
                        f"{name}: read {rows_seen} rows, expected {expected}"
                    )
            except Exception as e:  # noqa: BLE001 — record, don't abort the battery
                errors[name] = f"{type(e).__name__}: {e}"[:300]
                continue
            battery[name] = {
                "rows": rows_seen,
                "description": desc,
                "count": len(times),
                **{k: round(v, 3) for k, v in summarize(times).items() if k != "count"},
            }
        return battery, errors

    # -- orchestration ----------------------------------------------------

    def run(self, ctx: Context) -> dict[str, Any]:
        ingest = self._ingest(ctx)
        battery, errors = self._run_battery(ctx)

        return {
            **ingest,  # rows_per_second etc. — ingestion is the headline
            "streams": self.streams,
            "rows_per_stream": self.rows_per_stream,
            # Emitted in the query-workload shape so the query reporting reuses.
            "graphs": {
                DATASET_KEY: {
                    "queries": battery,
                    "setup": {
                        "ingest_ms": ingest["wall_seconds"] * 1000.0,
                        "ingest_rows": ingest["rows_written"],
                    },
                }
            },
            "query_errors": errors,
            **{f"server_{k}": v for k, v in ctx.server.resources().items()},
        }


def _count_info(resp: Any) -> int:
    """Number of stream-info entries in a /timeseries_info response.

    The endpoint's exact shape is treated as opaque: a dict keyed by uri, a
    list of records, or a wrapper around either. Counting entries is enough to
    assert the call returned info for the streams asked about.
    """
    if isinstance(resp, dict):
        for key in ("info", "streams", "results", "timeseries"):
            if isinstance(resp.get(key), (list, dict)):
                return len(resp[key])
        return len(resp)
    if isinstance(resp, list):
        return len(resp)
    return 0

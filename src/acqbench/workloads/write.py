"""Timeseries write path.

Covers the transport axis that matters most: JSON /insert_timeseries versus
Arrow IPC /insert_timeseries_arrow. Both land in bulk_insert_polars, so the
delta between them at a fixed backend is the HTTP/deserialization layer, while
the delta between duckdb and timescale at a fixed transport is the engine.

Fairness note: both transports serialize their payload to final wire bytes in
setup(), and the timed region is only `POST bytes -> response`. Timing one
transport's encoder but not the other's would confound "this transport is
faster" with "the harness did its encoding earlier". Client-side encode cost is
a real difference between the two, so it is measured — but reported separately
as `encode_ms` rather than folded into request latency.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from .. import datagen
from ..metrics import Samples, throughput
from ..protocol import ARROW_STREAM_MIME, Client, arrow_ipc_bytes
from .base import Context, Workload, register

SOURCE_PREFIX = "acqbench"


class _WriteBase(Workload):
    """Register streams, then pre-serialize one payload per batch."""

    #: Separates timestamp windows between variants so that two workloads in the
    #: same cell can never overwrite each other's rows and turn inserts into
    #: dedup work.
    slot: int = 0
    value_kind: str = "numeric"
    endpoint: str = ""
    content_type: str = ""

    def __init__(
        self,
        *,
        streams: int = 100,
        rows_per_stream: int = 100,
        batches: int = 10,
        **params: Any,
    ):
        super().__init__(
            streams=streams, rows_per_stream=rows_per_stream, batches=batches, **params
        )
        self.streams = streams
        self.rows_per_stream = rows_per_stream
        self.batches = batches
        self._client: Client | None = None
        self._bodies: list[bytes] = []
        self._encode_ms: list[float] = []
        self._source_id = ""

    def setup(self, ctx: Context) -> None:
        self._client = Client(ctx.base_url)
        # Per (cell, repetition) so repetitions never share streams and a retry
        # cannot inherit a half-written table.
        self._source_id = f"{SOURCE_PREFIX}_{self.name}_{ctx.cell.cell_id}_{ctx.repetition}"
        names = datagen.stream_names(self.streams)
        self._client.register_streams(self._source_id, names, value_kind=self.value_kind)

        self._bodies = []
        self._encode_ms = []
        for b in range(self.batches):
            window = datagen.window_for(
                repetition=ctx.repetition * self.batches + b,
                rows_per_stream=self.rows_per_stream,
                slot=self.slot,
            )
            data = datagen.generate(self._source_id, names, window, value_kind=self.value_kind)
            t0 = time.perf_counter()
            body = self._encode(data)
            self._encode_ms.append((time.perf_counter() - t0) * 1000.0)
            self._bodies.append(body)

    def _encode(self, data: dict) -> bytes:
        raise NotImplementedError

    def teardown(self, ctx: Context) -> None:
        if self._client:
            self._client.close()
            self._client = None
        self._bodies = []

    def run(self, ctx: Context) -> dict[str, Any]:
        assert self._client is not None
        http = self._client._http
        samples = Samples("insert")
        rows_reported = 0

        wall_start = time.perf_counter()
        for body in self._bodies:
            t0 = time.perf_counter()
            r = http.post(
                self.endpoint, content=body, headers={"Content-Type": self.content_type}
            )
            r.raise_for_status()
            samples.add((time.perf_counter() - t0) * 1000.0)
            rows_reported += int(r.json().get("rows_inserted", 0))
        wall = time.perf_counter() - wall_start

        expected = self.streams * self.rows_per_stream * self.batches
        # The server reports what it actually wrote. A mismatch means dedup
        # collapsed rows or rows were silently dropped; either way the
        # throughput below would be a lie, so fail loudly instead.
        if rows_reported != expected:
            raise RuntimeError(
                f"server wrote {rows_reported} rows, expected {expected}; "
                "overlapping timestamp windows (dedup) or dropped rows"
            )

        payload_bytes = sum(len(b) for b in self._bodies)
        return {
            "latency": samples.summary(),
            "rows_written": rows_reported,
            "batches": self.batches,
            "rows_per_batch": self.streams * self.rows_per_stream,
            "wall_seconds": wall,
            "rows_per_second": throughput(rows_reported, wall),
            "payload_bytes_total": payload_bytes,
            "payload_bytes_per_row": payload_bytes / max(rows_reported, 1),
            "encode_ms_mean": sum(self._encode_ms) / max(len(self._encode_ms), 1),
            **_resources(ctx),
        }


def _iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.isoformat()


@register("write_json")
class WriteJson(_WriteBase):
    """POST /insert_timeseries — JSON body."""

    slot = 0
    endpoint = "/insert_timeseries"
    content_type = "application/json"

    def _encode(self, data: dict) -> bytes:
        payload = [
            {
                "source_id": sid,
                "ref_name": rn,
                "point_uri": None,
                "replace": False,
                "values": [[_iso(ts), v] for ts, v in rows],
            }
            for (sid, rn), rows in data.items()
        ]
        return json.dumps(payload).encode()


@register("write_arrow")
class WriteArrow(_WriteBase):
    """POST /insert_timeseries_arrow — Arrow IPC stream body."""

    slot = 1
    endpoint = "/insert_timeseries_arrow"
    content_type = ARROW_STREAM_MIME

    def _encode(self, data: dict) -> bytes:
        return arrow_ipc_bytes(Client.build_arrow_table(data, value_kind=self.value_kind))


@register("write_arrow_text")
class WriteArrowText(WriteArrow):
    """Arrow write of text-valued streams — exercises the text_value column."""

    slot = 2
    value_kind = "text"


def _resources(ctx: Context) -> dict[str, Any]:
    return {f"server_{k}": v for k, v in ctx.server.resources().items()}

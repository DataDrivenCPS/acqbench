"""Raw HTTP client for the acquirium server API.

Deliberately does not import acquirium: the harness must speak the same wire
protocol to every ref it benchmarks, so it cannot depend on a client library
whose behaviour changes between the versions under test.

Every fact encoded here was verified empirically against a live server. The
non-obvious ones are called out at their use site because getting them wrong
produces plausible-looking but meaningless measurements rather than errors.
"""

from __future__ import annotations

import io
import uuid
from datetime import datetime, timezone
from typing import Any, Sequence

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc

# UUID5 namespace hard-coded in acquirium.internals.models.compute_ref_uri.
_REF_URI_NAMESPACE = uuid.UUID("6a8f3c2e-4b1d-5e7f-9012-3a4b5c6d7e8f")

ACQ_NS = "urn:acquirium#"
ARROW_STREAM_MIME = "application/vnd.apache.arrow.stream"


def compute_ref_uri(source_id: str, ref_name: str) -> str:
    """The storage key for a stream. Pure function — no round trip needed."""
    return ACQ_NS + str(uuid.uuid5(_REF_URI_NAMESPACE, f"{source_id}:{ref_name}"))


def point_uri_for(source_id: str, ref_name: str) -> str:
    return f"urn:acquirium:point#{source_id}.{ref_name}"


def _iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.isoformat()


# The server converts the ts column's timezone, which requires a tz-aware
# column; a naive timestamp raises. 'us' matches storage resolution exactly.
ARROW_NUMERIC_SCHEMA = pa.schema([
    ("source_id", pa.string()),
    ("ref_name", pa.string()),
    ("ts", pa.timestamp("us", tz="UTC")),
    ("value", pa.float64()),
])

ARROW_TEXT_SCHEMA = pa.schema([
    ("source_id", pa.string()),
    ("ref_name", pa.string()),
    ("ts", pa.timestamp("us", tz="UTC")),
    ("value", pa.string()),
])


class ProtocolError(RuntimeError):
    pass


class Client:
    """Connection-pooled client for one server.

    Uses a persistent httpx.Client so that measurements reflect server work
    rather than repeated TCP/TLS setup.
    """

    def __init__(self, base_url: str, *, timeout: float = 300.0):
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            limits=httpx.Limits(max_keepalive_connections=32, max_connections=64),
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- health / meta ----------------------------------------------------

    def health(self) -> dict[str, Any]:
        r = self._http.get("/health", timeout=30.0)
        r.raise_for_status()
        return r.json()

    def graph_version(self) -> int:
        r = self._http.get("/graph_version", timeout=30.0)
        r.raise_for_status()
        return r.json()["version"]

    # -- registration -----------------------------------------------------

    def make_registration_ttl(
        self,
        source_id: str,
        ref_names: Sequence[str],
        value_kind: str = "numeric",
        *,
        with_points: bool = True,
    ) -> str:
        """TTL registering one stream per ref_name.

        The ref node's URI must equal compute_ref_uri(source_id, ref_name) or
        the server rejects the graph with a managed-reference mismatch.

        valueKind must be stated explicitly: the server normalizes a missing
        value kind to "text", which would send float values into the text
        column and read back as nulls.
        """
        if value_kind not in ("numeric", "text"):
            raise ValueError("value_kind must be 'numeric' or 'text'")

        lines = [
            "@prefix acq: <urn:acquirium#> .",
            "@prefix ref: <https://brickschema.org/schema/Brick/ref#> .",
            "",
        ]
        for ref_name in ref_names:
            ref_uri = compute_ref_uri(source_id, ref_name)
            if with_points:
                lines.append(
                    f"<{point_uri_for(source_id, ref_name)}> ref:hasExternalReference <{ref_uri}> ."
                )
            lines.append(
                f"<{ref_uri}> a ref:TimeseriesReference ;\n"
                f'    acq:sourceId "{source_id}" ;\n'
                f'    acq:refName "{ref_name}" ;\n'
                f'    acq:valueKind "{value_kind}" .'
            )
            lines.append("")
        return "\n".join(lines)

    def insert_graph(self, ttl: str, *, replace: bool = False) -> dict[str, Any]:
        """POST /insert_graph with inline TTL text.

        replace defaults to True *server-side*, which wipes the main graph; this
        wrapper defaults it to False so incremental registration is additive.
        """
        r = self._http.post(
            "/insert_graph",
            json={"rdf_graph": ttl, "format": "turtle", "replace": replace},
        )
        r.raise_for_status()
        return r.json()

    def register_streams(
        self,
        source_id: str,
        ref_names: Sequence[str],
        value_kind: str = "numeric",
    ) -> dict[str, Any]:
        """Make streams insertable. Unregistered inserts are rejected with 400."""
        ttl = self.make_registration_ttl(source_id, ref_names, value_kind=value_kind)
        return self.insert_graph(ttl, replace=False)

    def register_datasource(self, source_id: str) -> dict[str, Any]:
        """Optional — cosmetic graph node for SPARQL discoverability."""
        r = self._http.post("/register_datasource", json={"source_id": source_id}, timeout=30.0)
        r.raise_for_status()
        return r.json()

    # -- write ------------------------------------------------------------

    def insert_json(
        self,
        streams: dict[tuple[str, str], list[tuple[datetime, Any]]],
        *,
        replace: bool = False,
    ) -> int:
        """POST /insert_timeseries. Body is a list of StreamInsert objects.

        Returns rows_inserted as reported by the server.
        """
        payload = [
            {
                "source_id": sid,
                "ref_name": rn,
                "point_uri": None,
                "replace": replace,
                "values": [[_iso(ts), v] for ts, v in rows],
            }
            for (sid, rn), rows in streams.items()
        ]
        r = self._http.post("/insert_timeseries", json=payload)
        r.raise_for_status()
        return int(r.json().get("rows_inserted", 0))

    def insert_arrow(
        self,
        streams: dict[tuple[str, str], list[tuple[datetime, Any]]],
        *,
        value_kind: str = "numeric",
    ) -> int:
        """POST /insert_timeseries_arrow with an Arrow IPC stream body."""
        table = self.build_arrow_table(streams, value_kind=value_kind)
        return self.insert_arrow_table(table)

    def insert_arrow_table(self, table: pa.Table) -> int:
        r = self._http.post(
            "/insert_timeseries_arrow",
            content=arrow_ipc_bytes(table),
            headers={"Content-Type": ARROW_STREAM_MIME},
        )
        r.raise_for_status()
        return int(r.json().get("rows_inserted", 0))

    @staticmethod
    def build_arrow_table(
        streams: dict[tuple[str, str], list[tuple[datetime, Any]]],
        *,
        value_kind: str = "numeric",
    ) -> pa.Table:
        """Build the request table. Separate from sending so that serialization
        can be kept out of, or measured apart from, the timed region."""
        source_ids: list[str] = []
        ref_names: list[str] = []
        tss: list[datetime] = []
        values: list[Any] = []
        for (sid, rn), rows in streams.items():
            for ts, v in rows:
                source_ids.append(sid)
                ref_names.append(rn)
                tss.append(ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc))
                values.append(v)
        schema = ARROW_NUMERIC_SCHEMA if value_kind == "numeric" else ARROW_TEXT_SCHEMA
        return pa.Table.from_pydict(
            {"source_id": source_ids, "ref_name": ref_names, "ts": tss, "value": values},
            schema=schema,
        )

    # -- read -------------------------------------------------------------

    def read_timeseries_table(
        self,
        uri: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: str = "asc",
        value_mode: str = "default",
    ) -> pa.Table:
        """GET /timeseries -> Arrow IPC stream.

        `uri` accepts a ref URI or a point URI linked by hasExternalReference.

        An unknown URI returns an empty table rather than an error, so callers
        that care about correctness must assert on row counts themselves.
        """
        params: dict[str, Any] = {"uri": uri, "order": order, "value_mode": value_mode}
        if start is not None:
            params["start"] = _iso(start)
        if end is not None:
            params["end"] = _iso(end)
        if limit is not None:
            params["limit"] = limit

        r = self._http.get("/timeseries", params=params)
        r.raise_for_status()
        return ipc.RecordBatchStreamReader(pa.BufferReader(r.content)).read_all()

    def read_timeseries_bytes(self, uri: str, **kw: Any) -> tuple[int, int]:
        """Read and return (row_count, decoded_bytes).

        The byte figure is the *decoded* Arrow table size (`Table.nbytes`), not
        the compressed size on the wire.
        """
        table = self.read_timeseries_table(uri, **kw)
        return len(table), table.nbytes

    def timeseries_info(self, uris: list[str]) -> Any:
        """POST /timeseries_info -> per-stream metadata (row count, span).

        A cheaper, different path from a scan: the store answers from catalog
        metadata rather than materializing rows.
        """
        r = self._http.post("/timeseries_info", json={"uris": uris})
        r.raise_for_status()
        return r.json()

    # -- graph ------------------------------------------------------------

    def sparql(self, query: str, *, use_union: bool = True) -> dict[str, Any]:
        """GET /sparql_json -> {"columns": [...], "rows": [[...]]}.

        use_union=True consults the ontology-closure union graph and is much
        slower; it is a meaningful axis to sweep on its own.
        """
        r = self._http.get(
            "/sparql_json", params={"query": query, "use_union": str(use_union).lower()}
        )
        r.raise_for_status()
        return r.json()

    # -- drivers ----------------------------------------------------------

    def start_driver(
        self, spec: str, config: dict, *, name: str, interval: float
    ) -> dict[str, Any]:
        """POST /drivers/start. Setup runs before this returns, so the
        round-trip time includes actor creation + driver setup — which on the
        Ray backend is exactly the actor-scheduling cost worth measuring."""
        r = self._http.post(
            "/drivers/start",
            json={"spec": spec, "config": config, "name": name, "interval": interval},
            timeout=300.0,
        )
        r.raise_for_status()
        return r.json()

    def stop_driver(self, name: str) -> dict[str, Any]:
        r = self._http.post("/drivers/stop", json={"name": name}, timeout=120.0)
        r.raise_for_status()
        return r.json()

    def list_drivers(self) -> dict[str, Any]:
        r = self._http.get("/drivers/list", timeout=30.0)
        r.raise_for_status()
        return r.json()

    def sparql_update(self, update: str) -> dict[str, Any]:
        r = self._http.post("/sparql_update", json={"update": update})
        r.raise_for_status()
        return r.json()

    def export_graph(self) -> bytes:
        r = self._http.get("/export_graph")
        r.raise_for_status()
        return r.content


def arrow_ipc_bytes(table: pa.Table) -> bytes:
    """Serialize as an Arrow IPC *stream* (not the file format)."""
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()

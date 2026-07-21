"""Graph store: insert_graph and SPARQL.

The `use_union` flag on /sparql_json is the axis that matters most here. With
it on, the server consults the ontology-closure union graph — which is where the
bundled ~500k-triple ontology load shows up in query cost. With it off, only the
main graph is touched. Both are measured because real callers use both.
"""

from __future__ import annotations

import time
from typing import Any

from ..metrics import Samples, throughput
from ..protocol import Client
from .base import Context, Workload, register

SOURCE_PREFIX = "acqbench_graph"


@register("graph_insert")
class GraphInsert(Workload):
    """POST /insert_graph with a synthetic TTL of N registered streams.

    This is the registration hot path: manager._sync_stream_refs_from_graph
    carries an in-code note that syncing is ~3.6s for 1000 refs unbatched
    versus ~16ms batched, so it is exactly the kind of thing a change here
    would move.
    """

    def __init__(self, *, points: int = 500, inserts: int = 5, **params: Any):
        super().__init__(points=points, inserts=inserts, **params)
        self.points = points
        self.inserts = inserts
        self._client: Client | None = None
        self._ttls: list[str] = []

    def setup(self, ctx: Context) -> None:
        self._client = Client(ctx.base_url)
        self._ttls = []
        for i in range(self.inserts):
            source_id = f"{SOURCE_PREFIX}_{ctx.cell.cell_id}_{ctx.repetition}_{i}"
            names = [f"p{j:05d}" for j in range(self.points)]
            self._ttls.append(
                self._client.make_registration_ttl(source_id, names, value_kind="numeric")
            )

    def teardown(self, ctx: Context) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def run(self, ctx: Context) -> dict[str, Any]:
        assert self._client is not None
        samples = Samples("insert_graph")
        v_before = self._client.graph_version()

        wall_start = time.perf_counter()
        for ttl in self._ttls:
            t0 = time.perf_counter()
            self._client.insert_graph(ttl, replace=False)
            samples.add((time.perf_counter() - t0) * 1000.0)
        wall = time.perf_counter() - wall_start

        v_after = self._client.graph_version()
        if v_after <= v_before:
            raise RuntimeError(
                f"graph_version did not advance ({v_before} -> {v_after}); "
                "the graph may not have been written"
            )

        total_points = self.points * self.inserts
        return {
            "latency": samples.summary(),
            "inserts": self.inserts,
            "points_per_insert": self.points,
            "points_total": total_points,
            "ttl_bytes_total": sum(len(t.encode()) for t in self._ttls),
            "wall_seconds": wall,
            "points_per_second": throughput(total_points, wall),
            "graph_version_delta": v_after - v_before,
            **{f"server_{k}": v for k, v in ctx.server.resources().items()},
        }


class _SparqlBase(Workload):
    """Seed a known graph, then query it a fixed number of times."""

    use_union: bool = False

    def __init__(self, *, points: int = 500, queries: int = 20, **params: Any):
        super().__init__(points=points, queries=queries, **params)
        self.points = points
        self.queries = queries
        self._client: Client | None = None
        self._source_id = ""

    def setup(self, ctx: Context) -> None:
        self._client = Client(ctx.base_url)
        self._source_id = f"{SOURCE_PREFIX}_q_{ctx.cell.cell_id}_{ctx.repetition}"
        names = [f"p{j:05d}" for j in range(self.points)]
        self._client.register_streams(self._source_id, names, value_kind="numeric")

    def teardown(self, ctx: Context) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def _query(self) -> str:
        raise NotImplementedError

    def _expected_rows(self) -> int | None:
        return None

    def run(self, ctx: Context) -> dict[str, Any]:
        assert self._client is not None
        samples = Samples("sparql")
        query = self._query()
        row_counts: list[int] = []

        wall_start = time.perf_counter()
        for _ in range(self.queries):
            t0 = time.perf_counter()
            res = self._client.sparql(query, use_union=self.use_union)
            samples.add((time.perf_counter() - t0) * 1000.0)
            row_counts.append(len(res.get("rows", [])))
        wall = time.perf_counter() - wall_start

        expected = self._expected_rows()
        if expected is not None and row_counts and row_counts[0] != expected:
            raise RuntimeError(
                f"query returned {row_counts[0]} rows, expected {expected}"
            )

        return {
            "latency": samples.summary(),
            "queries": self.queries,
            "rows_returned": row_counts[0] if row_counts else 0,
            "use_union": self.use_union,
            "wall_seconds": wall,
            "queries_per_second": throughput(self.queries, wall),
            **{f"server_{k}": v for k, v in ctx.server.resources().items()},
        }


@register("sparql_main")
class SparqlMain(_SparqlBase):
    """Query the main graph only (use_union=false)."""

    use_union = False

    def _query(self) -> str:
        return f"""
            SELECT ?ref ?name WHERE {{
              ?ref <urn:acquirium#sourceId> "{self._source_id}" .
              ?ref <urn:acquirium#refName> ?name .
            }}
        """

    def _expected_rows(self) -> int:
        return self.points


@register("sparql_union")
class SparqlUnion(SparqlMain):
    """Same query against the ontology-closure union graph (use_union=true).

    The delta against sparql_main is what the closure costs per query.
    """

    use_union = True


@register("sparql_ontology")
class SparqlOntology(_SparqlBase):
    """Query the bundled ontologies themselves — pure closure cost, no seeded data."""

    use_union = True

    def _query(self) -> str:
        return """
            SELECT ?s ?label WHERE {
              ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label .
            } LIMIT 1000
        """

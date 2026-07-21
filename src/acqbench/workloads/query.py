"""Acquirium's Python query interface, against real plant graphs.

This is the one workload that runs acquirium's own code in-process rather than
speaking raw HTTP, because the thing under test *is* the client: `Client/query.py`
is the largest module in the codebase, and the SPARQL it generates for a
multi-hop traversal is exactly what a query-planner change would move.

To keep that from tying the harness to one acquirium version, the query script
(`acqbench.queries`) is exec'd as a subprocess **with the ref's own
interpreter**. Each ref therefore exercises its own client and its own server,
which is what a user of that version would actually get.

Graph isolation matters here. Each graph is loaded with `replace=True`, which
wipes the main graph so only that one is present. Without it every graph would
be visible at once and the zero-result cases — a Sensor query against Benicia,
which has no sensors — would silently never happen. Those empty results are
measured on purpose: "no match" is a different cost path from "match", and a
regression can hide in it.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from ..metrics import summarize, throughput
from .base import Context, Workload, register

#: Ships with the suite; see graphs/README.md for provenance and hashes.
GRAPHS_DIR = Path(__file__).resolve().parents[3] / "graphs"

GRAPHS: dict[str, str] = {
    "benicia": "benicia.ttl",
    "benicia_100": "benicia-100.ttl",
    "watertap": "watertap-seawater-ro.ttl",
}


@register("query_api")
class QueryApi(Workload):
    """Run every query in acqbench.queries against every shipped graph.

    Needs a server of its own: loading graphs with replace=True wipes the main
    graph, which would destroy the stream registrations that the write and read
    workloads depend on if they shared one.
    """

    requires_cold_server = True

    def __init__(self, *, repeat: int = 3, graphs: list[str] | None = None, **params: Any):
        super().__init__(repeat=repeat, graphs=graphs, **params)
        self.repeat = repeat
        self.graphs = graphs or list(GRAPHS)

    def setup(self, ctx: Context) -> None:
        missing = [g for g in self.graphs if g not in GRAPHS]
        if missing:
            raise ValueError(
                f"unknown graph(s) {missing}; available: {', '.join(GRAPHS)}"
            )
        for g in self.graphs:
            p = GRAPHS_DIR / GRAPHS[g]
            if not p.exists():
                raise FileNotFoundError(f"graph {g} missing at {p}")

    def run(self, ctx: Context) -> dict[str, Any]:
        script = _script_path()
        per_graph: dict[str, Any] = {}
        all_times: list[float] = []
        errors: dict[str, str] = {}
        empty_everywhere: set[str] = set()
        nonempty: set[str] = set()

        for g in self.graphs:
            out_path = ctx.workdir / f"queries-{g}-rep{ctx.repetition}.json"
            proc = subprocess.run(
                [
                    str(ctx.ref_python), str(script),
                    "--base-url", ctx.base_url,
                    "--graph", str(GRAPHS_DIR / GRAPHS[g]),
                    "--repeat", str(self.repeat),
                    "--json-out", str(out_path),
                ],
                capture_output=True,
                text=True,
                timeout=1800,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"query script failed on graph {g} (exit {proc.returncode})\n"
                    f"--- stdout ---\n{proc.stdout[-2000:]}\n"
                    f"--- stderr ---\n{proc.stderr[-2000:]}"
                )
            payload = json.loads(out_path.read_text())
            if "queries" not in payload:
                raise RuntimeError(
                    f"query script wrote an unexpected schema for {g}: "
                    f"top-level keys {sorted(payload)}; expected a 'queries' map"
                )
            # Loading a graph is not free — 0.2s for benicia but ~52s for
            # watertap, which refreshes embeddings and rebuilds the closure. It
            # is setup, not query time, so it is recorded separately rather than
            # folded into any query's latency.
            setup = payload.get("setup", {})
            results = payload["queries"]

            graph_summary: dict[str, Any] = {}
            for qname, r in results.items():
                if r.get("error"):
                    errors[f"{g}:{qname}"] = r["error"][:300]
                    continue
                times = r.get("times_ms", [])
                all_times.extend(times)
                rows = r.get("rows", 0)
                (nonempty if rows > 0 else empty_everywhere).add(qname)
                graph_summary[qname] = {
                    "rows": rows,
                    # Empty results are kept and reported, not filtered: the
                    # no-match path is measured deliberately.
                    "empty": rows == 0,
                    **{k: round(v, 3) for k, v in summarize(times).items() if k != "count"},
                    "count": len(times),
                }
            per_graph[g] = {"queries": graph_summary, "setup": setup}

        # A query returning nothing on *every* graph is measuring nothing —
        # it is not a valid zero-result case, it is a broken query.
        dead = sorted(empty_everywhere - nonempty)

        return {
            "graphs": per_graph,
            "queries_run": sum(len(v["queries"]) for v in per_graph.values()),
            "repeat": self.repeat,
            "latency": summarize(all_times),
            "queries_per_second": throughput(len(all_times), sum(all_times) / 1000.0),
            "empty_on_all_graphs": dead,
            "errors": errors,
            **{f"server_{k}": v for k, v in ctx.server.resources().items()},
        }


def _script_path() -> Path:
    """Locate queries.py *without importing it*.

    It imports acquirium at module scope, which exists only in a ref's venv —
    importing it here, from the harness venv, would fail.
    """
    p = Path(__file__).resolve().parents[1] / "queries.py"
    if not p.exists():
        raise FileNotFoundError(f"query script not found at {p}")
    return p

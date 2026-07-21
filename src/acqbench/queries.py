"""Benchmark queries for the acquirium Python Query API.

This module is executed *inside a ref's own venv* (it imports ``acquirium``) as a
subprocess by the benchmark harness.  It therefore imports nothing beyond
``acquirium`` + the standard library.

Each entry in :data:`QUERIES` is a :class:`QuerySpec` whose ``fn`` is a pure
function of an ``Acquirium`` client and returns the number of rows the query
produced, so the harness can re-run and time it freely.

Design notes / gotchas encoded here
-----------------------------------
* **All class / predicate / unit arguments are passed as ``URIRef``.**  The public
  Query methods are wrapped in ``@flex_query_rdf_inputs``, which sends any *plain
  string* that does not look like a URI through the server-side embedding
  resolver.  That is both slow and non-deterministic, and it silently mangles the
  inverse-path syntax (``"^<uri>"`` does not look like a URI, so it gets resolved
  to some *other* predicate rather than being treated as an inverse path).
  ``URIRef`` values bypass the resolver entirely, so a benchmark measures the
  query, not the embedding index.
* **``use_union`` is not part of the Query cache key.**  ``Query.execute`` caches
  under the literal key ``"execute"`` and ``Query.metadata`` under
  ``f"metadata_table:{include_internals}"``.  Re-calling either with a different
  ``use_union`` on the *same* Query object returns the first result.  Every fn
  below therefore builds a fresh Query.
* **The union graph is the owl:imports closure of the main graph, not "main +
  all bundled ontologies".**  ``benicia.ttl`` / ``benicia-100.ttl`` declare no
  ``owl:imports``, so for them union == main and ``use_union`` changes nothing but
  the code path.  ``watertap-seawater-ro.ttl`` imports the NAWI ontology, so only
  there does ``use_union=True`` add subclass closure.
* **Data nodes require ``ref:hasExternalReference``.**  None of the plant graphs
  carry it, so every ``find_data`` / ``find_related_data`` / ``filter_by_*`` query
  would return 0 rows on every graph against a bare graph load.  :func:`setup_graph`
  registers one external reference per property (metadata only -- no timeseries
  rows are inserted) so that data-node resolution is actually exercised.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from acquirium.Client.acquirium import Acquirium
from acquirium.internals.internals_namespaces import URIRef

# --------------------------------------------------------------------------
# Vocabulary
# --------------------------------------------------------------------------

S223 = "http://data.ashrae.org/standard223#"
NAWI = "urn:nawi-water-ontology#"
UNIT = "http://qudt.org/vocab/unit/"
QK = "http://qudt.org/vocab/quantitykind/"

# classes
C_PUMP = URIRef(NAWI + "Pump")
C_TANK = URIRef(NAWI + "Tank")
C_SENSOR = URIRef(S223 + "Sensor")
C_EQUIPMENT = URIRef(S223 + "Equipment")
C_CONNECTION = URIRef(S223 + "Connection")
C_OBS_PROP = URIRef(S223 + "QuantifiableObservableProperty")

# predicates
P_HAS_PROPERTY = URIRef(S223 + "hasProperty")
P_HAS_CP = URIRef(S223 + "hasConnectionPoint")
P_CONNECTED_THROUGH = URIRef(S223 + "connectedThrough")
P_CONNECTS_TO = URIRef(S223 + "connectsTo")
# Inverse property paths work, but only when passed as a URIRef: as a plain
# string "^<uri>" does not satisfy looks_like_uri(), so the flex decorator sends
# it to the embedding resolver, which silently returns some unrelated predicate.
# (Verified: predicates=["^...#connectsFrom"] compiled to <...#connectsTo>.)
P_CNX_INV = URIRef("^" + S223 + "cnx")  # Connection -cnx-> ConnectionPoint, walked backwards

# filter values
U_MG_PER_L = URIRef(UNIT + "MilliGM-PER-L")
M_FLUID_WATER = URIRef(S223 + "Fluid-Water")
SUB_TSS = URIRef(S223 + "Solids-SuspendedSolids")
QK_VOLUME_FLOW = URIRef(QK + "VolumeFlowRate")

# An instance that exists only in the two benicia graphs.
URI_INFLUENT_PUMP = "urn:ex/Influent_Pump"


@dataclass(frozen=True)
class QuerySpec:
    name: str
    description: str
    feature: str
    fn: Callable[[Acquirium], int]


def _rows(query, *, use_union: bool = True) -> int:
    """Execute a Query and return its row count."""
    return len(query.execute(use_union=use_union).get("rows", []))


# --------------------------------------------------------------------------
# find_entity
# --------------------------------------------------------------------------


def q_entity_by_class_pump(aq: Acquirium) -> int:
    return _rows(aq.query().find_entity(_class=C_PUMP, alias="pump"))


def q_entity_by_uri(aq: Acquirium) -> int:
    return _rows(aq.query().find_entity(uri=URI_INFLUENT_PUMP, alias="pump"))


def q_entity_sensor_class(aq: Acquirium) -> int:
    return _rows(aq.query().find_entity(_class=C_SENSOR, alias="sensor"))


def q_entity_superclass_closure(aq: Acquirium) -> int:
    # s223:Equipment matches nothing directly; it only resolves through the
    # rdfs:subClassOf* closure contributed by an owl:imports'd ontology.
    return _rows(aq.query().find_entity(_class=C_EQUIPMENT, alias="eq"), use_union=True)


# --------------------------------------------------------------------------
# metadata() / use_union axis
# --------------------------------------------------------------------------


def q_metadata_broad_union(aq: Acquirium) -> int:
    q = aq.query().find_entity(_class=C_OBS_PROP, alias="prop")
    return q.metadata(use_union=True).height


def q_metadata_broad_no_union(aq: Acquirium) -> int:
    q = aq.query().find_entity(_class=C_OBS_PROP, alias="prop")
    return q.metadata(use_union=False).height


# --------------------------------------------------------------------------
# find_related
# --------------------------------------------------------------------------


def q_related_hops1(aq: Acquirium) -> int:
    q = aq.query().find_entity(_class=C_PUMP, alias="pump")
    q = q.find_related(_class=C_OBS_PROP, alias="prop", _from="pump", hops=1)
    return _rows(q, use_union=False)


def q_related_hops3(aq: Acquirium) -> int:
    # NOTE: use_union=False is deliberate and load-bearing. An unconstrained
    # hops=3 edge expands to a UNION of 1/2/3-step *variable-predicate* chains;
    # over a graph whose owl:imports closure pulls in the NAWI/s223/QUDT
    # ontologies (watertap) that is a cartesian blowup -- measured at >20 min
    # without completing, and the abandoned query keeps burning CPU and holding
    # the store lock after the client disconnects.  Restricting to the main
    # graph keeps this pair a clean measurement of the hop-limit axis alone
    # (1 -> 3 hops costs ~50x); the ontology-closure axis is measured separately
    # by metadata_broad_union / metadata_broad_no_union.
    q = aq.query().find_entity(_class=C_PUMP, alias="pump")
    q = q.find_related(_class=C_OBS_PROP, alias="prop", _from="pump", hops=3)
    return _rows(q, use_union=False)


def q_related_explicit_predicate(aq: Acquirium) -> int:
    q = aq.query().find_entity(_class=C_PUMP, alias="pump")
    q = q.find_related(
        _class=C_OBS_PROP, alias="prop", _from="pump", predicates=[P_HAS_PROPERTY]
    )
    return _rows(q)


def q_related_multi_hop_predicates(aq: Acquirium) -> int:
    # multi_hop_predicates=True lets the constrained predicate set repeat up to
    # `hops` times; without it the edge is pinned to a single hop.
    q = aq.query().find_entity(_class=C_PUMP, alias="pump")
    q = q.find_related(
        _class=C_OBS_PROP,
        alias="prop",
        _from="pump",
        predicates=[P_HAS_CP, P_HAS_PROPERTY],
        multi_hop_predicates=True,
        hops=2,
    )
    return _rows(q)


def q_related_downstream(aq: Acquirium) -> int:
    # nawi:Tank is a concrete class present in every plant graph, so the
    # directional traversal is actually exercised everywhere rather than being
    # short-circuited by a class that needs the subClassOf* closure.
    q = aq.query().find_entity(_class=C_PUMP, alias="pump")
    q = q.find_related(_class=C_TANK, alias="dn", _from="pump", direction="downstream", hops=3)
    return _rows(q)


def q_related_upstream(aq: Acquirium) -> int:
    q = aq.query().find_entity(_class=C_PUMP, alias="pump")
    q = q.find_related(_class=C_TANK, alias="up", _from="pump", direction="upstream", hops=3)
    return _rows(q)


def q_relate_to_join(aq: Acquirium) -> int:
    # Two independently built queries joined by an unconstrained <=2 hop edge.
    q1 = aq.query().find_entity(_class=C_PUMP, alias="pump")
    q2 = aq.query().find_entity(_class=C_CONNECTION, alias="conn")
    # use_union=False for the same reason as q_related_hops3: the join edge is
    # unconstrained and multi-hop.
    return _rows(q1.relate_to(q2, hops=2), use_union=False)


def q_connectivity_cp_chain(aq: Acquirium) -> int:
    # pump -hasConnectionPoint-> cp <-cnx- Connection -connectsTo-> equipment
    q = aq.query().find_entity(_class=C_PUMP, alias="pump")
    q = q.find_related(_class=C_CONNECTION, alias="conn", _from="pump", predicates=[P_CONNECTED_THROUGH])
    q = q.find_related(_class=C_PUMP, alias="dest", _from="conn", predicates=[P_CONNECTS_TO])
    return _rows(q)


# --------------------------------------------------------------------------
# data nodes
# --------------------------------------------------------------------------


def q_find_data_direct(aq: Acquirium) -> int:
    q = aq.query().find_entity(_class=C_PUMP, alias="pump")
    q = q.find_data(_from="pump", alias="data")
    return _rows(q)


def q_find_related_data_directional(aq: Acquirium) -> int:
    q = aq.query().find_entity(_class=C_PUMP, alias="pump")
    q = q.find_related_data(_from="pump", alias="data", direction="downstream", hops=2)
    return _rows(q)


def q_filter_by_unit(aq: Acquirium) -> int:
    q = aq.query().find_entity(_class=C_PUMP, alias="pump").find_data(_from="pump", alias="data")
    return _rows(q.filter_by_unit(U_MG_PER_L, _from="data"))


def q_filter_by_medium(aq: Acquirium) -> int:
    q = aq.query().find_entity(_class=C_PUMP, alias="pump").find_data(_from="pump", alias="data")
    return _rows(q.filter_by_medium(M_FLUID_WATER, _from="data"))


def q_filter_by_substance_exclude(aq: Acquirium) -> int:
    # exclude=True -> FILTER NOT EXISTS
    q = aq.query().find_entity(_class=C_PUMP, alias="pump").find_data(_from="pump", alias="data")
    return _rows(q.filter_by_substance(SUB_TSS, _from="data", exclude=True))


def q_filter_by_quantity_kind(aq: Acquirium) -> int:
    q = aq.query().find_entity(_class=C_PUMP, alias="pump").find_data(_from="pump", alias="data")
    return _rows(q.filter_by_quantity_kind(QK_VOLUME_FLOW, _from="data"))


# --------------------------------------------------------------------------
# registry
# --------------------------------------------------------------------------

QUERIES: list[QuerySpec] = [
    QuerySpec(
        "entity_by_class_pump",
        "All nawi:Pump instances.",
        "find_entity(_class=...)",
        q_entity_by_class_pump,
    ),
    QuerySpec(
        "entity_by_uri",
        "A single named instance -- the narrowest possible query.",
        "find_entity(uri=...)",
        q_entity_by_uri,
    ),
    QuerySpec(
        "entity_sensor_class",
        "All s223:Sensor instances (class absent from the benicia graphs).",
        "find_entity(_class=...) on a graph-specific class",
        q_entity_sensor_class,
    ),
    QuerySpec(
        "entity_superclass_closure",
        "s223:Equipment, matchable only via the owl:imports subClassOf* closure.",
        "ontology closure / subClassOf* under use_union=True",
        q_entity_superclass_closure,
    ),
    QuerySpec(
        "metadata_broad_union",
        "Every QuantifiableObservableProperty, materialized through metadata().",
        "metadata(use_union=True) + deliberately broad class",
        q_metadata_broad_union,
    ),
    QuerySpec(
        "metadata_broad_no_union",
        "Same broad class against the main graph only.",
        "metadata(use_union=False)",
        q_metadata_broad_no_union,
    ),
    QuerySpec(
        "related_hops1",
        "Properties at most 1 hop from a pump, any predicate.",
        "find_related(hops=1), unconstrained predicates",
        q_related_hops1,
    ),
    QuerySpec(
        "related_hops3",
        "Properties at most 3 hops from a pump, any predicate.",
        "find_related(hops=3) -- hop-limit blowup",
        q_related_hops3,
    ),
    QuerySpec(
        "related_explicit_predicate",
        "Properties reached from a pump via s223:hasProperty only.",
        "find_related(predicates=[...])",
        q_related_explicit_predicate,
    ),
    QuerySpec(
        "related_multi_hop_predicates",
        "Properties via repeated hasConnectionPoint/hasProperty steps.",
        "find_related(multi_hop_predicates=True)",
        q_related_multi_hop_predicates,
    ),
    QuerySpec(
        "related_downstream",
        "Tanks downstream of a pump in the s223 connection topology.",
        'find_related(direction="downstream")',
        q_related_downstream,
    ),
    QuerySpec(
        "related_upstream",
        "Tanks upstream of a pump in the s223 connection topology.",
        'find_related(direction="upstream")',
        q_related_upstream,
    ),
    QuerySpec(
        "relate_to_join",
        "Join two independently built queries (pumps x connections).",
        "relate_to(other)",
        q_relate_to_join,
    ),
    QuerySpec(
        "connectivity_cp_chain",
        "pump -connectedThrough-> Connection -connectsTo-> pump.",
        "multi-node connectivity traversal",
        q_connectivity_cp_chain,
    ),
    QuerySpec(
        "find_data_direct",
        "Data nodes hanging 1 hop off a pump.",
        "find_data()",
        q_find_data_direct,
    ),
    QuerySpec(
        "find_related_data_directional",
        "Data nodes up to 2 topology hops downstream of a pump.",
        'find_related_data(direction="downstream")',
        q_find_related_data_directional,
    ),
    QuerySpec(
        "filter_by_unit",
        "Pump data nodes measured in mg/L.",
        "filter_by_unit()",
        q_filter_by_unit,
    ),
    QuerySpec(
        "filter_by_medium",
        "Pump data nodes whose medium is s223:Fluid-Water.",
        "filter_by_medium()",
        q_filter_by_medium,
    ),
    QuerySpec(
        "filter_by_substance_exclude",
        "Pump data nodes that are NOT suspended-solids measurements.",
        "filter_by_substance(exclude=True) -> FILTER NOT EXISTS",
        q_filter_by_substance_exclude,
    ),
    QuerySpec(
        "filter_by_quantity_kind",
        "Pump data nodes whose quantity kind is VolumeFlowRate.",
        "filter_by_quantity_kind()",
        q_filter_by_quantity_kind,
    ),
]


# --------------------------------------------------------------------------
# graph setup
# --------------------------------------------------------------------------

_BENCH_SOURCE_ID = "acqbench"


def _local_name(uri: str) -> str:
    for sep in ("#", "/"):
        if sep in uri:
            uri = uri.rsplit(sep, 1)[-1]
    return uri


def register_data_refs(aq: Acquirium) -> int:
    """Give every property an external reference so data nodes resolve.

    The plant graphs carry no ``ref:hasExternalReference``, and ``find_data`` /
    ``find_related_data`` / ``filter_by_*`` all require one.  This writes
    metadata-only stream registrations (no timeseries rows) for each property.
    Returns the number of properties registered.
    """
    props: list[str] = []
    for cls in (S223 + "QuantifiableObservableProperty", S223 + "QuantifiableActuatableProperty"):
        res = aq.client.sparql_query(
            f"SELECT ?p WHERE {{ ?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{cls}> }}",
            use_union=False,
        )
        props.extend(row[0] for row in res.get("rows", []) if row and row[0])

    if not props:
        return 0

    aq.register_datasource(_BENCH_SOURCE_ID)
    aq.register_streams(
        [
            {"point_uri": p, "source_id": _BENCH_SOURCE_ID, "ref_name": _local_name(p)}
            for p in props
        ]
    )
    return len(props)


def setup_graph(aq: Acquirium, graph_path: str | Path, *, register_refs: bool = True) -> dict:
    """Load a graph as the sole content of the main graph, then prime data nodes."""
    ttl = Path(graph_path).read_text()
    t0 = time.perf_counter()
    aq.insert_graph(ttl, format="turtle", replace=True)
    insert_ms = (time.perf_counter() - t0) * 1000.0

    t0 = time.perf_counter()
    n_refs = register_data_refs(aq) if register_refs else 0
    register_ms = (time.perf_counter() - t0) * 1000.0

    return {
        "insert_ms": round(insert_ms, 1),
        "register_ms": round(register_ms, 1),
        "registered_refs": n_refs,
    }


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------


def _parse_base_url(base_url: str) -> tuple[str, int, bool]:
    url = base_url.strip()
    use_ssl = url.startswith("https://")
    for prefix in ("https://", "http://"):
        if url.startswith(prefix):
            url = url[len(prefix) :]
            break
    url = url.rstrip("/")
    host, _, port = url.partition(":")
    return host or "localhost", int(port) if port else (443 if use_ssl else 8000), use_ssl


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run the acquirium Query API benchmark queries.")
    ap.add_argument("--base-url", required=True, help="e.g. http://127.0.0.1:8000")
    ap.add_argument("--graph", required=True, help="path to a .ttl graph to load (replace=True)")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--json-out", default=None)
    ap.add_argument("--only", default=None, help="comma-separated query names")
    ap.add_argument(
        "--no-register-refs",
        action="store_true",
        help="skip external-reference registration (all data-node queries then return 0)",
    )
    args = ap.parse_args(argv)

    host, port, use_ssl = _parse_base_url(args.base_url)
    aq = Acquirium(server_url=host, server_port=port, use_ssl=use_ssl)

    setup = setup_graph(aq, args.graph, register_refs=not args.no_register_refs)

    selected = QUERIES
    if args.only:
        wanted = {s.strip() for s in args.only.split(",") if s.strip()}
        selected = [q for q in QUERIES if q.name in wanted]

    # Warm up the imports-closure cache: the first union query after an insert
    # pays for building the whole union graph and would otherwise be charged to
    # whichever query happens to run first.
    try:
        aq.query().find_entity(_class=C_PUMP, alias="warmup").execute(use_union=True)
    except Exception:
        pass

    results: dict[str, dict] = {}
    for spec in selected:
        rows: int | None = None
        times: list[float] = []
        error: str | None = None
        for _ in range(max(1, args.repeat)):
            t0 = time.perf_counter()
            try:
                rows = spec.fn(aq)
            except Exception as exc:  # noqa: BLE001 - one bad query must not kill the run
                error = f"{type(exc).__name__}: {exc}"
                times.append((time.perf_counter() - t0) * 1000.0)
                break
            times.append((time.perf_counter() - t0) * 1000.0)
        results[spec.name] = {
            "rows": rows,
            "times_ms": [round(t, 2) for t in times],
            "error": error,
        }
        status = error if error else f"{rows} rows"
        print(f"{spec.name:32s} {status:>28s}  {min(times):8.1f} ms (min of {len(times)})")

    payload = {
        "graph": str(args.graph),
        "setup": setup,
        "queries": results,
    }
    if args.json_out:
        out = Path(args.json_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2))
        print(f"\nwrote {out}")

    n_err = sum(1 for r in results.values() if r["error"])
    print(f"\n{len(results)} queries, {n_err} errored")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

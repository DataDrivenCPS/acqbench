"""Summarize and compare results.

Repetitions are aggregated by **median**, not mean: a single slow repetition
(GC pause, a compaction, a noisy neighbour) should not move the reported
figure. Spread is reported alongside so that a comparison resting on noisy
inputs can be recognized as such.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .results import read_results

#: Relative change below which two numbers are treated as the same. Run-to-run
#: noise on a laptop is comfortably a few percent, so anything tighter would
#: report phantom regressions.
NOISE_FLOOR = 0.05

#: The metric each workload is judged on, and whether bigger is better.
HEADLINE: dict[str, tuple[str, bool]] = {
    "query_api": ("latency.median_ms", False),
    "ingest_query": ("rows_per_second", True),  # ingestion is the headline
    "write_json": ("rows_per_second", True),
    "write_arrow": ("rows_per_second", True),
    "write_arrow_text": ("rows_per_second", True),
    "read_full": ("rows_per_second", True),
    "read_window": ("rows_per_second", True),
    "read_limit": ("latency.median_ms", False),
    "graph_insert": ("points_per_second", True),
    "sparql_main": ("queries_per_second", True),
    "sparql_union": ("queries_per_second", True),
    "sparql_ontology": ("queries_per_second", True),
    "startup_cold": ("time_to_healthy_ms", False),
    "startup_warm": ("time_to_healthy_ms", False),
}


@dataclass(frozen=True)
class Key:
    """Everything that must match for two measurements to be comparable.

    `profile` is part of the key because DEBUG logging is not free: acquirium's
    timed_debug skips its own cost when DEBUG is off, so a profiled run is
    measurably slower than an unprofiled one. Comparing across that boundary
    would report the logging as a regression.
    """

    workload: str
    backend: str
    topology: str
    read_batch_size: int
    profile: bool = False

    def label(self) -> str:
        suffix = "/profiled" if self.profile else ""
        return f"{self.workload} [{self.backend}/{self.topology}/rbs={self.read_batch_size}{suffix}]"


@dataclass
class Agg:
    key: Key
    ref_spec: str
    ref_resolved: str
    metric: str
    values: list[float]
    higher_is_better: bool
    failures: int = 0

    @property
    def median(self) -> float:
        return statistics.median(self.values) if self.values else float("nan")

    @property
    def spread_pct(self) -> float:
        """Peak-to-peak spread as a fraction of the median — a noise indicator."""
        if len(self.values) < 2:
            return 0.0
        med = self.median
        if med == 0:
            return 0.0
        return (max(self.values) - min(self.values)) / abs(med)


def _dig(d: dict, dotted: str) -> Any:
    cur: Any = d
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def aggregate(results_path: Path) -> dict[tuple[Key, str], Agg]:
    """Collapse repetitions into one Agg per (key, ref)."""
    buckets: dict[tuple[Key, str], Agg] = {}
    for row in read_results(results_path):
        wl = row.get("workload")
        if wl not in HEADLINE:
            continue
        metric, hib = HEADLINE[wl]
        key = Key(
            workload=wl,
            backend=row.get("backend", "?"),
            topology=row.get("topology", "?"),
            read_batch_size=int(row.get("read_batch_size", 0)),
            profile=bool(row.get("profile", False)),
        )
        bk = (key, row.get("ref_spec", "?"))
        agg = buckets.get(bk)
        if agg is None:
            agg = buckets[bk] = Agg(
                key=key,
                ref_spec=row.get("ref_spec", "?"),
                ref_resolved=row.get("ref_resolved", "?"),
                metric=metric,
                values=[],
                higher_is_better=hib,
            )
        if not row.get("ok", False):
            agg.failures += 1
            continue
        v = _dig(row.get("metrics", {}), metric)
        if isinstance(v, (int, float)):
            agg.values.append(float(v))
    return buckets


@dataclass
class Comparison:
    key: Key
    metric: str
    baseline_ref: str
    candidate_ref: str
    baseline: float
    candidate: float
    higher_is_better: bool
    noisy: bool

    @property
    def rel_change(self) -> float:
        """Signed change of the raw metric, candidate vs baseline."""
        if self.baseline == 0:
            return float("inf") if self.candidate else 0.0
        return (self.candidate - self.baseline) / abs(self.baseline)

    @property
    def improvement(self) -> float:
        """Positive means the candidate is better, whatever the metric's polarity."""
        return self.rel_change if self.higher_is_better else -self.rel_change

    @property
    def verdict(self) -> str:
        if self.noisy:
            return "noisy"
        if abs(self.improvement) < NOISE_FLOOR:
            return "same"
        return "faster" if self.improvement > 0 else "SLOWER"


def compare(
    results_path: Path, baseline_ref: str, candidate_refs: Iterable[str] | None = None
) -> list[Comparison]:
    """Compare each candidate ref against `baseline_ref`, like for like."""
    aggs = aggregate(results_path)

    by_key: dict[Key, dict[str, Agg]] = defaultdict(dict)
    for (key, ref), agg in aggs.items():
        by_key[key][ref] = agg

    refs_present = {ref for _, ref in aggs}
    if baseline_ref not in refs_present:
        raise ValueError(
            f"baseline {baseline_ref!r} has no results; present: {', '.join(sorted(refs_present))}"
        )
    candidates = list(candidate_refs) if candidate_refs else sorted(refs_present - {baseline_ref})

    out: list[Comparison] = []
    for key, per_ref in sorted(by_key.items(), key=lambda kv: kv[0].label()):
        base = per_ref.get(baseline_ref)
        if base is None or not base.values:
            continue
        for cand_ref in candidates:
            cand = per_ref.get(cand_ref)
            if cand is None or not cand.values:
                continue
            out.append(
                Comparison(
                    key=key,
                    metric=base.metric,
                    baseline_ref=baseline_ref,
                    candidate_ref=cand_ref,
                    baseline=base.median,
                    candidate=cand.median,
                    higher_is_better=base.higher_is_better,
                    # If either side's own repetitions disagree by more than the
                    # effect we'd be claiming, the claim isn't supportable.
                    noisy=max(base.spread_pct, cand.spread_pct) > NOISE_FLOOR * 2,
                )
            )
    return out


def marginal_cost(results_path: Path) -> list[dict[str, Any]]:
    """What each topology step costs, relative to `server` alone.

    This is the point of the topology axis: the delta between `server` and
    `server+drivers` on an otherwise identical cell is what running drivers
    takes away from the server's throughput.
    """
    aggs = aggregate(results_path)
    # Group by everything except topology.
    grouped: dict[tuple[str, str, str, int], dict[str, Agg]] = defaultdict(dict)
    for (key, ref), agg in aggs.items():
        gk = (key.workload, key.backend, ref, key.read_batch_size)
        grouped[gk][key.topology] = agg

    rows: list[dict[str, Any]] = []
    for (workload, backend, ref, rbs), by_topo in sorted(grouped.items()):
        base = by_topo.get("server")
        if base is None or not base.values:
            continue
        for topo, agg in sorted(by_topo.items()):
            if topo == "server" or not agg.values:
                continue
            delta = (agg.median - base.median) / abs(base.median) if base.median else 0.0
            rows.append(
                {
                    "workload": workload,
                    "backend": backend,
                    "ref": ref,
                    "read_batch_size": rbs,
                    "topology": topo,
                    "metric": base.metric,
                    "server_only": base.median,
                    "with_components": agg.median,
                    "rel_change": delta if base.higher_is_better else -delta,
                }
            )
    return rows


def span_table(
    results_path: Path, *, workload: str | None = None, ref: str | None = None
) -> list[dict[str, Any]]:
    """Server-side span timings, aggregated across repetitions.

    Only populated for runs recorded with `--profile`.
    """
    acc: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for row in read_results(results_path):
        if not row.get("ok") or not row.get("spans"):
            continue
        if workload and row.get("workload") != workload:
            continue
        if ref and row.get("ref_spec") != ref:
            continue
        for name, stats in row["spans"].items():
            acc[(row["workload"], row.get("ref_spec", "?"), name)].append(stats)

    out: list[dict[str, Any]] = []
    for (wl, ref_spec, name), stats_list in acc.items():
        out.append(
            {
                "workload": wl,
                "ref": ref_spec,
                "span": name,
                "logger": stats_list[0].get("logger", ""),
                # Median across repetitions of each repetition's own total.
                "total_ms": statistics.median([s["total_ms"] for s in stats_list]),
                "mean_ms": statistics.median([s["mean_ms"] for s in stats_list]),
                "calls": statistics.median([s["count"] for s in stats_list]),
                "reps": len(stats_list),
            }
        )
    out.sort(key=lambda r: r["total_ms"], reverse=True)
    return out


@dataclass
class SpanDelta:
    workload: str
    span: str
    baseline_ms: float
    candidate_ms: float
    baseline_calls: float
    candidate_calls: float

    @property
    def delta_ms(self) -> float:
        return self.candidate_ms - self.baseline_ms

    @property
    def rel_change(self) -> float:
        if self.baseline_ms == 0:
            return float("inf") if self.candidate_ms else 0.0
        return (self.candidate_ms - self.baseline_ms) / self.baseline_ms

    @property
    def verdict(self) -> str:
        if self.baseline_ms == 0 and self.candidate_ms > 0:
            return "NEW"
        if self.candidate_ms == 0 and self.baseline_ms > 0:
            return "gone"
        if abs(self.rel_change) < NOISE_FLOOR:
            return "same"
        return "SLOWER" if self.rel_change > 0 else "faster"


def compare_spans(
    results_path: Path, baseline_ref: str, candidate_ref: str, *, workload: str | None = None
) -> list[SpanDelta]:
    """Attribute a ref-to-ref difference to individual server-side spans.

    This is the drill-down for a confusing headline number: it says *which*
    internal step moved, rather than only that the total did. Ordered by
    absolute time shifted, since that is what explains the headline.
    """
    base_rows = {
        (r["workload"], r["span"]): r
        for r in span_table(results_path, workload=workload, ref=baseline_ref)
    }
    cand_rows = {
        (r["workload"], r["span"]): r
        for r in span_table(results_path, workload=workload, ref=candidate_ref)
    }
    if not base_rows and not cand_rows:
        return []

    out: list[SpanDelta] = []
    for k in sorted(set(base_rows) | set(cand_rows)):
        b = base_rows.get(k)
        c = cand_rows.get(k)
        out.append(
            SpanDelta(
                workload=k[0],
                span=k[1],
                baseline_ms=b["total_ms"] if b else 0.0,
                candidate_ms=c["total_ms"] if c else 0.0,
                baseline_calls=b["calls"] if b else 0.0,
                candidate_calls=c["calls"] if c else 0.0,
            )
        )
    out.sort(key=lambda d: abs(d.delta_ms), reverse=True)
    return out


def query_table(
    results_path: Path, *, ref: str | None = None, graph: str | None = None
) -> list[dict[str, Any]]:
    """Per-(query, graph, ref) latencies from the query_api workload.

    Zero-row results are kept and marked, not filtered out: a query that matches
    nothing exercises a different path from one that matches, and both are
    measured on purpose.
    """
    acc: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for row in read_results(results_path):
        # Any workload that emits a `graphs` metric feeds this table — both the
        # graph-query workload (keyed by plant graph) and ingest_query (keyed by
        # its synthetic dataset). They never collide because the keys differ.
        if not row.get("ok") or not (row.get("metrics", {}) or {}).get("graphs"):
            continue
        r = row.get("ref_spec", "?")
        if ref and r != ref:
            continue
        for g, per_graph in (row.get("metrics", {}).get("graphs") or {}).items():
            if graph and g != graph:
                continue
            for qname, stats in (per_graph.get("queries") or {}).items():
                acc[(qname, g, r)].append(stats)

    out: list[dict[str, Any]] = []
    for (qname, g, r), stats_list in acc.items():
        medians = [s["median_ms"] for s in stats_list if "median_ms" in s]
        if not medians:
            continue
        rows_returned = stats_list[0].get("rows", 0)
        med = statistics.median(medians)
        # Peak-to-peak spread across repetitions, as a fraction of the median —
        # the same noise indicator the throughput report uses. A ref-to-ref
        # delta smaller than this is not a real effect.
        spread = (max(medians) - min(medians)) / med if med and len(medians) > 1 else 0.0
        out.append(
            {
                "query": qname,
                "graph": g,
                "ref": r,
                "rows": rows_returned,
                "empty": rows_returned == 0,
                "median_ms": med,
                "spread_pct": spread,
                "reps": len(medians),
            }
        )
    out.sort(key=lambda x: (x["query"], x["graph"], x["ref"]))
    return out


def dead_queries(results_path: Path, *, ref: str | None = None) -> list[str]:
    """Queries that returned zero rows on *every* graph.

    A zero-result on one graph is a deliberate measurement of the no-match path.
    Zero on all of them means the query matches nothing anywhere — it is
    exercising almost none of the machinery, and its very low latency would
    otherwise read as a fast query rather than a broken one.
    """
    by_query: dict[str, list[int]] = defaultdict(list)
    for r in query_table(results_path, ref=ref):
        by_query[r["query"]].append(r["rows"])
    return sorted(q for q, rows in by_query.items() if not any(rows))


@dataclass
class QueryDelta:
    query: str
    graph: str
    rows: int
    baseline_ms: float
    candidate_ms: float
    baseline_spread: float = 0.0
    candidate_spread: float = 0.0

    @property
    def empty(self) -> bool:
        return self.rows == 0

    @property
    def rel_change(self) -> float:
        if self.baseline_ms == 0:
            return float("inf") if self.candidate_ms else 0.0
        return (self.candidate_ms - self.baseline_ms) / self.baseline_ms

    @property
    def noisy(self) -> bool:
        # If either ref's own repetitions disagree by more than the effect being
        # claimed, the claim isn't supportable — the delta is within the jitter.
        return max(self.baseline_spread, self.candidate_spread) >= abs(self.rel_change)

    @property
    def verdict(self) -> str:
        if abs(self.rel_change) < NOISE_FLOOR:
            return "same"
        if self.noisy:
            return "noisy"
        return "SLOWER" if self.rel_change > 0 else "faster"


def compare_queries(
    results_path: Path, baseline_ref: str, candidate_ref: str
) -> list[QueryDelta]:
    """Per-query, per-graph comparison of two refs.

    Only compares like for like: a query is matched on (name, graph), so a
    zero-result case is compared against the same zero-result case.
    """
    base = {
        (r["query"], r["graph"]): r for r in query_table(results_path, ref=baseline_ref)
    }
    cand = {
        (r["query"], r["graph"]): r for r in query_table(results_path, ref=candidate_ref)
    }
    out: list[QueryDelta] = []
    for k in sorted(set(base) & set(cand)):
        b, c = base[k], cand[k]
        if b["rows"] != c["rows"]:
            # Same query, same graph, different row counts means the refs
            # disagree about the data — a correctness difference, not a
            # performance one. Surfacing it as a timing delta would bury it.
            continue
        out.append(
            QueryDelta(
                query=k[0], graph=k[1], rows=b["rows"],
                baseline_ms=b["median_ms"], candidate_ms=c["median_ms"],
                baseline_spread=b.get("spread_pct", 0.0),
                candidate_spread=c.get("spread_pct", 0.0),
            )
        )
    out.sort(key=lambda d: abs(d.rel_change), reverse=True)
    return out


def query_row_mismatches(
    results_path: Path, baseline_ref: str, candidate_ref: str
) -> list[dict[str, Any]]:
    """Queries where two refs returned different row counts.

    This is a correctness signal, not a perf one, and it matters more than any
    timing: the two versions disagree about what the graph contains.
    """
    base = {
        (r["query"], r["graph"]): r for r in query_table(results_path, ref=baseline_ref)
    }
    cand = {
        (r["query"], r["graph"]): r for r in query_table(results_path, ref=candidate_ref)
    }
    out = []
    for k in sorted(set(base) & set(cand)):
        if base[k]["rows"] != cand[k]["rows"]:
            out.append(
                {
                    "query": k[0],
                    "graph": k[1],
                    "baseline_rows": base[k]["rows"],
                    "candidate_rows": cand[k]["rows"],
                }
            )
    return out


def driver_grid(
    results_path: Path, *, ref: str | None = None, metric: str = "tick_latency_ms.median_ms"
) -> list[dict[str, Any]]:
    """Grid points from the driver_tick workload, one per cell, aggregated by ref.

    Each cell is one (drivers, rows_per_tick, period) point. `metric` is a
    dotted path into a point's metrics (e.g. `tick_latency_ms.p95_ms`,
    `driver_online_spread_s`, `jitter_ms.median_ms`, `tick_ingest_rps_median`).
    """
    acc: dict[tuple[str, str], list[float]] = defaultdict(list)
    meta: dict[str, dict] = {}
    for row in read_results(results_path):
        if row.get("workload") != "driver_tick" or not row.get("ok"):
            continue
        r = row.get("ref_spec", "?")
        if ref and r != ref:
            continue
        m = row.get("metrics", {})
        d, rpt, p = m.get("drivers"), m.get("rows_per_tick"), m.get("period_s")
        if d is None or p is None:
            continue
        key = f"d{d}_r{rpt}_p{int(p)}"
        v = _dig(m, metric)
        if isinstance(v, (int, float)):
            acc[(key, r)].append(float(v))
        meta.setdefault(key, {"drivers": d, "rows_per_tick": rpt, "period_s": p})

    out: list[dict[str, Any]] = []
    for (key, r), vals in acc.items():
        med = statistics.median(vals)
        out.append({
            "point": key, "ref": r, "metric": metric,
            "value": med, "reps": len(vals),
            "spread_pct": (max(vals) - min(vals)) / med if len(vals) > 1 and med else 0.0,
            **meta.get(key, {}),
        })
    out.sort(key=lambda x: (x.get("period_s") or 0, x.get("rows_per_tick") or 0, x.get("drivers") or 0))
    return out


@dataclass
class GridDelta:
    point: str
    drivers: int
    rows_per_tick: int
    period_s: float
    metric: str
    baseline: float
    candidate: float
    baseline_spread: float
    candidate_spread: float
    higher_is_better: bool

    @property
    def rel_change(self) -> float:
        if self.baseline == 0:
            return float("inf") if self.candidate else 0.0
        return (self.candidate - self.baseline) / abs(self.baseline)

    @property
    def improvement(self) -> float:
        return self.rel_change if self.higher_is_better else -self.rel_change

    @property
    def noisy(self) -> bool:
        return max(self.baseline_spread, self.candidate_spread) >= abs(self.rel_change)

    @property
    def verdict(self) -> str:
        if abs(self.improvement) < NOISE_FLOOR:
            return "same"
        if self.noisy:
            return "noisy"
        return "faster" if self.improvement > 0 else "SLOWER"


def compare_driver_grid(
    results_path: Path,
    baseline_ref: str,
    candidate_ref: str,
    *,
    metric: str = "tick_latency_ms.median_ms",
    higher_is_better: bool = False,
) -> list[GridDelta]:
    """Compare two refs at every grid point for a chosen driver metric."""
    base = {r["point"]: r for r in driver_grid(results_path, ref=baseline_ref, metric=metric)}
    cand = {r["point"]: r for r in driver_grid(results_path, ref=candidate_ref, metric=metric)}
    out: list[GridDelta] = []
    for k in sorted(set(base) & set(cand)):
        b, c = base[k], cand[k]
        out.append(GridDelta(
            point=k, drivers=b.get("drivers"), rows_per_tick=b.get("rows_per_tick"),
            period_s=b.get("period_s"), metric=metric,
            baseline=b["value"], candidate=c["value"],
            baseline_spread=b.get("spread_pct", 0.0), candidate_spread=c.get("spread_pct", 0.0),
            higher_is_better=higher_is_better,
        ))
    out.sort(key=lambda d: (d.period_s or 0, d.rows_per_tick or 0, d.drivers or 0))
    return out


def app_scale_points(results_path: Path, *, ref: str | None = None) -> list[dict[str, Any]]:
    """Flatten the app_scale escalation points, one row per (ref, N).

    Includes both successful counts and the first failing count (the ceiling
    boundary), so the table shows exactly where each branch ran out.
    """
    out: list[dict[str, Any]] = []
    for row in read_results(results_path):
        if row.get("workload") != "app_scale" or not row.get("ok"):
            continue
        r = row.get("ref_spec", "?")
        if ref and r != ref:
            continue
        m = row.get("metrics", {})
        for p in m.get("points", []):
            lat = p.get("received_to_completed_ms", {}) or {}
            disp = p.get("completed_to_endpoint_ms", {}) or {}
            out.append({
                "ref": r,
                "branch": m.get("branch", "?"),
                "n": p.get("n"),
                "complete": p.get("complete", False),
                "apps_online": p.get("apps_online", 0),
                "startup_s": p.get("startup_s"),
                "run_ms_median": lat.get("median_ms"),
                "run_ms_p95": lat.get("p95_ms"),
                "dispatch_ms_median": disp.get("median_ms"),
                "throughput_per_s": p.get("steady_throughput_per_s"),
                "app_memory_mb": p.get("app_memory_mb"),
                "per_app_memory_mb": p.get("per_app_memory_mb"),
                "failure": p.get("failure"),
            })
    out.sort(key=lambda x: (x["ref"], x["n"] or 0))
    return out


def app_scale_ceilings(results_path: Path) -> dict[str, dict[str, Any]]:
    """Per-ref breaking point: the largest N where all apps came online, and the
    smallest N that failed.

    Derived from the union of all points for a ref, so a base escalation plus a
    later extension (e.g. probing past 100 into swap) combine into one verdict
    rather than the later run overwriting the earlier ceiling.
    """
    complete: dict[str, list[int]] = defaultdict(list)
    failed: dict[str, list[int]] = defaultdict(list)
    branch: dict[str, str] = {}
    reason: dict[str, str] = {}
    for row in read_results(results_path):
        if row.get("workload") != "app_scale" or not row.get("ok"):
            continue
        ref = row.get("ref_spec", "?")
        m = row.get("metrics", {})
        branch[ref] = m.get("branch", "?")
        for p in m.get("points", []):
            n = p.get("n")
            if n is None:
                continue
            if p.get("complete"):
                complete[ref].append(n)
            else:
                failed[ref].append(n)
                reason.setdefault(ref, p.get("failure") or m.get("ceiling_reason") or "")

    out: dict[str, dict[str, Any]] = {}
    for ref in set(complete) | set(failed):
        max_ok = max(complete[ref], default=0)
        first_fail = min((n for n in failed[ref] if n > max_ok), default=None)
        out[ref] = {
            "branch": branch.get(ref, "?"),
            "ceiling": max_ok,
            "first_failure": first_fail,
            "reason": reason.get(ref, "not reached" if first_fail is None else ""),
        }
    return out


def summary_rows(results_path: Path) -> list[dict[str, Any]]:
    """Flat table of every (key, ref) aggregate, for display or export."""
    rows = []
    for (key, ref), agg in sorted(aggregate(results_path).items(), key=lambda kv: kv[0][0].label()):
        rows.append(
            {
                "workload": key.workload,
                "backend": key.backend,
                "topology": key.topology,
                "read_batch_size": key.read_batch_size,
                "ref": ref,
                "resolved": agg.ref_resolved,
                "metric": agg.metric,
                "median": agg.median,
                "n": len(agg.values),
                "spread_pct": agg.spread_pct,
                "failures": agg.failures,
            }
        )
    return rows

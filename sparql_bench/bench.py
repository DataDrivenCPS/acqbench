#!/usr/bin/env python3
"""Graph-store SPARQL benchmark: main vs graph-store-derived-cache.

Runs the DPR SPARQL corpus (harvested from acquirium/.run_logs) against a
disposable OxigraphGraphStore loaded with the DPR deployment model.

The two branches expose different query APIs, so the adapter below normalizes
them:

    main     store.sparql_query(q, use_union=<bool>)
    derived  store.sparql_query(q, include_dependencies=<bool>,
                                wait_for_fresh=<bool>)

Run inside the venv built for the branch under test:
    <wt>/<branch>/.venv/bin/python bench.py --branch main --out main.json
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import shutil
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path
from pathlib import Path as pathlib_Path

from rdflib import Graph, Literal, RDF, URIRef

from acquirium.Storage.graph_store import OxigraphGraphStore

HERE = Path(__file__).resolve().parent
DPR_TTL = Path("/Users/umsaka/Documents/DDCPS/NAWI/acquirium/dpr_scripts/union-dpr-model.ttl")


# ---------------------------------------------------------------- statistics
def percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    idx = (len(sorted_values) - 1) * fraction
    lo, hi = math.floor(idx), math.ceil(idx)
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (idx - lo)


def summarize(latencies_ms: list[float]) -> dict:
    if not latencies_ms:
        return {"count": 0}
    v = sorted(latencies_ms)
    return {
        "count": len(v),
        "min_ms": round(v[0], 3),
        "p50_ms": round(percentile(v, 0.50), 3),
        "p95_ms": round(percentile(v, 0.95), 3),
        "p99_ms": round(percentile(v, 0.99), 3),
        "max_ms": round(v[-1], 3),
        "mean_ms": round(statistics.fmean(v), 3),
    }


# ------------------------------------------------------------------ adapter
class Store:
    """Uniform query surface over the two branch APIs."""

    def __init__(self, branch: str, store_path: Path, env_root: Path):
        self.branch = branch
        t0 = time.perf_counter()
        self.store = OxigraphGraphStore(store_path=store_path, env_root=env_root)
        self.init_s = time.perf_counter() - t0
        self.is_derived = hasattr(self.store, "graph_status")

    def query(self, q: str, union: bool, wait_for_fresh: bool = False):
        if self.is_derived:
            return self.store.sparql_query(
                q, include_dependencies=union, wait_for_fresh=wait_for_fresh
            )
        return self.store.sparql_query(q, use_union=union)

    def insert_ttl(self, text: str, replace: bool = False):
        return self.store.insert_graph(text, format="turtle", replace=replace)

    def insert_graph_obj(self, g: Graph, replace: bool = False):
        return self.store.insert_graph(g, format="turtle", replace=replace)

    def status(self):
        return self.store.graph_status() if self.is_derived else None

    def close(self):
        try:
            self.store.close()
        except Exception:
            pass


# ---------------------------------------------------------------- workloads
def timed(fn, *a, **kw):
    t0 = time.perf_counter_ns()
    out = fn(*a, **kw)
    return (time.perf_counter_ns() - t0) / 1e6, out


def run_with_timeout(store: Store, q: str, union: bool, budget_s: float):
    """Run one query, abandoning it if it blows the budget.

    Oxigraph executes inside Rust and cannot be interrupted from Python, so an
    over-budget query is left running on a daemon thread and the process exits
    at the end of the pass. That is why probing is a separate process from the
    timed run: abandoned work never pollutes the numbers we keep.
    """
    box: dict = {}

    def work():
        try:
            t0 = time.perf_counter_ns()
            out = store.query(q, union)
            box["ms"] = (time.perf_counter_ns() - t0) / 1e6
            box["rows"] = len(out["rows"])
        except Exception as exc:
            box["error"] = f"{type(exc).__name__}: {exc}"

    t = threading.Thread(target=work, daemon=True)
    t.start()
    t.join(budget_s)
    if t.is_alive():
        return None, None, f"timeout>{budget_s:g}s"
    return box.get("ms"), box.get("rows"), box.get("error")


def probe_phase(store: Store, corpus: list[dict], budget_s: float,
                jsonl: pathlib_Path, start_index: int) -> bool:
    """One timed execution per query, appended to a JSONL as we go.

    Returns True when the whole slice completed. On main, an abandoned query
    still holds the store's global RLock, so every later query blocks on it
    forever — one timeout poisons the rest of the process. So we record the
    timeout, stop, and let the driver restart us at the next index.
    """
    for offset, entry in enumerate(corpus):
        idx = start_index + offset
        ms, rows, err = run_with_timeout(store, entry["query"], entry["union"], budget_s)
        rec = {"index": idx, "id": entry["id"], "union": entry["union"],
               "probe_ms": ms, "rows": rows, "error": err}
        with open(jsonl, "a") as fh:
            fh.write(json.dumps(rec) + "\n")
        shown = "TIMEOUT" if err and "timeout" in err else (f"{ms:.1f}ms" if ms else "ERR")
        print(f"  probe [{idx}] {entry['id']} {shown} rows={rows} {err or ''}",
              file=sys.stderr, flush=True)
        if err and "timeout" in err:
            print(f"  -> timeout at index {idx}; restarting process to release the lock",
                  file=sys.stderr, flush=True)
            return False
    return True


def plan_reps(probe_ms: float | None, reps: int, warmup: int) -> tuple[int, int]:
    """Spend roughly a constant time budget per query, not a constant rep count."""
    if probe_ms is None:
        return 0, 1
    if probe_ms > 2000:
        return 0, 2
    if probe_ms > 200:
        return 1, 3
    if probe_ms > 20:
        return 1, 5
    return warmup, reps


def serial_phase(store: Store, corpus: list[dict], warmup: int, reps: int,
                 probes: dict[str, float | None] | None = None) -> list[dict]:
    """Per-query latency, one thread, no contention."""
    results = []
    for i, entry in enumerate(corpus, 1):
        q, union = entry["query"], entry["union"]
        rows = None
        err = None
        w, r = plan_reps(probes.get(entry["id"]) if probes else None, reps, warmup) \
            if probes else (warmup, reps)
        try:
            for _ in range(w):
                store.query(q, union)
            lats = []
            for _ in range(r):
                ms, out = timed(store.query, q, union)
                lats.append(ms)
                rows = len(out["rows"])
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            lats = []
        results.append({
            "id": entry["id"],
            "union": union,
            "log_count": entry["count"],
            "nchars": entry["nchars"],
            "rows": rows,
            "error": err,
            **summarize(lats),
        })
        print(f"  [{i}/{len(corpus)}] {entry['id']} union={union} "
              f"rows={rows} p50={results[-1].get('p50_ms')}ms {err or ''}",
              file=sys.stderr, flush=True)
    return results


def concurrency_phase(store: Store, mix: list[dict], readers: int, ops_per_reader: int,
                      wait_for_fresh: bool = False) -> dict:
    """N reader threads hammering a frequency-weighted query mix."""
    barrier = threading.Barrier(readers + 1)
    lat_lock = threading.Lock()
    all_lat: list[float] = []
    errors: list[str] = []

    def reader(seed: int):
        rng = random.Random(seed)
        local: list[float] = []
        local_err: list[str] = []
        barrier.wait()
        for _ in range(ops_per_reader):
            entry = rng.choice(mix)
            t0 = time.perf_counter_ns()
            try:
                store.query(entry["query"], entry["union"], wait_for_fresh=wait_for_fresh)
            except Exception as exc:
                local_err.append(f"{type(exc).__name__}: {exc}")
            local.append((time.perf_counter_ns() - t0) / 1e6)
        with lat_lock:
            all_lat.extend(local)
            errors.extend(local_err)

    threads = [threading.Thread(target=reader, args=(1000 + i,)) for i in range(readers)]
    for t in threads:
        t.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0

    ops = readers * ops_per_reader
    return {
        "readers": readers,
        "ops": ops,
        "wait_for_fresh": wait_for_fresh,
        "elapsed_s": round(elapsed, 4),
        "throughput_qps": round(ops / elapsed, 2) if elapsed else 0.0,
        "latency": summarize(all_lat),
        "errors": errors[:5],
        "error_count": len(errors),
    }


def read_under_write_phase(store: Store, mix: list[dict], readers: int, ops_per_reader: int,
                           writes: int, wait_for_fresh: bool = False) -> dict:
    """Readers under a concurrent writer, i.e. cache invalidation in flight."""
    barrier = threading.Barrier(readers + 2)
    lock = threading.Lock()
    read_lat: list[float] = []
    write_lat: list[float] = []
    errors: list[str] = []
    stop = threading.Event()

    def reader(seed: int):
        rng = random.Random(seed)
        local, local_err = [], []
        barrier.wait()
        for _ in range(ops_per_reader):
            entry = rng.choice(mix)
            t0 = time.perf_counter_ns()
            try:
                store.query(entry["query"], entry["union"], wait_for_fresh=wait_for_fresh)
            except Exception as exc:
                local_err.append(f"{type(exc).__name__}: {exc}")
            local.append((time.perf_counter_ns() - t0) / 1e6)
        with lock:
            read_lat.extend(local)
            errors.extend(local_err)

    def writer():
        local, local_err = [], []
        ns = "urn:acquirium:bench:"
        barrier.wait()
        for i in range(writes):
            if stop.is_set():
                break
            g = Graph()
            s = URIRef(f"{ns}w{i}")
            g.add((s, RDF.type, URIRef(f"{ns}Thing")))
            g.add((s, URIRef(f"{ns}value"), Literal(i)))
            t0 = time.perf_counter_ns()
            try:
                store.insert_graph_obj(g)
            except Exception as exc:
                local_err.append(f"{type(exc).__name__}: {exc}")
            local.append((time.perf_counter_ns() - t0) / 1e6)
            time.sleep(0.05)
        with lock:
            write_lat.extend(local)
            errors.extend(local_err)

    threads = [threading.Thread(target=reader, args=(2000 + i,)) for i in range(readers)]
    wt = threading.Thread(target=writer)
    for t in threads:
        t.start()
    wt.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in threads:
        t.join()
    stop.set()
    wt.join()
    elapsed = time.perf_counter() - t0

    return {
        "readers": readers,
        "wait_for_fresh": wait_for_fresh,
        "elapsed_s": round(elapsed, 4),
        "read_throughput_qps": round(readers * ops_per_reader / elapsed, 2) if elapsed else 0.0,
        "read_latency": summarize(read_lat),
        "write_latency": summarize(write_lat),
        "errors": errors[:5],
        "error_count": len(errors),
    }


# --------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--branch", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--corpus", default=str(HERE / "extracted_queries.json"))
    ap.add_argument("--reps", type=int, default=10)
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--readers", default="1,2,4,8,16")
    ap.add_argument("--ops-per-reader", type=int, default=40)
    ap.add_argument("--mix-size", type=int, default=12)
    # Writes on main synchronously re-materialize the union graph (seconds each),
    # so the read-under-write phase gets its own smaller op budget to stay bounded.
    ap.add_argument("--ruw-ops", type=int, default=15)
    ap.add_argument("--ruw-writes", type=int, default=12)
    ap.add_argument("--skip-serial", action="store_true")
    ap.add_argument("--probe-only", action="store_true",
                    help="one watchdog-bounded execution per query, then exit")
    ap.add_argument("--probe-budget", type=float, default=25.0)
    ap.add_argument("--probes", default=None,
                    help="probe JSON used to size reps per query")
    # Abandoned over-budget queries keep burning cores, so the probe pass runs in
    # slices and the process is restarted between them to reap the runaways.
    ap.add_argument("--slice", default=None, help="corpus slice, e.g. 40:80")
    args = ap.parse_args()

    corpus = json.load(open(args.corpus))
    if args.slice:
        a, _, b = args.slice.partition(":")
        corpus = corpus[int(a or 0): int(b) if b else None]
    workdir = Path(tempfile.mkdtemp(prefix=f"gsbench-{args.branch}-"))
    report: dict = {"branch": args.branch, "workdir": str(workdir),
                    "python": sys.version.split()[0], "pid": os.getpid()}

    try:
        print(f"[{args.branch}] opening store in {workdir}", file=sys.stderr)
        store = Store(args.branch, workdir / "graph", workdir / "env")
        report["is_derived"] = store.is_derived
        report["store_init_s"] = round(store.init_s, 3)
        print(f"[{args.branch}] store init {store.init_s:.1f}s derived={store.is_derived}",
              file=sys.stderr)

        # ---- insert (timed; the user wants this kept even though it's not the focus)
        ttl = DPR_TTL.read_text()
        ins_ms, ins_out = timed(store.insert_ttl, ttl)
        report["insert"] = {"wall_ms": round(ins_ms, 1), "result": {
            k: (v if isinstance(v, (int, float, bool, str)) else str(v))
            for k, v in (ins_out or {}).items()}}
        print(f"[{args.branch}] insert {ins_ms:.0f}ms -> {ins_out}", file=sys.stderr)

        # ---- first derived-cache / union materialization (one-time cost)
        probe = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"
        first_ms, first_out = timed(store.query, probe, True, True)
        report["first_union_query_ms"] = round(first_ms, 1)
        report["first_union_rows"] = first_out["rows"]
        print(f"[{args.branch}] first union/fresh query {first_ms:.0f}ms -> {first_out['rows']}",
              file=sys.stderr)

        # graph sizes, for the semantic-divergence note
        sizes = {}
        for label, u in (("data_only", False), ("with_dependencies", True)):
            try:
                _, o = timed(store.query, probe, u)
                sizes[label] = o["rows"][0][0]
            except Exception as exc:
                sizes[label] = f"error: {exc}"
        report["triple_counts"] = {k: (int(str(v)) if str(v).lstrip('-').isdigit() else str(v))
                                   for k, v in sizes.items()}
        report["graph_status"] = store.status()
        print(f"[{args.branch}] triples {report['triple_counts']}", file=sys.stderr)

        # ---- probe pass: bound every query once, then exit before the timed run
        if args.probe_only:
            start = int(args.slice.split(":")[0]) if args.slice else 0
            print(f"[{args.branch}] probe pass over {len(corpus)} queries from index {start} "
                  f"(budget {args.probe_budget}s)", file=sys.stderr)
            done = probe_phase(store, corpus, args.probe_budget,
                               pathlib_Path(args.out), start)
            report["probe_slice_complete"] = done
            # 0 = slice finished, 7 = stopped early on a timeout, driver resumes
            sys.exit(0 if done else 7)

        probes = None
        if args.probes:
            probes = {p["id"]: p.get("probe_ms") for p in json.load(open(args.probes))}

        # ---- serial per-query latency
        if not args.skip_serial:
            print(f"[{args.branch}] serial phase over {len(corpus)} queries", file=sys.stderr)
            report["serial"] = serial_phase(store, corpus, args.warmup, args.reps, probes)

        # ---- concurrency, on a frequency-weighted mix of the hottest queries
        ok = {r["id"] for r in report.get("serial", []) if not r.get("error")}
        pool = [c for c in corpus if not ok or c["id"] in ok]
        mix = pool[: args.mix_size]
        report["mix_ids"] = [m["id"] for m in mix]
        report["concurrency"] = []
        for n in [int(x) for x in args.readers.split(",")]:
            res = concurrency_phase(store, mix, n, args.ops_per_reader)
            report["concurrency"].append(res)
            print(f"[{args.branch}] readers={n} qps={res['throughput_qps']} "
                  f"p50={res['latency'].get('p50_ms')} p95={res['latency'].get('p95_ms')}",
                  file=sys.stderr)

        # ---- derived-only: strict (wait_for_fresh) reads
        if store.is_derived:
            report["concurrency_fresh"] = []
            for n in [int(x) for x in args.readers.split(",")]:
                res = concurrency_phase(store, mix, n, args.ops_per_reader, wait_for_fresh=True)
                report["concurrency_fresh"].append(res)
                print(f"[{args.branch}] FRESH readers={n} qps={res['throughput_qps']} "
                      f"p50={res['latency'].get('p50_ms')}", file=sys.stderr)

        # ---- read under concurrent write
        report["read_under_write"] = []
        for n in (4, 8):
            res = read_under_write_phase(store, mix, n, args.ruw_ops, writes=args.ruw_writes)
            report["read_under_write"].append(res)
            print(f"[{args.branch}] RUW readers={n} qps={res['read_throughput_qps']} "
                  f"p50={res['read_latency'].get('p50_ms')} "
                  f"write_p50={res['write_latency'].get('p50_ms')}", file=sys.stderr)
        if store.is_derived:
            res = read_under_write_phase(store, mix, 8, args.ruw_ops,
                                         writes=args.ruw_writes, wait_for_fresh=True)
            res["label"] = "wait_for_fresh"
            report["read_under_write"].append(res)
            print(f"[{args.branch}] RUW-FRESH readers=8 qps={res['read_throughput_qps']} "
                  f"p50={res['read_latency'].get('p50_ms')}", file=sys.stderr)

        store.close()
    finally:
        # In probe mode --out is the append-only JSONL the probe writes itself.
        if not args.probe_only:
            Path(args.out).write_text(json.dumps(report, indent=2, default=str))
            print(f"[{args.branch}] wrote {args.out}", file=sys.stderr)
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()

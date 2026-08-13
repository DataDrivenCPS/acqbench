#!/usr/bin/env python3
"""Head-of-line blocking: does one expensive query stall cheap concurrent ones?

This isolates the architectural difference. On main, sparql_query holds the
store's global RLock across execution *and* row materialization, so a heavy
query should block every other reader for its full duration. On
graph-store-derived-cache the lock is held only for the lazy Oxigraph call, so
cheap queries should keep flowing.

Measured: latency of a cheap query, first alone, then while a background thread
runs an expensive query in a loop.

    <wt>/<branch>/.venv/bin/python hol_test.py --branch main --out hol_main.json
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
import time
from pathlib import Path

from bench import DPR_TTL, Store, summarize, timed

HERE = Path(__file__).resolve().parent

# Cheap: a single bound-predicate lookup. Milliseconds.
CHEAP = ("SELECT ?s WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
         "<http://data.ashrae.org/standard223#Sensor> } LIMIT 50")


def pick_heavy(corpus, probes, lo=1200.0):
    """Slowest query that still completed inside the probe budget."""
    by_id = {p["id"]: p for p in probes}
    cands = [(by_id[q["id"]]["probe_ms"], q) for q in corpus
             if by_id.get(q["id"], {}).get("probe_ms")]
    cands.sort(key=lambda t: -t[0])
    for ms, q in cands:
        if ms >= lo:
            return q, ms
    return cands[0][1], cands[0][0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--branch", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--seconds", type=float, default=20.0)
    ap.add_argument("--baseline-reps", type=int, default=40)
    ap.add_argument("--heavy-id", default=None,
                    help="pin the expensive query so both branches use the same one")
    args = ap.parse_args()

    RES = HERE / "results"
    corpus = json.loads((RES / "viable_corpus.json").read_text())
    probes = json.loads((RES / f"probe_{args.branch}.json").read_text())
    if args.heavy_id:
        heavy = next(q for q in corpus if q["id"] == args.heavy_id)
        heavy_ms = next((p["probe_ms"] for p in probes if p["id"] == args.heavy_id), None)
    else:
        heavy, heavy_ms = pick_heavy(corpus, probes)

    import tempfile, shutil
    wd = Path(tempfile.mkdtemp(prefix=f"hol-{args.branch}-"))
    rep = {"branch": args.branch, "heavy_id": heavy["id"], "heavy_probe_ms": heavy_ms,
           "heavy_union": heavy["union"]}
    try:
        store = Store(args.branch, wd / "graph", wd / "env")
        rep["is_derived"] = store.is_derived
        store.insert_ttl(DPR_TTL.read_text())
        store.query("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }", True, True)  # warm

        # ---- baseline: cheap query alone
        for _ in range(5):
            store.query(CHEAP, True)
        base = []
        for _ in range(args.baseline_reps):
            ms, out = timed(store.query, CHEAP, True)
            base.append(ms)
        rep["cheap_rows"] = len(out["rows"])
        rep["baseline"] = summarize(base)
        print(f"[{args.branch}] baseline cheap p50={rep['baseline']['p50_ms']:.2f}ms",
              file=sys.stderr)

        # ---- under load: cheap query while a heavy one runs in a loop
        stop = threading.Event()
        heavy_lat, heavy_err = [], []

        def hog():
            while not stop.is_set():
                t0 = time.perf_counter_ns()
                try:
                    store.query(heavy["query"], heavy["union"])
                except Exception as exc:
                    heavy_err.append(f"{type(exc).__name__}: {exc}")
                heavy_lat.append((time.perf_counter_ns() - t0) / 1e6)

        t = threading.Thread(target=hog, daemon=True)
        t.start()
        time.sleep(0.4)  # let the heavy query get in flight first

        under, deadline = [], time.perf_counter() + args.seconds
        while time.perf_counter() < deadline:
            ms, _ = timed(store.query, CHEAP, True)
            under.append(ms)
        stop.set()
        t.join(timeout=60)

        rep["under_load"] = summarize(under)
        rep["heavy"] = summarize(heavy_lat)
        rep["heavy_errors"] = heavy_err[:3]
        b50 = rep["baseline"]["p50_ms"] or 0.001
        rep["blocking_factor_p50"] = round(rep["under_load"]["p50_ms"] / b50, 2)
        rep["cheap_qps_under_load"] = round(len(under) / args.seconds, 2)
        print(f"[{args.branch}] under load cheap p50={rep['under_load']['p50_ms']:.2f}ms "
              f"p95={rep['under_load']['p95_ms']:.2f}ms "
              f"blocking={rep['blocking_factor_p50']}x "
              f"cheap_qps={rep['cheap_qps_under_load']} "
              f"heavy_n={rep['heavy']['count']}", file=sys.stderr)
        store.close()
    finally:
        Path(args.out).write_text(json.dumps(rep, indent=2, default=str))
        shutil.rmtree(wd, ignore_errors=True)
        print(f"[{args.branch}] wrote {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()

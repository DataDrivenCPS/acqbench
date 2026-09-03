#!/usr/bin/env python3
"""30-query GET /timeseries workload for the DPR dataset.

    python queries.py gen  <base_url> <lookup.json> <out queries.json>
    python queries.py run  <base_url> <queries.json> <reps> <warmup> <out result.json> [pg]

`gen` discovers streams on a live server and freezes absolute parameters so
the identical set can be replayed against any backend later. `run` replays it:
per query 1 cold request (recorded separately), <warmup> discarded, <reps>
timed; reports latency stats, rows, bytes, throughput, and server CPU seconds
consumed over the timed reps (server session + postgres processes if `pg`).
"""
from __future__ import annotations

import json
import statistics
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import pyarrow as pa
import requests
from pyarrow import ipc

ARROW = "application/vnd.apache.arrow.stream"
SOURCE_ID = "dpr-trailer"
_REF_NS = uuid.UUID("6a8f3c2e-4b1d-5e7f-9012-3a4b5c6d7e8f")
UNMAPPED_NUMERIC = "UV-Ultraviolet Intensity (mW/cm^2)"


def ref_uri(name: str) -> str:
    return "urn:acquirium#" + str(uuid.uuid5(_REF_NS, f"{SOURCE_ID}:{name}"))


def read_table(content: bytes) -> pa.Table:
    return ipc.RecordBatchStreamReader(pa.BufferReader(content)).read_all()


def get(s, base, params):
    r = s.get(f"{base}/timeseries?{urlencode(params)}", headers={"Accept": ARROW}, timeout=3600)
    r.raise_for_status()
    return r


def cpu_seconds(pg: bool) -> float:
    """CPU seconds of the acquirium server session (env SERVER_SID) + postgres."""
    import os
    sid = os.environ.get("SERVER_SID")
    out = subprocess.run(["ps", "-eo", "sid,comm,cputimes"], capture_output=True, text=True).stdout
    tot = 0.0
    for line in out.splitlines()[1:]:
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        if (sid and parts[0] == sid) or (pg and parts[1].startswith("postgres")):
            tot += float(parts[2])
    return tot


# ----------------------------------------------------------------- gen

def gen(base: str, lookup: str, out: str) -> None:
    s = requests.Session()
    with open(lookup) as f:
        mapping = json.load(f)
    uris = list(mapping)
    info = s.post(f"{base}/timeseries_info", json={"uris": uris}, timeout=600).json()
    live = {u: v for u, v in info.items() if (v or {}).get("row_count", 0) > 0}
    if not live:
        sys.exit("no mapped stream has data")

    def numeric_rows(u):
        return read_table(get(s, base, dict(uri=u, limit=1, value_mode="numeric")).content).num_rows

    ranked = sorted(live.items(), key=lambda kv: -kv[1]["row_count"])
    numeric = [u for u, _ in ranked if "status" not in u and "state" not in u and numeric_rows(u)]
    text = [u for u, _ in ranked if ("status" in u or "state" in u) and not numeric_rows(u)]
    if len(numeric) < 2 or not text:
        sys.exit(f"stream discovery failed: numeric={len(numeric)} text={len(text)}")
    P1, P2, T1 = numeric[0], numeric[1], text[0]
    R1 = ref_uri(UNMAPPED_NUMERIC)
    r1_rows = read_table(get(s, base, dict(uri=R1, limit=1)).content).num_rows
    if not r1_rows:
        sys.exit(f"computed ref uri for {UNMAPPED_NUMERIC!r} has no data")

    # anchor on the newest real row so windows never fall into a void
    t = read_table(get(s, base, dict(uri=P1, limit=1, order="desc")).content)
    anchor = t.column("ts")[0].as_py()
    if anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=timezone.utc)
    first = read_table(get(s, base, dict(uri=P1, limit=1, order="asc")).content).column("ts")[0].as_py()

    def win(uri, delta, back=timedelta(0), **kw):
        end = anchor - back
        return dict(uri=uri, start=(end - delta).isoformat(), end=end.isoformat(), **kw)

    D, H, M = timedelta(days=1), timedelta(hours=1), timedelta(minutes=1)
    Q = [
        ("P1 full range", dict(uri=P1)),
        ("P1 full range desc", dict(uri=P1, order="desc")),
        ("P1 90 days", win(P1, 90 * D)),
        ("P1 30 days", win(P1, 30 * D)),
        ("P1 7 days", win(P1, 7 * D)),
        ("P1 1 day", win(P1, D)),
        ("P1 6 hours", win(P1, 6 * H)),
        ("P1 1 hour", win(P1, H)),
        ("P1 10 min", win(P1, 10 * M)),
        ("P1 1 min", win(P1, M)),
        ("P1 1 day, 45 days back", win(P1, D, back=45 * D)),
        ("P1 1 hour, 45 days back", win(P1, H, back=45 * D)),
        ("P2 30 days", win(P2, 30 * D)),
        ("P2 1 day", win(P2, D)),
        ("P2 1 hour", win(P2, H)),
        ("P2 numeric full", dict(uri=P2, value_mode="numeric")),
        ("T1 text full range", dict(uri=T1)),
        ("T1 text 1 day", win(T1, D)),
        ("T1 text 1 hour", win(T1, H)),
        ("T1 text mode 1 day", win(T1, D, value_mode="text")),
        ("R1 ref-uri full range", dict(uri=R1)),
        ("R1 ref-uri 7 days", win(R1, 7 * D)),
        ("P1 latest 1 desc", dict(uri=P1, limit=1, order="desc")),
        ("P1 latest 100 desc", dict(uri=P1, limit=100, order="desc")),
        ("P1 limit 1000 asc", dict(uri=P1, limit=1000)),
        ("P1 30 days limit 10000", win(P1, 30 * D, limit=10000)),
        ("P1 numeric 30 days", win(P1, 30 * D, value_mode="numeric")),
        ("P1 coalesce 1 day", win(P1, D, value_mode="coalesce")),
        ("EMPTY P1 window after data", dict(uri=P1, start=(anchor + 30 * D).isoformat(),
                                            end=(anchor + 60 * D).isoformat())),
        ("EMPTY unregistered uri", dict(uri="urn:dpr/does-not-exist")),
    ]
    assert len(Q) == 30
    doc = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "anchor": anchor.isoformat(), "first_row": first.isoformat(),
        "streams": {"P1": P1, "P2": P2, "T1": T1, "R1": R1, "R1_name": UNMAPPED_NUMERIC},
        "stream_rows": {k: live.get(v, {}).get("row_count") for k, v in
                        {"P1": P1, "P2": P2, "T1": T1}.items()},
        "queries": [{"id": i + 1, "label": l, "params": p, "expect_empty": l.startswith("EMPTY")}
                    for i, (l, p) in enumerate(Q)],
    }
    with open(out, "w") as f:
        json.dump(doc, f, indent=1)
    print(f"wrote {out}: P1={P1} P2={P2} T1={T1} anchor={anchor}")


# ----------------------------------------------------------------- run

def pct(v, p):
    s = sorted(v)
    return s[min(int(round((len(s) - 1) * p)), len(s) - 1)]


def run(base: str, qfile: str, reps: int, warmup: int, out: str, pg: bool) -> None:
    with open(qfile) as f:
        doc = json.load(f)
    s = requests.Session()
    results = []
    for q in doc["queries"]:
        params = q["params"]
        entry = {"id": q["id"], "label": q["label"], "params": params}
        try:
            t0 = time.perf_counter()
            r = get(s, base, params)
            entry["cold_ms"] = round((time.perf_counter() - t0) * 1e3, 2)
            tbl = read_table(r.content)
            entry["rows"], entry["bytes"] = tbl.num_rows, len(r.content)
            for _ in range(warmup):
                get(s, base, params)
            cpu0 = cpu_seconds(pg)
            lat = []
            for _ in range(reps):
                t0 = time.perf_counter()
                get(s, base, params)
                lat.append((time.perf_counter() - t0) * 1e3)
            cpu1 = cpu_seconds(pg)
            entry.update({
                "n": reps, "lat_ms": [round(x, 2) for x in lat],
                "p50_ms": round(pct(lat, .5), 2), "mean_ms": round(statistics.mean(lat), 2),
                "p95_ms": round(pct(lat, .95), 2), "min_ms": round(min(lat), 2),
                "max_ms": round(max(lat), 2),
                "stdev_ms": round(statistics.pstdev(lat), 2),
                "rows_per_s": round(entry["rows"] / (statistics.mean(lat) / 1e3), 0),
                "mb_per_s": round(entry["bytes"] / 1e6 / (statistics.mean(lat) / 1e3), 2),
                "server_cpu_s_per_req": round((cpu1 - cpu0) / reps, 4),
            })
            if q["expect_empty"] != (entry["rows"] == 0):
                entry["warning"] = f"expected empty={q['expect_empty']} but rows={entry['rows']}"
        except Exception as e:
            entry["error"] = f"{type(e).__name__}: {e}"
        results.append(entry)
        print(f"# {q['id']:2d} {q['label']:<30} rows={entry.get('rows','?'):>8} "
              f"cold={entry.get('cold_ms','?')} p50={entry.get('p50_ms','?')} ms "
              f"{entry.get('error','')}{entry.get('warning','')}", file=sys.stderr, flush=True)
    with open(out, "w") as f:
        json.dump({"queries_file": qfile, "reps": reps, "warmup": warmup,
                   "ran": datetime.now(timezone.utc).isoformat(), "results": results}, f, indent=1)


if __name__ == "__main__":
    if sys.argv[1] == "gen":
        gen(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        run(sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), sys.argv[6],
            len(sys.argv) > 7 and sys.argv[7] == "pg")

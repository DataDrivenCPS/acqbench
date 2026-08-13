#!/usr/bin/env python3
"""Query-latency probe against a running acquirium server (branch-agnostic).

Exercises the two HTTP query templates the API exposes — POST /timeseries_info
and GET /timeseries — the way a real client does, including the Arrow stream
accept header, so the numbers are comparable across branches whose storage
schemas differ. Emits a JSON blob on stdout.

argv: <port> <dpr_scripts_dir> <info_reps> <ts_reps> <warmup>
"""

from __future__ import annotations

import json
import statistics
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests

ARROW = "application/vnd.apache.arrow.stream"

# Mirrors acquirium's internals: stream ref URIs are a deterministic uuid5, so a
# client can address a stream without asking the server to enumerate them.
_REF_NS = uuid.UUID("6a8f3c2e-4b1d-5e7f-9012-3a4b5c6d7e8f")
SOURCE_ID = "dpr-trailer"          # DPRTrailerCSVDriver.SOURCE_ID
_URI_UNSAFE = str.maketrans({"\n": "_", "\r": "_", "\t": "_", " ": "_",
                             "(": "", ")": "", "#": "", "?": "", "<": "", ">": "",
                             '"': "", "'": "", "\\": "", "`": ""})
# Candidate streams for the numeric-valued shape, already in _safe_name form.
# The first group is deliberately *unmapped* by property_to_column.json: mapped
# points get re-registered by the driver's lookup without a value_kind, which
# pins them to 'text' and makes value_mode="numeric" return nothing.
NUMERIC_COLUMNS = [
    "UV-Ultraviolet_Intensity_mW/cm^2", "UV-LP-Current_Draw_A", "MULT-TOC-2_ppm",
    "UF-Feed_Pressure_psi", "INF-Filter_Pressure_psi", "BAF-C1_Flow_GPM",
]


def safe_name(s: str) -> str:
    return " ".join(s.split()).translate(_URI_UNSAFE)


def compute_ref_uri(source_id: str, ref_name: str) -> str:
    return "urn:acquirium#" + str(uuid.uuid5(_REF_NS, f"{source_id}:{ref_name}"))


def pct(v: list[float], p: float) -> float:
    s = sorted(v)
    return round(s[min(int(len(s) * p), len(s) - 1)] * 1e3, 2)


def stat(v: list[float]) -> dict:
    return {"p50_ms": pct(v, .5), "mean_ms": round(statistics.mean(v) * 1e3, 2),
            "p95_ms": pct(v, .95), "min_ms": round(min(v) * 1e3, 2),
            "max_ms": round(max(v) * 1e3, 2), "n": len(v)}


def last_ts(s, base: str, uri: str):
    """Timestamp of the newest actual row of *uri* (one row over the wire)."""
    try:
        import pyarrow as pa
        from pyarrow import ipc
        r = s.get(f"{base}/timeseries?{urlencode({'uri': uri, 'limit': 1, 'order': 'desc'})}",
                  headers={"Accept": ARROW}, timeout=1800)
        t = ipc.RecordBatchStreamReader(pa.BufferReader(r.content)).read_all()
        if t.num_rows:
            v = t.column("ts")[0].as_py()
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return None


def count_rows(content: bytes) -> int:
    try:
        import pyarrow as pa
        from pyarrow import ipc
        return ipc.RecordBatchStreamReader(pa.BufferReader(content)).read_all().num_rows
    except Exception:
        return -1


def main() -> int:
    port, scripts, info_reps, ts_reps, warmup = (
        int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]))
    base = f"http://127.0.0.1:{port}"
    s = requests.Session()
    out: dict = {}

    with open(f"{scripts}/property_to_column.json") as f:
        uris = list(json.load(f).keys())

    # ---- discover which streams actually carry data, and their time span ----
    info = s.post(f"{base}/timeseries_info", json={"uris": uris}, timeout=600).json()
    live = {u: v for u, v in info.items() if (v or {}).get("row_count", 0) > 0}
    out["streams"] = {"requested": len(uris), "with_data": len(live),
                      "total_rows": sum(v["row_count"] for v in live.values())}
    if not live:
        out["error"] = "no stream from property_to_column.json resolved to data"
        print(json.dumps(out))
        return 0

    ranked = sorted(live.items(), key=lambda kv: -kv[1]["row_count"])
    dense_uri, dense = ranked[0]
    sparse_uri, sparse = ranked[-1]

    # Anchor every window on a REAL row, not on (earliest+latest)/2: DPR exports
    # carry occasional bogus timestamps (decades off), so the midpoint of the
    # reported range can land in a void and silently measure empty queries.
    anchor = last_ts(s, base, dense_uri)
    if anchor is None:
        anchor = datetime.fromisoformat(dense["latest"].replace("Z", "+00:00"))
    out["anchor"] = anchor.isoformat()
    out["reported_range"] = {"earliest": dense.get("earliest"), "latest": dense.get("latest")}

    # value_mode="numeric" filters numeric_value IS NOT NULL, and every point_uri
    # in property_to_column.json maps to a State/Status *text* column — so the
    # numeric shape has to address a stream directly. Stream ref URIs are a
    # deterministic uuid5 of (source_id, ref_name), so we can compute them
    # without enumerating anything server-side.
    def numeric_capable(u: str) -> bool:
        r = s.get(f"{base}/timeseries?{urlencode({'uri': u, 'limit': 1, 'value_mode': 'numeric'})}",
                  headers={"Accept": ARROW}, timeout=600)
        return r.status_code == 200 and count_rows(r.content) > 0

    numeric_uri, numeric_name = None, None
    for col in NUMERIC_COLUMNS:
        cand = compute_ref_uri(SOURCE_ID, safe_name(col))
        if numeric_capable(cand):
            numeric_uri, numeric_name = cand, col
            break

    # Branch-comparable measure of the value_kind problem: how many of the
    # semantically-mapped points can answer a numeric-mode query at all?
    sample = [u for u, _ in ranked[:20]]
    mapped_numeric = sum(1 for u in sample if numeric_capable(u))
    out["value_kind_probe"] = {
        "mapped_points_sampled": len(sample),
        "mapped_points_numeric_capable": mapped_numeric,
        "numeric_stream_used": numeric_name, "numeric_stream_uri": numeric_uri,
    }

    # ---- template 1: timeseries_info -------------------------------------
    lat_batch, lat_single = [], []
    for i in range(info_reps + warmup):
        t0 = time.perf_counter()
        s.post(f"{base}/timeseries_info", json={"uris": uris}, timeout=600)
        if i >= warmup:
            lat_batch.append(time.perf_counter() - t0)
    single_uris = [u for u, _ in ranked[:20]] or [dense_uri]
    for i in range(info_reps + warmup):
        u = single_uris[i % len(single_uris)]
        t0 = time.perf_counter()
        s.post(f"{base}/timeseries_info", json={"uris": [u]}, timeout=600)
        if i >= warmup:
            lat_single.append(time.perf_counter() - t0)
    out["timeseries_info"] = {
        "n_uris": len(uris),
        "batch_p50_ms": pct(lat_batch, .5), "batch_mean_ms": round(statistics.mean(lat_batch) * 1e3, 2),
        "batch_p95_ms": pct(lat_batch, .95),
        "single_p50_ms": pct(lat_single, .5), "single_mean_ms": round(statistics.mean(lat_single) * 1e3, 2),
        "single_p95_ms": pct(lat_single, .95),
        "dense_stream_rows": dense["row_count"], "sparse_stream_rows": sparse["row_count"],
    }

    # ---- template 2: timeseries, across selectivities ---------------------
    def win(delta: timedelta, **extra) -> dict:
        """A window of *delta* ending on the anchor row — guaranteed non-empty."""
        return dict(uri=dense_uri, start=(anchor - delta).isoformat(),
                    end=anchor.isoformat(), **extra)

    shapes = [
        ("full range",         dict(uri=dense_uri)),
        ("30 days",            win(timedelta(days=30))),
        ("7 days",             win(timedelta(days=7))),
        ("1 day",              win(timedelta(days=1))),
        ("1 hour",             win(timedelta(hours=1))),
        ("10 min",             win(timedelta(minutes=10))),
        ("1 min",              win(timedelta(minutes=1))),
        ("limit 1000",         dict(uri=dense_uri, limit=1000)),
        ("latest 100 desc",    dict(uri=dense_uri, limit=100, order="desc")),
        ("coalesce 1 day",     win(timedelta(days=1), value_mode="coalesce")),
        ("sparse full range",  dict(uri=sparse_uri)),
        # addressed by computed stream ref URI, and the only numeric-valued shape
        ("numeric stream 1 day", dict(uri=numeric_uri or dense_uri,
                                      start=(anchor - timedelta(days=1)).isoformat(),
                                      end=anchor.isoformat(), value_mode="numeric")),
        ("numeric stream full",  dict(uri=numeric_uri or dense_uri)),
        # strictly after the newest row, so it is empty by construction
        ("EMPTY after data",   dict(uri=dense_uri,
                                    start=(anchor + timedelta(days=30)).isoformat(),
                                    end=(anchor + timedelta(days=60)).isoformat())),
    ]
    res = []
    for label, params in shapes:
        lat, rows, err = [], -1, None
        for i in range(ts_reps + warmup):
            t0 = time.perf_counter()
            try:
                r = s.get(f"{base}/timeseries?{urlencode(params)}",
                          headers={"Accept": ARROW}, timeout=1800)
                dt = time.perf_counter() - t0
                if r.status_code != 200:
                    err = f"HTTP {r.status_code}: {r.text[:200]}"
                    break
                if i == 0:
                    rows = count_rows(r.content)
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                break
            if i >= warmup:
                lat.append(dt)
        entry = {"label": label, "rows": rows, "params": {k: str(v) for k, v in params.items()}}
        entry.update(stat(lat) if lat else {"p50_ms": None, "mean_ms": None, "p95_ms": None})
        if err:
            entry["error"] = err
        res.append(entry)
        print(f"# {label}: rows={rows} p50={entry.get('p50_ms')}ms {err or ''}", file=sys.stderr)
    out["timeseries"] = res
    print(json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())

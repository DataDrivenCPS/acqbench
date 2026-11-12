#!/usr/bin/env python3
"""Extend the frozen 50-query set with 250 more, discovered on a live server with a fixed seed.

    python queries300.py <base_url> <lookup.json> <queries_1000.json> <out queries_300.json>
"""
import json, random, sys
from datetime import datetime, timedelta, timezone
import requests
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from queries import read_table, get  # noqa: E402

base, lookup, q50, out = sys.argv[1:5]
rng = random.Random(140)
s = requests.Session()
mapping = json.load(open(lookup))
info = s.post(f"{base}/timeseries_info", json={"uris": list(mapping)}, timeout=600).json()
live = {u: v for u, v in info.items() if (v or {}).get("row_count", 0) > 0}
def parse(t): 
    d = datetime.fromisoformat(t.replace("Z", "+00:00")); return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
rng_of = {u: (parse(v["earliest"]), parse(v["latest"])) for u, v in live.items()}
# keep streams whose range is sane (one source file writes year 0025)
for u, (a, b) in list(rng_of.items()):
    if a.year < 2000: rng_of[u] = (datetime(2021, 6, 1, tzinfo=timezone.utc), b)
uris = sorted(live)
base_doc = json.load(open(q50))
Q = list(base_doc["queries"]); assert len(Q) == 50
M, H, D = timedelta(minutes=1), timedelta(hours=1), timedelta(days=1)
LENS = [("1min", M), ("10min", 10 * M), ("1h", H), ("6h", 6 * H), ("1d", D), ("7d", 7 * D), ("30d", 30 * D), ("90d", 90 * D)]
def win(u, delta, **kw):
    a, b = rng_of[u]
    span = (b - a) - delta
    end = b - timedelta(seconds=rng.uniform(0, max(span.total_seconds(), 1)))
    return dict(uri=u, start=(end - delta).isoformat(), end=end.isoformat(), **kw)
new = []
# 100 random windows
for k in range(100):
    u = rng.choice(uris); name, delta = LENS[k % len(LENS)]
    new.append((f"W{k+1} {name} {u.rsplit('/',1)[-1][:28]}", win(u, delta)))
# 50 LIMIT reads
for k in range(50):
    u = rng.choice(uris); lim = [1, 10, 100, 1000, 10000][k % 5]; order = "desc" if k % 2 else "asc"
    if k % 3 == 0:
        new.append((f"L{k+1} limit {lim} {order} 30d {u.rsplit('/',1)[-1][:24]}", win(u, 30 * D, limit=lim, order=order)))
    else:
        new.append((f"L{k+1} limit {lim} {order} {u.rsplit('/',1)[-1][:28]}", dict(uri=u, limit=lim, order=order)))
# 40 value_mode variants
for k in range(40):
    u = rng.choice(uris); mode = ["numeric", "text", "coalesce", "default"][k % 4]; name, delta = LENS[2 + (k % 5)]
    p = win(u, delta); 
    if mode != "default": p["value_mode"] = mode
    new.append((f"M{k+1} {mode} {name} {u.rsplit('/',1)[-1][:24]}", p))
# 10 full-range + 20 one-year windows
for k in range(10):
    u = rng.choice(uris); new.append((f"F{k+1} full {u.rsplit('/',1)[-1][:30]}", dict(uri=u)))
for k in range(20):
    u = rng.choice(uris); new.append((f"Y{k+1} 1y {u.rsplit('/',1)[-1][:30]}", win(u, 365 * D)))
# 30 info batches
for k in range(30):
    n = [1, 5, 20, 50, len(uris)][k % 5]; sub = rng.sample(uris, n)
    new.append((f"I{k+1} info n={n}", dict(kind="info", uris=sub)))
assert len(new) == 250
for i, (l, p) in enumerate(new):
    Q.append({"id": 51 + i, "label": l, "params": p, "expect_empty": False})
doc = dict(base_doc); doc["generated_300"] = datetime.now(timezone.utc).isoformat(); doc["n_live_300"] = len(uris); doc["queries"] = Q
json.dump(doc, open(out, "w"), indent=1)
print(f"wrote {out}: {len(Q)} queries over {len(uris)} live streams")

#!/usr/bin/env python3
"""100 read shapes not covered by queries_300.json: open-ended windows, explicit whole-range windows,
multi-year desc, partial overlaps, deliberate empties, sparse streams, storage-key reads, odd info batches.

    python queries100b.py <base_url> <lookup.json> <out queries_100b.json>
"""
import json, random, sys
from datetime import datetime, timedelta, timezone
import requests
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from queries import ref_uri  # noqa: E402

base, lookup, out = sys.argv[1:4]
rng = random.Random(1400)
s = requests.Session()
mapping = json.load(open(lookup))
info = s.post(f"{base}/timeseries_info", json={"uris": list(mapping)}, timeout=600).json()
live = {u: v for u, v in info.items() if (v or {}).get("row_count", 0) > 0}
def parse(t):
    d = datetime.fromisoformat(t.replace("Z", "+00:00")); return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
R = {}
for u, v in live.items():
    a, b = parse(v["earliest"]), parse(v["latest"])
    if a.year < 2000: a = datetime(2021, 6, 1, tzinfo=timezone.utc)
    R[u] = (a, b)
uris = sorted(live); pick = lambda: rng.choice(uris)
short = lambda u: u.rsplit("/", 1)[-1][:26]
H, D = timedelta(hours=1), timedelta(days=1)
Q = []
add = lambda label, p, empty=False: Q.append({"id": 301 + len(Q), "label": label, "params": p, "expect_empty": empty})
# 15 open-ended start (start only, runs to latest)
for k, back in enumerate([H, D, 7 * D, 30 * D, 365 * D] * 3):
    u = pick(); add(f"S{k+1} start-only last {back.days}d{back.seconds//3600}h {short(u)}", dict(uri=u, start=(R[u][1] - back).isoformat()))
# 15 open-ended end (end only, from earliest)
for k, fwd in enumerate([H, D, 7 * D, 30 * D, 365 * D] * 3):
    u = pick(); add(f"E{k+1} end-only first {fwd.days}d{fwd.seconds//3600}h {short(u)}", dict(uri=u, end=(R[u][0] + fwd).isoformat()))
# 10 explicit whole-range windows (start=earliest, end=latest): the entire stream through the windowed path
for k in range(10):
    u = pick(); add(f"X{k+1} explicit whole range {short(u)}", dict(uri=u, start=R[u][0].isoformat(), end=R[u][1].isoformat()))
# 10 multi-year windows, desc
for k in range(10):
    u = pick(); yrs = 2 + (k % 2); a, b = R[u]; end = b - timedelta(days=rng.randint(0, 200))
    add(f"B{k+1} {yrs}y desc {short(u)}", dict(uri=u, start=(end - yrs * 365 * D).isoformat(), end=end.isoformat(), order="desc"))
# 10 partial overlaps at the stream edges
for k in range(10):
    u = pick(); a, b = R[u]
    if k % 2 == 0: add(f"P{k+1} straddles earliest {short(u)}", dict(uri=u, start=(a - 30 * D).isoformat(), end=(a + D).isoformat()))
    else: add(f"P{k+1} straddles latest {short(u)}", dict(uri=u, start=(b - D).isoformat(), end=(b + 30 * D).isoformat()))
# 10 deliberate empties
for k in range(10):
    u = pick(); a, b = R[u]
    if k % 4 == 0: add(f"EMPTY Z{k+1} after latest {short(u)}", dict(uri=u, start=(b + 30 * D).isoformat(), end=(b + 60 * D).isoformat()), True)
    elif k % 4 == 1: add(f"EMPTY Z{k+1} before earliest {short(u)}", dict(uri=u, start=(a - 400 * D).isoformat(), end=(a - 300 * D).isoformat()), True)
    elif k % 4 == 2: add(f"EMPTY Z{k+1} unregistered uri", dict(uri=f"urn:dpr/no-such-stream-{k}", start=a.isoformat(), end=b.isoformat()), True)
    else: add(f"EMPTY Z{k+1} limit 1 after latest {short(u)}", dict(uri=u, start=(b + D).isoformat(), limit=1, order="desc"), True)
# 10 sparse streams (fewest rows): full and 30-day
sparse = sorted(uris, key=lambda u: live[u]["row_count"])[:5]
for k, u in enumerate(sparse):
    add(f"Q{2*k+1} sparse full {short(u)} ({live[u]['row_count']} rows)", dict(uri=u))
    a, b = R[u]; add(f"Q{2*k+2} sparse 30d {short(u)}", dict(uri=u, start=(b - 30 * D).isoformat(), end=b.isoformat()))
# 10 storage-key reads (urn:acquirium#uuid computed from the column name)
for k in range(10):
    u = pick(); key = ref_uri(mapping[u]); a, b = R[u]
    if k % 2 == 0: add(f"K{k+1} storage-key 7d {short(u)}", dict(uri=key, start=(b - 7 * D).isoformat(), end=b.isoformat()))
    else: add(f"K{k+1} storage-key latest 10 {short(u)}", dict(uri=key, limit=10, order="desc"))
# 10 odd info batches
add("N1 info 5 unregistered", dict(kind="info", uris=[f"urn:dpr/nope-{i}" for i in range(5)]))
add("N2 info 10 registered + 10 unregistered", dict(kind="info", uris=rng.sample(uris, 10) + [f"urn:dpr/nope-{i}" for i in range(10)]))
add("N3 info same uri x20", dict(kind="info", uris=[pick()] * 20))
add("N4 info 5 storage keys", dict(kind="info", uris=[ref_uri(mapping[u]) for u in rng.sample(uris, 5)]))
add("N5 info sparse 5", dict(kind="info", uris=sparse))
for k in range(5): add(f"N{k+6} info random 2", dict(kind="info", uris=rng.sample(uris, 2)))
assert len(Q) == 100, len(Q)
json.dump({"generated": datetime.now(timezone.utc).isoformat(), "n_live": len(uris), "queries": Q}, open(out, "w"), indent=1)
print(f"wrote {out}: {len(Q)} queries")

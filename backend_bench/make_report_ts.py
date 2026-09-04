#!/usr/bin/env python3
"""Four-way Timescale comparison across campaigns.
Usage: make_report_ts.py <out.html> <spec.json>
spec: {"variants":[{"label":..,"file":..,"baseline":..(same-day main json),"samples":..,"cls":..}],
       "duckdb": <main.duckdb json for reference>, "queries": <queries.json>, "findings": <html file or null>}"""
from __future__ import annotations
import html, json, math, sys
from pathlib import Path

out = Path(sys.argv[1]); spec = json.load(open(sys.argv[2]))
E = html.escape
def L(p): return json.load(open(p))
V = [dict(v, o=L(v["file"]), b=L(v["baseline"]), s=[json.loads(l) for l in open(v["samples"])] if v.get("samples") else []) for v in spec["variants"]]
DK = L(spec["duckdb"]); Q = L(spec["queries"])
def f0(x): return "—" if x is None else f"{x:,.0f}"
def f1(x): return "—" if x is None else f"{x:,.1f}"
def f2(x): return "—" if x is None else f"{x:,.2f}"
def gb(x): return "—" if not x else f"{x/1e9:.2f} GB"
def mins(x): return "—" if x is None else f"{x/60:.1f} min"
def qm(o): return {r["id"]: r for r in o.get("query", {}).get("results", [])}
for v in V: v["q"], v["qb"] = qm(v["o"]), qm(v["b"])
QD = qm(DK)
def hyper(o):
    h = (o.get("db", {}).get("hypertable") or "0|0|0|0").split("|"); return [int(x) for x in h]

def line_chart(series, w=900, h=260, xlabel="minutes", ylabel="M rows"):
    pad_l, pad_r, pad_t, pad_b = 52, 16, 12, 30
    xs = [x for *_, pts in series for x, _ in pts]; ys = [y for *_, pts in series for _, y in pts]
    if not xs: return "<p class=sub>no timeline data</p>"
    xmax, ymax = max(xs) or 1, max(ys) or 1
    sx = lambda x: pad_l + (x / xmax) * (w - pad_l - pad_r)
    sy = lambda y: pad_t + (1 - y / ymax) * (h - pad_t - pad_b)
    g = []
    for i in range(6):
        y = ymax * i / 5; g.append(f'<line class=grid x1={pad_l} x2={w-pad_r} y1={sy(y):.1f} y2={sy(y):.1f}/><text class="tick tick-y" x={pad_l-6} y={sy(y)+4:.1f}>{y:.0f}</text>')
    for i in range(6):
        x = xmax * i / 5; g.append(f'<text class="tick tick-x" x={sx(x):.1f} y={h-8}>{x:.0f}</text>')
    paths = []
    for name, cls, dashed, pts in series:
        if not pts: continue
        d = " ".join(f"{'M' if i == 0 else 'L'}{sx(x):.1f},{sy(y):.1f}" for i, (x, y) in enumerate(pts))
        paths.append(f'<path d="{d}" fill=none style="stroke:var(--{cls})" stroke-width=2 stroke-linejoin=round{" stroke-dasharray=6,4" if dashed else ""}/>')
        x, y = pts[-1]; paths.append(f'<text class=barlab x={sx(x)-4:.1f} y={sy(y)-6:.1f} text-anchor=end>{E(name)}</text>')
    return (f'<svg class=chart viewBox="0 0 {w} {h}">{"".join(g)}<line class=axis x1={pad_l} x2={w-pad_r} y1={h-pad_b} y2={h-pad_b}/>{"".join(paths)}'
            f'<text class=tick x={w-pad_r} y={h-8} text-anchor=end>{xlabel}</text><text class=tick x={pad_l} y={pad_t} text-anchor=start>{ylabel}</text></svg>')

def hbar_log(rows, cols, w=900):
    n = len(cols); rh, gap, pad_l, pad_r, top = 8, 7, 250, 60, 22
    vals = [v for _, vs in rows for v in vs if v]
    lo, hi = math.log10(max(min(vals) * .8, .1)), math.log10(max(vals) * 1.3)
    span = w - pad_l - pad_r
    sx = lambda v: pad_l + (math.log10(max(v, .1)) - lo) / (hi - lo) * span
    h = top + len(rows) * (n * rh + gap) + 24
    g = []; t = 10 ** math.floor(lo)
    while t <= 10 ** hi:
        if lo <= math.log10(t) <= hi:
            g.append(f'<line class=grid x1={sx(t):.1f} x2={sx(t):.1f} y1={top-4} y2={h-20}/><text class="tick tick-x" x={sx(t):.1f} y={h-6}>{t:g} ms</text>')
        t *= 10
    for i, (lab, vs) in enumerate(rows):
        y = top + i * (n * rh + gap)
        g.append(f'<text class=tick x={pad_l-8} y={y+n*rh/2+3:.1f} text-anchor=end>{E(lab)}</text>')
        for j, v in enumerate(vs):
            if v:
                g.append(f'<rect x={pad_l} y={y+j*rh} width={max(sx(v)-pad_l,1):.1f} height={rh-1} rx=2 style="fill:var(--{cols[j][1]})"><title>{E(cols[j][0])} · {E(lab)}: {v} ms</title></rect>'
                         f'<text class=barlab x={sx(v)+4:.1f} y={y+j*rh+rh-1} text-anchor=start>{v:g}</text>')
    return f'<svg class=chart viewBox="0 0 {w} {h}">{"".join(g)}</svg>'
def legend(items): return "<div class=legend>" + "".join(f'<span class=lg><i style="background:var(--{c})"></i>{E(n)}</span>' for n, c in items) + "</div>"
def med(rs): rs = sorted(rs); return rs[len(rs)//2] if rs else None

# ---- tiles
tiles = ""
for v in V:
    i, db, ib = v["o"]["ingest"], v["o"]["db"], v["b"]["ingest"]
    mr = med([v["q"][k]["p50_ms"] / v["qb"][k]["p50_ms"] for k in v["q"] if k in v["qb"] and v["qb"][k].get("p50_ms")])
    tiles += (f'<div class=tile><div class=k>{E(v["label"])}</div><div class=v style="color:var(--{v["cls"]})">{f0(i["rows_per_s"])} rows/s</div>'
              f'<div class=n>{mins(i["wall_s"])} · {gb(db.get("db_bytes"))} · {f0(db.get("bytes_per_row"))} B/row<br>vs same-day main: {i["rows_per_s"]/ib["rows_per_s"]:.2f}× ingest, {v["b"]["db"]["db_bytes"]/db["db_bytes"]:.2f}× smaller, median query {f2(mr)}×</div></div>')

# ---- ingest table
rows = [
    ("ref @ commit · run date", lambda v: f'<code>{E(v["o"]["ref"])}</code>@<code>{E(v["o"]["head"][:8])}</code> · {E(v["o"]["started"][:10])}'),
    ("write path", lambda v: E(v.get("path", ""))),
    ("timeseries key / indexes on hypertable", lambda v: E(v.get("schema", ""))),
    ("ingest wall (200 files, 136.8M rows)", lambda v: mins(v["o"]["ingest"]["wall_s"])),
    ("rows / s", lambda v: f0(v["o"]["ingest"]["rows_per_s"])),
    ("same-day main rows / s", lambda v: f0(v["b"]["ingest"]["rows_per_s"])),
    ("acquirium CPU during ingest", lambda v: f'{v["o"]["ingest"]["cpu_utilisation_server"]} cores'),
    ("postgres CPU during ingest", lambda v: f'{v["o"]["ingest"]["cpu_utilisation_pg"]} cores'),
    ("DB on disk", lambda v: gb(v["o"]["db"].get("db_bytes"))),
    ("table bytes", lambda v: gb(hyper(v["o"])[0])),
    ("index bytes", lambda v: gb(hyper(v["o"])[1])),
    ("bytes / row", lambda v: f0(v["o"]["db"].get("bytes_per_row"))),
    ("chunks (compressed)", lambda v: E(str(v["o"]["db"].get("chunks", "")))),
    ("peak server / postgres RSS", lambda v: f'{f0(v["o"]["peak_rss_mb"]["server_rss_mb"])} / {f0(v["o"]["peak_rss_mb"]["pg_rss_mb"])} MB'),
    ("count(*) cross-check", lambda v: f'{v["o"]["db"].get("row_count", 0):,}'),
    ("ERROR/Traceback lines", lambda v: str(v["o"]["ingest"].get("log_error_lines"))),
]
ingest_tbl = "<table><tr><th>metric</th>" + "".join(f"<th>{E(v['label'])}</th>" for v in V) + "</tr>" + "".join(
    f"<tr><td>{E(k)}</td>" + "".join(f"<td class=num>{fn(v)}</td>" for v in V) + "</tr>" for k, fn in rows) + "</table>"
timeline_fig = line_chart([(v["label"], v["cls"], False, [(p["t_s"]/60, p["rows"]/1e6) for p in v["o"]["ingest"].get("timeline", [])]) for v in V])
def rss(v, key):
    s = v["s"]
    if not s: return []
    t0 = s[0]["t"]; return [((x["t"]-t0)/60, x[key]/1024) for x in s]
rss_fig = line_chart([(f'{v["label"]} postgres', v["cls"], False, rss(v, "pg_rss_mb")) for v in V], ylabel="GB RSS (postgres)")

# ---- indexes
maps = []
for v in V:
    m = {}
    for line in (v["o"]["db"].get("indexes") or "").splitlines():
        if "|" in line:
            n, b = line.split("|", 1); m[n] = int(b) if b.strip().isdigit() else None
    maps.append(m)
names = sorted({n for m in maps for n in m})
idx_tbl = "<table><tr><th>index on <code>timeseries</code></th>" + "".join(f"<th>{E(v['label'])}</th>" for v in V) + "</tr>" + "".join(
    f"<tr><td><code>{E(n)}</code></td>" + "".join(f"<td class=num>{gb(m[n]) if m.get(n) else '<span class=ok>—</span>'}</td>" for m in maps) + "</tr>" for n in names) + \
    "<tr><td><b>total index bytes</b></td>" + "".join(f"<td class=num><b>{gb(hyper(v['o'])[1])}</b></td>" for v in V) + "</tr></table>"

# ---- queries
cols = [(v["label"], v["cls"]) for v in V]
hdr = "<tr><th>#</th><th>query</th><th>rows</th><th>duckdb p50</th>" + "".join(f"<th>{E(v['label'])} p50</th><th>cold</th><th>÷ main</th>" for v in V) + "</tr>"
qt = "<table>" + hdr; bars = []
for q in Q["queries"]:
    i = q["id"]; d = QD.get(i, {})
    qt += f"<tr><td>{i}</td><td>{E(q['label'])}</td><td class=num>{d.get('rows', 0):,}</td><td class=num>{f2(d.get('p50_ms'))}</td>"
    vs = []
    for v in V:
        r, b = v["q"].get(i, {}), v["qb"].get(i, {})
        ratio = r["p50_ms"] / b["p50_ms"] if r.get("p50_ms") and b.get("p50_ms") else None
        cls = "" if ratio is None else ("good" if ratio < 0.8 else ("warn" if ratio > 1.25 else ""))
        qt += f"<td class=num>{f2(r.get('p50_ms'))}</td><td class=num>{f2(r.get('cold_ms'))}</td><td class='num {cls}'>{f2(ratio)}×</td>"
        vs.append(r.get("p50_ms"))
    qt += "</tr>"; bars.append((f"{i}. {q['label']}", vs))
qt += "</table>"
bar_fig = hbar_log(bars, cols)

# ---- regime summary: median ratio-to-same-day-main by row-count bucket
buckets = [("≥ 200k rows (scans)", lambda r: r >= 200000), ("50k–200k", lambda r: 50000 <= r < 200000), ("2k–50k", lambda r: 2000 <= r < 50000),
           ("≤ 400 rows (point reads)", lambda r: r <= 400), ("LIMIT shapes (#23–26)", None)]
reg = "<table><tr><th>regime</th><th>duckdb (main) p50 median</th>" + "".join(f"<th>{E(v['label'])} p50 median · ÷ main</th>" for v in V) + "</tr>"
for name, pred in buckets:
    ids = [23, 24, 25, 26] if pred is None else [q["id"] for q in Q["queries"] if q["id"] not in (23, 24, 25, 26) and pred(QD.get(q["id"], {}).get("rows", -1))]
    reg += f"<tr><td>{E(name)} <span class=sub>({len(ids)} q)</span></td><td class=num>{f2(med([QD[i]['p50_ms'] for i in ids if i in QD]))} ms</td>"
    for v in V:
        p = med([v["q"][i]["p50_ms"] for i in ids if i in v["q"]]); r = med([v["q"][i]["p50_ms"]/v["qb"][i]["p50_ms"] for i in ids if i in v["q"] and i in v["qb"]])
        cls = "" if r is None else ("good" if r < 0.8 else ("warn" if r > 1.25 else ""))
        reg += f"<td class='num {cls}'>{f2(p)} ms · {f2(r)}×</td>"
    reg += "</tr>"
reg += "</table>"

def params(p): return ", ".join(f"{k}={v}" for k, v in p.items() if k != "uri")
qdef = "<table><tr><th>#</th><th>label</th><th>stream</th><th>params</th></tr>" + "".join(
    f"<tr><td>{q['id']}</td><td>{E(q['label'])}</td><td><code>{E(q['params']['uri'])}</code></td><td>{E(params(q['params']))}</td></tr>" for q in Q["queries"]) + "</table>"
FIND = Path(spec["findings"]).read_text() if spec.get("findings") and Path(spec["findings"]).exists() else ""
CSS = open(Path(__file__).parent / "report.css").read() + "\n:root{--series-4:#8e5bd1;--series-5:#c9a300}\n"
page = f"""<title>Timescale Store Variants</title>
<style>{CSS}</style>
<div class=wrap>
<h1>acquirium Timescale backend: main vs the three fix variants</h1>
<p class=sub>DPR trailer SCADA, 200 CSVs, 136.8M rows · cloud-vm (4 vCPU / 16 GiB cgroup) · <code>timescale/timescaledb:latest-pg17</code>, fresh container per run ·
each variant was run in its own campaign right after a run of <code>main</code> @ <code>c09d321</code> on the same day, and every "÷ main" number is against that same-day main run ·
duckdb column = <code>main</code> duckdb backend from the 2026-09-04 campaign, for scale</p>
<div class=tiles>{tiles}</div>
{FIND}
<h2>Ingest and footprint</h2>
<div class=tw>{ingest_tbl}</div>
<figure class=fig><figcaption><b>Cumulative rows ingested</b><span class=unit>M rows vs minutes since driver start</span></figcaption>{timeline_fig}{legend(cols)}</figure>
<figure class=fig><figcaption><b>Postgres resident memory</b><span class=unit>GB, sampled every 5 s across boot → ingest → queries</span></figcaption>{rss_fig}{legend(cols)}</figure>
<h3>Indexes on the hypertable after ingest</h3>
<div class=tw>{idx_tbl}</div>
<h2>Query latency — 30 <code>GET /timeseries</code> queries × 10 reps</h2>
<p>Same frozen query set (<code>queries.json</code>, 2026-08-26) on every run: 1 cold request, 2 warm-ups discarded, 10 timed reps, p50 reported. Absolute Timescale latencies drift 15–20% between days with host load, so compare the "÷ main" columns across variants, not raw milliseconds.</p>
<h3>By regime — median p50 and median ratio to same-day main</h3>
<div class=tw>{reg}</div>
<figure class=fig><figcaption><b>p50 latency per query</b><span class=unit>ms, log scale</span></figcaption>{bar_fig}{legend(cols)}</figure>
<div class=tw>{qt}</div>
<h2>Query definitions</h2>
<div class=tw>{qdef}</div>
</div>"""
out.write_text(page.replace("/>", " />")); print("wrote", out)

#!/usr/bin/env python3
"""Render an N-run backend report from a results dir produced by bench_backends.py
(--runs). Usage: make_report3.py <results dir> <out.html> <tag=label>[,<tag=label>...]
e.g.  main.duckdb=duckdb (main),main.timescale=timescale (main),ums-timescale-fix.timescale=timescale (fix)
The first timescale run is the timescale baseline; the last one is "the fix"."""
from __future__ import annotations
import html, json, math, sys
from pathlib import Path

res, out = Path(sys.argv[1]), Path(sys.argv[2])
RUNS = [tuple(x.split("=", 1)) for x in sys.argv[3].split(",")]
CLS = ["series-1", "series-2", "series-3", "series-4", "series-5"]
R = [(tag, label, CLS[i], json.load(open(res / f"{tag}.json"))) for i, (tag, label) in enumerate(RUNS)]
Q = json.load(open(res / "queries.json"))
E = html.escape
def samples(tag):
    p = res / f"{tag}.samples.jsonl"
    return [json.loads(l) for l in open(p)] if p.exists() else []
S = {tag: samples(tag) for tag, *_ in R}
def f0(x): return "—" if x is None else f"{x:,.0f}"
def f1(x): return "—" if x is None else f"{x:,.1f}"
def f2(x): return "—" if x is None else f"{x:,.2f}"
def gb(x): return "—" if not x else f"{x/1e9:.2f} GB"
def mins(x): return "—" if x is None else f"{x/60:.1f} min"
def qmap(o): return {r["id"]: r for r in o.get("query", {}).get("results", [])}
QM = {tag: qmap(o) for tag, _, _, o in R}
ING = {tag: o.get("ingest", {}) for tag, _, _, o in R}
DB = {tag: o.get("db", {}) for tag, _, _, o in R}
TS = [x for x in R if x[3].get("backend") == "timescale"]
DK = [x for x in R if x[3].get("backend") == "duckdb"]
ts_base, ts_fix = (TS[0], TS[-1]) if len(TS) >= 2 else (None, None)

# ---------------------------------------------------------------- charts
def line_chart(series, w=900, h=260, xlabel="minutes", ylabel="M rows"):
    """series: [(name, cls, dashed, [(x,y),...])]"""
    pad_l, pad_r, pad_t, pad_b = 52, 16, 12, 30
    xs = [x for *_, pts in series for x, _ in pts]; ys = [y for *_, pts in series for _, y in pts]
    if not xs: return "<p class=sub>no timeline data</p>"
    xmax, ymax = max(xs) or 1, max(ys) or 1
    sx = lambda x: pad_l + (x / xmax) * (w - pad_l - pad_r)
    sy = lambda y: pad_t + (1 - y / ymax) * (h - pad_t - pad_b)
    g = []
    for i in range(6):
        y = ymax * i / 5; g.append(f'<line class=grid x1={pad_l} x2={w-pad_r} y1={sy(y):.1f} y2={sy(y):.1f}/>'
                                   f'<text class="tick tick-y" x={pad_l-6} y={sy(y)+4:.1f}>{y:.0f}</text>')
    for i in range(6):
        x = xmax * i / 5; g.append(f'<text class="tick tick-x" x={sx(x):.1f} y={h-8}>{x:.0f}</text>')
    paths = []
    for name, cls, dashed, pts in series:
        if not pts: continue
        d = " ".join(f"{'M' if i == 0 else 'L'}{sx(x):.1f},{sy(y):.1f}" for i, (x, y) in enumerate(pts))
        paths.append(f'<path d="{d}" fill=none style="stroke:var(--{cls})" stroke-width=2 stroke-linejoin=round{" stroke-dasharray=6,4" if dashed else ""}/>')
        x, y = pts[-1]; paths.append(f'<text class=barlab x={sx(x)-4:.1f} y={sy(y)-6:.1f} text-anchor=end>{E(name)}</text>')
    return (f'<svg class=chart viewBox="0 0 {w} {h}">{"".join(g)}<line class=axis x1={pad_l} x2={w-pad_r} y1={h-pad_b} y2={h-pad_b}/>'
            f'{"".join(paths)}<text class=tick x={w-pad_r} y={h-8} text-anchor=end>{xlabel}</text>'
            f'<text class=tick x={pad_l} y={pad_t} text-anchor=start>{ylabel}</text></svg>')

def hbar_log(rows, w=900):
    """rows: [(label, [v_run1, v_run2, ...])] grouped horizontal bars, log x (ms)."""
    n = len(R); rh, gap, pad_l, pad_r, top = 9, 7, 250, 60, 22
    vals = [v for _, vs in rows for v in vs if v]
    lo, hi = math.log10(max(min(vals) * .8, .1)), math.log10(max(vals) * 1.3)
    span = w - pad_l - pad_r
    sx = lambda v: pad_l + (math.log10(max(v, .1)) - lo) / (hi - lo) * span
    h = top + len(rows) * (n * rh + gap) + 24
    g = []
    t = 10 ** math.floor(lo)
    while t <= 10 ** hi:
        if lo <= math.log10(t) <= hi:
            g.append(f'<line class=grid x1={sx(t):.1f} x2={sx(t):.1f} y1={top-4} y2={h-20}/>'
                     f'<text class="tick tick-x" x={sx(t):.1f} y={h-6}>{t:g} ms</text>')
        t *= 10
    for i, (lab, vs) in enumerate(rows):
        y = top + i * (n * rh + gap)
        g.append(f'<text class=tick x={pad_l-8} y={y+n*rh/2+3:.1f} text-anchor=end>{E(lab)}</text>')
        for j, v in enumerate(vs):
            if v:
                g.append(f'<rect x={pad_l} y={y+j*rh} width={max(sx(v)-pad_l,1):.1f} height={rh-1} rx=2 style="fill:var(--{R[j][2]})"><title>{E(R[j][1])} · {E(lab)}: {v} ms</title></rect>'
                         f'<text class=barlab x={sx(v)+4:.1f} y={y+j*rh+rh-1} text-anchor=start>{v:g}</text>')
    return f'<svg class=chart viewBox="0 0 {w} {h}">{"".join(g)}</svg>'

def legend(items): return "<div class=legend>" + "".join(f'<span class=lg><i style="background:var(--{c})"></i>{E(n)}</span>' for n, c in items) + "</div>"

# ---------------------------------------------------------------- headline
def med_ratio(a, b):
    """median over queries of p50(b)/p50(a)."""
    rs = [b[k]["p50_ms"] / a[k]["p50_ms"] for k in a if k in b and a[k].get("p50_ms") and b[k].get("p50_ms")]
    rs.sort(); return rs[len(rs)//2] if rs else None

tiles = ""
for tag, label, cls, o in R:
    i = ING[tag]
    tiles += (f'<div class=tile><div class=k>{E(label)} ingest</div><div class=v style="color:var(--{cls})">{f0(i.get("rows_per_s"))} rows/s</div>'
              f'<div class=n>{i.get("rows_inserted",0):,} rows · {mins(i.get("wall_s"))} · {gb(DB[tag].get("db_bytes"))} on disk{"" if i.get("complete") else " · ⚠ incomplete"}</div></div>')
if ts_base and ts_fix:
    b, f = ts_base, ts_fix
    sp = (ING[f[0]].get("rows_per_s") or 0) / (ING[b[0]].get("rows_per_s") or 1)
    dr = (DB[b[0]].get("db_bytes") or 0) / (DB[f[0]].get("db_bytes") or 1)
    mr = med_ratio(QM[b[0]], QM[f[0]])
    tiles += (f'<div class=tile><div class=k>fix vs main · timescale</div><div class=v>{sp:.2f}× ingest · {dr:.2f}× smaller</div>'
              f'<div class=n>median query p50 ratio fix ÷ main = {f2(mr)} (&lt;1 = fix faster)</div></div>')

# ---------------------------------------------------------------- ingest table
ingest_rows = [
    ("acquirium ref @ commit", lambda i, o: f'<code>{E(o.get("ref","?"))}</code> @ <code>{E(o.get("head","?")[:10])}</code>'),
    ("files", lambda i, o: f'{i.get("files_ingested","—")}/{i.get("files_total","—")}'),
    ("rows inserted", lambda i, o: f'{i.get("rows_inserted",0):,}'),
    ("wall (driver start → last file)", lambda i, o: mins(i.get("wall_s"))),
    ("rows / s", lambda i, o: f0(i.get("rows_per_s"))),
    ("driver setup (graph + stream registration)", lambda i, o: f'{i.get("driver_start_s","—")} s'),
    ("server boot to /health", lambda i, o: f'{o.get("boot_s","—")} s'),
    ("server CPU-s during ingest (utilisation)", lambda i, o: f'{f0(i.get("server_cpu_s"))} ({i.get("cpu_utilisation_server","—")} cores)'),
    ("postgres CPU-s during ingest (utilisation)", lambda i, o: f'{f0(i.get("pg_cpu_s"))} ({i.get("cpu_utilisation_pg","—")} cores)' if o["backend"] == "timescale" else "n/a"),
    ("peak server RSS", lambda i, o: f'{f0(o.get("peak_rss_mb",{}).get("server_rss_mb"))} MB'),
    ("peak postgres RSS", lambda i, o: f'{f0(o.get("peak_rss_mb",{}).get("pg_rss_mb"))} MB' if o["backend"] == "timescale" else "n/a"),
    ("DB size on disk", lambda i, o: gb(o.get("db",{}).get("db_bytes"))),
    ("bytes / row", lambda i, o: f0(o.get("db",{}).get("bytes_per_row"))),
    ("count(*) cross-check", lambda i, o: f'{o.get("db",{}).get("row_count",0):,}' if o.get("db",{}).get("row_count") else "—"),
    ("streams (value kinds)", lambda i, o: f'{o.get("db",{}).get("streams","—")} ({E(str(o.get("db",{}).get("value_kinds","")))})'),
    ("ERROR/Traceback lines in server log", lambda i, o: str(i.get("log_error_lines","—"))),
]
ingest_tbl = "<table><tr><th>metric</th>" + "".join(f"<th>{E(l)}</th>" for _, l, _, _ in R) + "</tr>" + "".join(
    f"<tr><td>{E(k)}</td>" + "".join(f"<td class=num>{fn(ING[t], o)}</td>" for t, _, _, o in R) + "</tr>"
    for k, fn in ingest_rows) + "</table>"

timeline_fig = line_chart([(l, c, False, [(p["t_s"] / 60, p["rows"] / 1e6) for p in ING[t].get("timeline", [])]) for t, l, c, _ in R])
def rss_series(tag, key):
    s = S[tag]
    if not s: return []
    t0 = s[0]["t"]; return [((x["t"] - t0) / 60, x[key] / 1024) for x in s]
rss_fig = line_chart([(f"{l} server", c, False, rss_series(t, "server_rss_mb")) for t, l, c, _ in R] +
                     [(f"{l} postgres", c, True, rss_series(t, "pg_rss_mb")) for t, l, c, o in R if o.get("backend") == "timescale"],
                     ylabel="GB RSS")

# ---------------------------------------------------------------- queries
def cell(r, k, fmt=f2):
    v = r.get(k); return f"<td class=num>{fmt(v) if v is not None else E(r.get('error','—')[:40])}</td>"
def cpu(r): return f1(r["server_cpu_s_per_req"] * 1e3) if r.get("server_cpu_s_per_req") is not None else "—"
hdr = "<tr><th>#</th><th>query</th><th>rows</th>" + "".join(
    f"<th>{E(l)} p50</th><th>p95</th><th>cold</th><th>cpu ms</th>" for _, l, _, _ in R)
ratios = []
if ts_base and ts_fix: ratios.append(("fix ÷ main (ts) p50", ts_base[0], ts_fix[0]))
if DK and ts_fix: ratios.append((f"{ts_fix[1]} ÷ {DK[0][1]} p50", DK[0][0], ts_fix[0]))
hdr += "".join(f"<th>{E(n)}</th>" for n, _, _ in ratios) + "</tr>"
qt = "<table>" + hdr
bars = []
for q in Q["queries"]:
    rs = [QM[t].get(q["id"], {}) for t, *_ in R]
    rows = next((r["rows"] for r in rs if r.get("rows") is not None), None)
    warn = "".join(f' <span class=warn title="{E(l)}: {E(r.get("warning",""))}">⚠</span>' for (_, l, _, _), r in zip(R, rs) if r.get("warning"))
    qt += f"<tr><td>{q['id']}</td><td>{E(q['label'])}{warn}</td><td class=num>{rows:,}</td>" if rows is not None else f"<tr><td>{q['id']}</td><td>{E(q['label'])}{warn}</td><td class=num>—</td>"
    for r in rs: qt += f"{cell(r,'p50_ms')}{cell(r,'p95_ms')}{cell(r,'cold_ms')}<td class=num>{cpu(r)}</td>"
    for _, ta, tb in ratios:
        a, b = QM[ta].get(q["id"], {}), QM[tb].get(q["id"], {})
        r = (b["p50_ms"] / a["p50_ms"]) if a.get("p50_ms") and b.get("p50_ms") else None
        cls = "" if r is None else ("good" if r < 0.9 else ("warn" if r > 1.1 else ""))
        qt += f"<td class='num {cls}'>{f2(r)}×</td>"
    qt += "</tr>"
    bars.append((f"{q['id']}. {q['label']}", [r.get("p50_ms") for r in rs]))
qt += "</table>"
bar_fig = hbar_log(bars)

big = [q for q in Q["queries"] if any(QM[t].get(q["id"], {}).get("rows", 0) >= 50000 for t, *_ in R)]
thr = "<table><tr><th>query</th><th>rows</th>" + "".join(f"<th>{E(l)} rows/s</th><th>MB/s</th>" for _, l, _, _ in R) + "</tr>" + "".join(
    f"<tr><td>{E(q['label'])}</td><td class=num>{QM[R[0][0]].get(q['id'],{}).get('rows',0):,}</td>" +
    "".join(f"<td class=num>{f0(QM[t].get(q['id'],{}).get('rows_per_s'))}</td><td class=num>{f1(QM[t].get(q['id'],{}).get('mb_per_s'))}</td>" for t, *_ in R) + "</tr>"
    for q in big) + "</table>"

def params(p): return ", ".join(f"{k}={v}" for k, v in p.items() if k != "uri")
qdef = "<table><tr><th>#</th><th>label</th><th>stream</th><th>params</th></tr>" + "".join(
    f"<tr><td>{q['id']}</td><td>{E(q['label'])}</td><td><code>{E(q['params']['uri'])}</code></td><td>{E(params(q['params']))}</td></tr>"
    for q in Q["queries"]) + "</table>"

# ---------------------------------------------------------------- timescale state
def idx_map(o):
    m = {}
    for line in (o.get("db", {}).get("indexes") or "").splitlines():
        if "|" in line:
            n, b = line.split("|", 1)
            try: m[n] = int(b)
            except ValueError: m[n] = None
    return m
idx_tbl = ""
if TS:
    maps = [(l, idx_map(o)) for _, l, _, o in TS]
    names = sorted({n for _, m in maps for n in m})
    idx_tbl = "<table><tr><th>index on <code>timeseries</code></th>" + "".join(f"<th>{E(l)}</th>" for l, _ in maps) + "</tr>" + "".join(
        f"<tr><td><code>{E(n)}</code></td>" + "".join(f"<td class=num>{gb(m[n]) if n in m and m[n] else ('—' if n in m else '<span class=ok>dropped</span>')}</td>" for _, m in maps) + "</tr>"
        for n in names) + "<tr><td><b>total index bytes (hypertable_detailed_size)</b></td>" + "".join(
        f"<td class=num><b>{gb(int((o.get('db',{}).get('hypertable') or '0|0|0|0').split('|')[1]))}</b></td>" for _, _, _, o in TS) + "</tr></table>"
ts_state = "".join(f"<h3>{E(l)}</h3><pre class=code>{E(json.dumps(o.get('db',{}), indent=1))}</pre>" for _, l, _, o in TS)
tsdb = TS[0][3].get("tsdb", {}) if TS else {}
errs = "".join(f'<div class="note bad"><b>{E(l)} error:</b> {E(o["error"])}</div>' for _, l, _, o in R if o.get("error"))

FINDINGS = (res / "findings.html").read_text() if (res / "findings.html").exists() else ""
CSS = open(Path(__file__).parent / "report.css").read() + "\n:root{--series-4:#8e5bd1;--series-5:#c9a300}\n"
first, last = R[0][3], R[-1][3]
reps, warm = first.get("query", {}).get("reps", "?"), first.get("query", {}).get("warmup", "?")
page = f"""<title>Timescale Index Fix Benchmark</title>
<style>{CSS}</style>
<div class=wrap>
<h1>acquirium timeseries backend: <code>main</code> vs <code>ums-timescale-fix</code></h1>
<p class=sub>{" · ".join(f'{E(l)}: <code>{E(o.get("ref","?"))}</code>@<code>{E(o.get("head","?")[:10])}</code>' for _, l, _, o in R)}<br>
DPR trailer SCADA, 200 CSVs · cloud-vm (4 vCPU / 16 GiB cgroup) · {E(first.get('started','')[:16])} → {E(last.get('finished','')[:16])} UTC · runs sequential, fresh clone + venv per ref</p>
{errs}
<div class=tiles>{tiles}</div>
{FINDINGS}
<h2>Ingest</h2>
<p>Same code path for every run: <code>acquirium server</code> with <code>recreate = true</code>, then the DPR CSV driver (a thin <code>CSVIngestDriver</code> subclass) pushed
over HTTP with <code>insert_batch_rows = 50000</code>. Wall time is from <code>driver start</code> returning to the driver's last "inserted N observation(s)" log line.
TimescaleDB is <code>{E(tsdb.get('image','?'))}</code> in a docker container inside the same VM, tuned with <code>TS_TUNE_MEMORY=8GB TS_TUNE_NUM_CPUS=4</code>,
fresh container + volume per run; schema as created by the acquirium ref under test.</p>
<div class=tw>{ingest_tbl}</div>
<figure class=fig><figcaption><b>Cumulative rows ingested</b><span class=unit>M rows vs minutes since driver start</span></figcaption>{timeline_fig}
{legend([(l, c) for _, l, c, _ in R])}</figure>
<figure class=fig><figcaption><b>Resident memory</b><span class=unit>GB, sampled every 5 s across boot → ingest → queries; dashed = postgres processes</span></figcaption>{rss_fig}
{legend([(l, c) for _, l, c, _ in R])}</figure>

<h2>Query latency — 30 <code>GET /timeseries</code> queries × {reps} reps</h2>
<p>Identical frozen query set on every run (<code>queries.json</code> from the 2026-08-26 campaign: absolute timestamps anchored on the newest real row).
Per query: 1 cold request (first ever touch, reported separately), {warm} warm-ups discarded, then {reps} timed reps.
"cpu ms" is server-side CPU per request (acquirium process tree, plus postgres processes for timescale; 100 ms resolution). Streams: P1 = <code>{E(Q['streams']['P1'])}</code>,
P2 = <code>{E(Q['streams']['P2'])}</code>, T1 = <code>{E(Q['streams']['T1'])}</code> (text), R1 = ref-URI of "{E(Q['streams']['R1_name'])}". Anchor {E(Q['anchor'][:19])}.</p>
<figure class=fig><figcaption><b>p50 latency per query</b><span class=unit>ms, log scale</span></figcaption>{bar_fig}
{legend([(l, c) for _, l, c, _ in R])}</figure>
<div class=tw>{qt}</div>
<h3>Throughput on the large reads (≥ 50k rows)</h3>
<div class=tw>{thr}</div>

<h2>TimescaleDB state after ingest</h2>
<div class=tw>{idx_tbl}</div>
{ts_state}
<pre class=code>{E(str(tsdb.get('settings','')))}</pre>

<h2>Query definitions</h2>
<div class=tw>{qdef}</div>
</div>"""
out.write_text(page.replace("/>", " />"))
print("wrote", out)

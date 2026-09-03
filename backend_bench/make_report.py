#!/usr/bin/env python3
"""Render reports/backend_duckdb_vs_timescale.html from a results dir
(duckdb.json, timescale.json, queries.json, *.samples.jsonl)."""
from __future__ import annotations
import html, json, sys
from pathlib import Path

res = Path(sys.argv[1]); out = Path(sys.argv[2])
D = json.load(open(res / "duckdb.json")); T = json.load(open(res / "timescale.json"))
Q = json.load(open(res / "queries.json"))
B = [("duckdb", D, "series-1"), ("timescale", T, "series-2")]
def samples(b):
    p = res / f"{b}.samples.jsonl"
    return [json.loads(l) for l in open(p)] if p.exists() else []
S = {b: samples(b) for b, _, _ in B}
E = html.escape
def f0(x): return "—" if x is None else f"{x:,.0f}"
def f1(x): return "—" if x is None else f"{x:,.1f}"
def f2(x): return "—" if x is None else f"{x:,.2f}"
def gb(x): return "—" if not x else f"{x/1e9:.2f} GB"
def mins(x): return "—" if x is None else f"{x/60:.1f} min"

# ---------------------------------------------------------------- helpers
def qmap(b): return {r["id"]: r for r in b.get("query", {}).get("results", [])}
QD, QT = qmap(D), qmap(T)

def line_chart(series, w=900, h=260, xlabel="minutes", ylabel="M rows"):
    """series: [(name, cls, [(x,y),...])]"""
    pad_l, pad_r, pad_t, pad_b = 52, 16, 12, 30
    xs = [x for _, _, pts in series for x, _ in pts]; ys = [y for _, _, pts in series for _, y in pts]
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
    for name, cls, pts in series:
        d = " ".join(f"{'M' if i == 0 else 'L'}{sx(x):.1f},{sy(y):.1f}" for i, (x, y) in enumerate(pts))
        paths.append(f'<path d="{d}" fill=none style="stroke:var(--{cls})" stroke-width=2 stroke-linejoin=round/>')
        if pts:
            x, y = pts[-1]; paths.append(f'<text class=barlab x={sx(x)-4:.1f} y={sy(y)-6:.1f} text-anchor=end>{name}</text>')
    return (f'<svg class=chart viewBox="0 0 {w} {h}">{"".join(g)}<line class=axis x1={pad_l} x2={w-pad_r} y1={h-pad_b} y2={h-pad_b}/>'
            f'{"".join(paths)}<text class=tick x={w-pad_r} y={h-8} text-anchor=end>{xlabel}</text>'
            f'<text class=tick x={pad_l} y={pad_t} text-anchor=start>{ylabel}</text></svg>')

def hbar_log(rows, w=900):
    """rows: [(label, v_duck, v_ts)] grouped horizontal bars, log x (ms)."""
    import math
    rh, gap, pad_l, pad_r, top = 11, 6, 250, 60, 22
    vals = [v for _, a, b in rows for v in (a, b) if v]
    lo, hi = math.log10(max(min(vals) * .8, .1)), math.log10(max(vals) * 1.3)
    span = w - pad_l - pad_r
    sx = lambda v: pad_l + (math.log10(max(v, .1)) - lo) / (hi - lo) * span
    h = top + len(rows) * (2 * rh + gap) + 24
    g = []
    t = 10 ** math.floor(lo)
    while t <= 10 ** hi:
        if lo <= math.log10(t) <= hi:
            g.append(f'<line class=grid x1={sx(t):.1f} x2={sx(t):.1f} y1={top-4} y2={h-20}/>'
                     f'<text class="tick tick-x" x={sx(t):.1f} y={h-6}>{t:g} ms</text>')
        t *= 10
    for i, (lab, a, b) in enumerate(rows):
        y = top + i * (2 * rh + gap)
        g.append(f'<text class=tick x={pad_l-8} y={y+rh+3} text-anchor=end>{E(lab)}</text>')
        for j, (v, cls) in enumerate(((a, "series-1"), (b, "series-2"))):
            if v:
                g.append(f'<rect x={pad_l} y={y+j*rh} width={max(sx(v)-pad_l,1):.1f} height={rh-1} rx=3 style="fill:var(--{cls})"><title>{E(lab)}: {v} ms</title></rect>'
                         f'<text class=barlab x={sx(v)+4:.1f} y={y+j*rh+rh-2} text-anchor=start>{v:g}</text>')
    return f'<svg class=chart viewBox="0 0 {w} {h}">{"".join(g)}</svg>'

# ---------------------------------------------------------------- sections
ing = {b: o.get("ingest", {}) for b, o, _ in B}
db = {b: o.get("db", {}) for b, o, _ in B}
def ratio(a, b, inv=False):
    if not a or not b: return "—"
    r = (b / a) if inv else (a / b)
    return f"{r:.2f}×"

tiles = ""
for b, o, cls in B:
    i = ing[b]
    tiles += (f'<div class=tile><div class=k>{b} ingest</div><div class=v style="color:var(--{cls})">{f0(i.get("rows_per_s"))} rows/s</div>'
              f'<div class=n>{i.get("rows_inserted",0):,} rows · {mins(i.get("wall_s"))}{"" if i.get("complete") else " · ⚠ incomplete"}</div></div>')
dp = [r for r in QD.values() if r.get("p50_ms") is not None]
def med_ratio():
    rs = [QT[k]["p50_ms"] / QD[k]["p50_ms"] for k in QD if k in QT and QD[k].get("p50_ms") and QT[k].get("p50_ms")]
    rs.sort(); return rs[len(rs)//2] if rs else None
mr = med_ratio()
tiles += (f'<div class=tile><div class=k>query p50, median over 30 queries</div><div class=v>{f2(mr)}×</div>'
          f'<div class=n>timescale ÷ duckdb (&gt;1 = duckdb faster)</div></div>')
tiles += (f'<div class=tile><div class=k>on-disk size</div><div class=v>{gb(db["duckdb"].get("db_bytes"))} / {gb(db["timescale"].get("db_bytes"))}</div>'
          f'<div class=n>duckdb / timescale · {f0(db["duckdb"].get("bytes_per_row"))} / {f0(db["timescale"].get("bytes_per_row"))} B/row</div></div>')

ingest_rows = [
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
ingest_tbl = "<table><tr><th>metric</th><th>duckdb</th><th>timescale</th></tr>" + "".join(
    f"<tr><td>{E(k)}</td><td class=num>{fn(ing['duckdb'], D)}</td><td class=num>{fn(ing['timescale'], T)}</td></tr>"
    for k, fn in ingest_rows) + "</table>"

tl = [(b, cls, [(p["t_s"] / 60, p["rows"] / 1e6) for p in ing[b].get("timeline", [])]) for b, _, cls in B]
timeline_fig = line_chart(tl)

def rss_series(b, key):
    s = S[b]
    if not s: return []
    t0 = s[0]["t"]; return [((x["t"] - t0) / 60, x[key] / 1024) for x in s]
rss_fig = line_chart([("duckdb server", "series-1", rss_series("duckdb", "server_rss_mb")),
                      ("timescale server", "series-2", rss_series("timescale", "server_rss_mb")),
                      ("postgres", "series-3", rss_series("timescale", "pg_rss_mb"))], ylabel="GB RSS")

# query table
qrows = []
for q in Q["queries"]:
    a, b = QD.get(q["id"], {}), QT.get(q["id"], {})
    qrows.append((q, a, b))
def cell(r, k, fmt=f2):
    v = r.get(k); return f"<td class=num>{fmt(v) if v is not None else E(r.get('error','—')[:40])}</td>"
qt = ("<table><tr><th>#</th><th>query</th><th>rows</th><th>bytes</th>"
      "<th>duck p50</th><th>duck p95</th><th>duck cold</th><th>duck cpu ms</th>"
      "<th>ts p50</th><th>ts p95</th><th>ts cold</th><th>ts cpu ms</th><th>ts ÷ duck p50</th></tr>")
for q, a, b in qrows:
    r = (b["p50_ms"] / a["p50_ms"]) if a.get("p50_ms") and b.get("p50_ms") else None
    win = "" if r is None else ("win-m" if r > 1 else "win-b")
    warn = "".join(f' <span class=warn title="{E(x.get("warning",""))}">⚠</span>' for x in (a, b) if x.get("warning"))
    qt += (f"<tr><td>{q['id']}</td><td>{E(q['label'])}{warn}</td><td class=num>{a.get('rows', b.get('rows','—')):,}</td>"
           f"<td class=num>{f0(a.get('bytes'))}</td>{cell(a,'p50_ms')}{cell(a,'p95_ms')}{cell(a,'cold_ms')}"
           f"<td class=num>{f1(a.get('server_cpu_s_per_req',0)*1e3) if a.get('server_cpu_s_per_req') is not None else '—'}</td>"
           f"{cell(b,'p50_ms')}{cell(b,'p95_ms')}{cell(b,'cold_ms')}"
           f"<td class=num>{f1(b.get('server_cpu_s_per_req',0)*1e3) if b.get('server_cpu_s_per_req') is not None else '—'}</td>"
           f"<td class='num {win}'>{f2(r)}×</td></tr>")
qt += "</table>"
bar_fig = hbar_log([(f"{q['id']}. {q['label']}", a.get("p50_ms"), b.get("p50_ms")) for q, a, b in qrows])

# throughput on the big reads
big = [(q, a, b) for q, a, b in qrows if a.get("rows", 0) >= 50000]
thr = "<table><tr><th>query</th><th>rows</th><th>duck rows/s</th><th>duck MB/s</th><th>ts rows/s</th><th>ts MB/s</th></tr>" + "".join(
    f"<tr><td>{E(q['label'])}</td><td class=num>{a.get('rows',0):,}</td><td class=num>{f0(a.get('rows_per_s'))}</td><td class=num>{f1(a.get('mb_per_s'))}</td>"
    f"<td class=num>{f0(b.get('rows_per_s'))}</td><td class=num>{f1(b.get('mb_per_s'))}</td></tr>" for q, a, b in big) + "</table>"

def params(p): return ", ".join(f"{k}={v}" for k, v in p.items() if k != "uri")
qdef = "<table><tr><th>#</th><th>label</th><th>stream</th><th>params</th></tr>" + "".join(
    f"<tr><td>{q['id']}</td><td>{E(q['label'])}</td><td><code>{E(q['params']['uri'])}</code></td><td>{E(params(q['params']))}</td></tr>"
    for q in Q["queries"]) + "</table>"

tsdb = T.get("tsdb", {})
errs = "".join(f'<div class="note bad"><b>{b} error:</b> {E(o["error"])}</div>' for b, o, _ in B if o.get("error"))

CSS = open(Path(__file__).parent / "report.css").read()
page = f"""<title>DuckDB vs TimescaleDB Backend</title>
<style>{CSS}</style>
<div class=wrap>
<h1>acquirium timeseries backend: DuckDB vs TimescaleDB</h1>
<p class=sub>acquirium <code>main</code> @ <code>{E(D.get('head','?')[:10])}</code> · DPR trailer SCADA, 200 CSVs · cloud-vm (4 vCPU / 16 GiB cgroup) ·
duckdb run {E(D.get('started','')[:16])} → timescale run {E(T.get('finished','')[:16])} UTC</p>
{errs}
<div class=tiles>{tiles}</div>

<h2>What the numbers say</h2>
<div class="note"><b>Ingest.</b> DuckDB took the 200 files at <b>{f0(ing['duckdb'].get('rows_per_s'))} rows/s</b> ({mins(ing['duckdb'].get('wall_s'))}), TimescaleDB at
<b>{f0(ing['timescale'].get('rows_per_s'))} rows/s</b> ({mins(ing['timescale'].get('wall_s'))}) — a {ratio(ing['duckdb'].get('rows_per_s'), ing['timescale'].get('rows_per_s'))} gap.
With Timescale, postgres itself ran at {ing['timescale'].get('cpu_utilisation_pg')} cores for the whole ingest while the acquirium process idled at {ing['timescale'].get('cpu_utilisation_server')} cores:
the bottleneck is the server-side upsert + 5 index maintenances per row, not the CSV parsing or the HTTP path (which is identical on both runs).</div>
<div class="note"><b>Disk.</b> {gb(db['duckdb'].get('db_bytes'))} vs {gb(db['timescale'].get('db_bytes'))}. The Timescale hypertable is
{gb(int(T.get('db',{}).get('hypertable','0|0|0|0').split('|')[0]))} of table data and <b>{gb(int(T.get('db',{}).get('hypertable','0|0|0|0').split('|')[1]))} of indexes</b> — the acquirium schema
creates a unique (ref_uri, ts) index plus four partial indexes, and none of the 12 chunks were compressed yet (the compression policy only touches chunks older than 7 days, on its own schedule).
That is {f0(db['timescale'].get('bytes_per_row'))} B/row vs {f0(db['duckdb'].get('bytes_per_row'))} B/row.</div>
<div class="note"><b>Queries.</b> The two backends win different regimes. On the large scans (≥ 60k rows) DuckDB is ~20–30% faster (e.g. 535k rows: {f2(QD.get(1,{}).get('p50_ms'))} ms vs {f2(QT.get(1,{}).get('p50_ms'))} ms).
On point/small-window reads (≤ 10k rows) Timescale is 2–3× faster: it answers in ~1.2–1.5 ms where DuckDB has a ~3 ms floor (a fresh connection per read, see the duckdb-index notes).
Two shapes stand out: <i>latest 1 desc</i> ({f2(QD.get(23,{}).get('p50_ms'))} vs {f2(QT.get(23,{}).get('p50_ms'))} ms) and <i>30 days limit 10000</i>
({f2(QD.get(26,{}).get('p50_ms'))} vs {f2(QT.get(26,{}).get('p50_ms'))} ms) — DuckDB appears to materialise the whole window before applying LIMIT, postgres stops at the index scan.
Both EMPTY queries return 0 rows on both backends in ~1.3–2.5 ms.</div>
<div class="note"><b>Cold reads.</b> The first-ever request against each large stream costs an extra ~200 ms on Timescale (629 ms cold vs 408 ms warm on P1 full range; the smoke run even saw a 7 s first query) as pages come off disk into shared_buffers; DuckDB's cold and warm numbers are within noise.</div>
<h2>Ingest</h2>
<p>Same code path for both: <code>acquirium server</code> with <code>recreate = true</code>, then the DPR CSV driver (a thin <code>CSVIngestDriver</code> subclass) pushed
over HTTP with <code>insert_batch_rows = 50000</code>. Wall time is from <code>driver start</code> returning to the driver's last "inserted N observation(s)" log line.
TimescaleDB is <code>{E(tsdb.get('image','?'))}</code> in a docker container inside the same VM, tuned with <code>TS_TUNE_MEMORY=8GB TS_TUNE_NUM_CPUS=4</code>,
default acquirium schema (hypertable on <code>ts</code>, 5 indexes, compression policy after 7 days).</p>
<div class=tw>{ingest_tbl}</div>
<figure class=fig><figcaption><b>Cumulative rows ingested</b><span class=unit>M rows vs minutes since driver start</span></figcaption>{timeline_fig}
<div class=legend><span class=lg><i style="background:var(--series-1)"></i>duckdb</span><span class=lg><i style="background:var(--series-2)"></i>timescale</span></div></figure>
<figure class=fig><figcaption><b>Resident memory</b><span class=unit>GB, sampled every 5 s across boot → ingest → queries</span></figcaption>{rss_fig}
<div class=legend><span class=lg><i style="background:var(--series-1)"></i>duckdb server</span><span class=lg><i style="background:var(--series-2)"></i>timescale server</span><span class=lg><i style="background:var(--series-3)"></i>postgres processes</span></div></figure>

<h2>Query latency — 30 <code>GET /timeseries</code> queries × {D.get('query',{}).get('reps','?')} reps</h2>
<p>Identical query set on both backends (<code>queries.json</code>, frozen from the duckdb run: absolute timestamps anchored on the newest real row).
Per query: 1 cold request (first ever touch, reported separately), {D.get('query',{}).get('warmup','?')} warm-ups discarded, then {D.get('query',{}).get('reps','?')} timed reps.
"cpu ms" is server-side CPU per request (acquirium process tree, plus postgres processes for timescale; 100 ms resolution — 1 s <code>ps</code> granularity over 10 reps, so small queries read 0 or 100). Streams: P1 = <code>{E(Q['streams']['P1'])}</code>,
P2 = <code>{E(Q['streams']['P2'])}</code>, T1 = <code>{E(Q['streams']['T1'])}</code> (text), R1 = ref-URI of "{E(Q['streams']['R1_name'])}". Anchor {E(Q['anchor'][:19])}.</p>
<figure class=fig><figcaption><b>p50 latency per query</b><span class=unit>ms, log scale</span></figcaption>{bar_fig}
<div class=legend><span class=lg><i style="background:var(--series-1)"></i>duckdb</span><span class=lg><i style="background:var(--series-2)"></i>timescale</span></div></figure>
<div class=tw>{qt}</div>
<h3>Throughput on the large reads (≥ 50k rows)</h3>
<div class=tw>{thr}</div>

<h2>TimescaleDB state after ingest</h2>
<pre class=code>{E(json.dumps(T.get('db',{}), indent=1))}</pre>
<pre class=code>{E(str(tsdb.get('settings','')))}</pre>

<h2>Query definitions</h2>
<div class=tw>{qdef}</div>
</div>"""
out.write_text(page.replace("/>", " />"))
print("wrote", out)

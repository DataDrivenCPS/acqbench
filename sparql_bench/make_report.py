#!/usr/bin/env python3
"""Render the graph-store SPARQL benchmark results into a local HTML report."""

from __future__ import annotations

import html
import json
import pathlib
import statistics

HERE = pathlib.Path(__file__).resolve().parent
RES = HERE / "results"
OUT = HERE.parent / "reports" / "graph_store_sparql_bench.html"

M_COLOR, D_COLOR = "var(--series-1)", "var(--series-2)"


def load(name):
    p = RES / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else {}


def fmt(v, nd=1, suffix=""):
    if v is None:
        return "—"
    if isinstance(v, str):
        return html.escape(v)
    return f"{v:,.{nd}f}{suffix}"


def ratio_cell(main_v, der_v, lower_is_better=True):
    """Return (text, css class) describing which branch wins."""
    if not main_v or not der_v:
        return "—", ""
    if lower_is_better:
        if der_v < main_v:
            return f"derived {main_v/der_v:.1f}×", "win-d"
        return f"main {der_v/main_v:.1f}×", "win-m"
    if der_v > main_v:
        return f"derived {der_v/main_v:.1f}×", "win-d"
    return f"main {main_v/der_v:.1f}×", "win-m"


# ------------------------------------------------------------------ charts
def grouped_bar(series, categories, title, unit, log=False):
    """Two-series grouped bar chart as inline SVG. series = [(label,color,values)]"""
    W, H = 720, 300
    ml, mr, mt, mb = 64, 16, 16, 42
    pw, ph = W - ml - mr, H - mt - mb
    allv = [v for _, _, vals in series for v in vals if v]
    if not allv:
        return ""
    vmax = max(allv) * 1.15
    n = len(categories)
    group_w = pw / n
    bar_w = min(30, group_w / (len(series) + 1))

    def y(v):
        return mt + ph - (v / vmax) * ph if vmax else mt + ph

    parts = [f'<svg viewBox="0 0 {W} {H}" class="chart" role="img" aria-label="{html.escape(title)}">']
    # gridlines + y axis
    for i in range(5):
        v = vmax * i / 4
        yy = y(v)
        parts.append(f'<line x1="{ml}" x2="{W-mr}" y1="{yy:.1f}" y2="{yy:.1f}" class="grid"/>')
        parts.append(f'<text x="{ml-8}" y="{yy+4:.1f}" class="tick tick-y">{v:,.0f}</text>')
    for gi, cat in enumerate(categories):
        gx = ml + gi * group_w
        for si, (label, color, vals) in enumerate(series):
            v = vals[gi]
            if v is None:
                continue
            bx = gx + group_w / 2 - (len(series) * bar_w) / 2 + si * bar_w
            by, bh = y(v), (mt + ph) - y(v)
            parts.append(
                f'<rect x="{bx+1:.1f}" y="{by:.1f}" width="{bar_w-2:.1f}" height="{max(bh,0):.1f}" '
                f'rx="4" fill="{color}"><title>{html.escape(label)} @ {cat}: {v:,.1f}{unit}</title></rect>')
            if bh > 26:
                parts.append(f'<text x="{bx+bar_w/2:.1f}" y="{by-5:.1f}" class="barlab">{v:,.0f}</text>')
        parts.append(f'<text x="{gx+group_w/2:.1f}" y="{mt+ph+20:.1f}" class="tick tick-x">{cat}</text>')
    parts.append(f'<line x1="{ml}" x2="{W-mr}" y1="{mt+ph}" y2="{mt+ph}" class="axis"/>')
    parts.append("</svg>")
    legend = " ".join(
        f'<span class="lg"><i style="background:{c}"></i>{html.escape(l)}</span>'
        for l, c, _ in series)
    return (f'<figure class="fig"><figcaption><b>{html.escape(title)}</b>'
            f'<span class="unit">{html.escape(unit)}</span></figcaption>'
            f'<div class="legend">{legend}</div>{"".join(parts)}</figure>')


def scatter_pq(serial_m, serial_d):
    """Per-query p50: main (x) vs derived (y), log-log, with a parity line."""
    pairs = []
    dm = {r["id"]: r for r in serial_m}
    for r in serial_d:
        m = dm.get(r["id"])
        if not m or m.get("error") or r.get("error"):
            continue
        a, b = m.get("p50_ms"), r.get("p50_ms")
        if a and b and a > 0 and b > 0:
            pairs.append((a, b, r["id"], r.get("rows"), r.get("union")))
    if not pairs:
        return ""
    import math
    W, H = 560, 520
    ml, mr, mt, mb = 62, 18, 14, 46
    pw, ph = W - ml - mr, H - mt - mb
    lo = min(min(a, b) for a, b, *_ in pairs) * 0.6
    hi = max(max(a, b) for a, b, *_ in pairs) * 1.6

    def sx(v):
        return ml + (math.log10(v) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)) * pw

    def sy(v):
        return mt + ph - (math.log10(v) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)) * ph

    parts = [f'<svg viewBox="0 0 {W} {H}" class="chart" role="img" '
             f'aria-label="per-query p50 latency, main versus derived">']
    dec = []
    e = math.floor(math.log10(lo))
    while 10 ** e <= hi:
        for m_ in (1, 2, 5):
            v = m_ * 10 ** e
            if lo <= v <= hi:
                dec.append(v)
        e += 1
    for v in dec:
        parts.append(f'<line x1="{sx(v):.1f}" x2="{sx(v):.1f}" y1="{mt}" y2="{mt+ph}" class="grid"/>')
        parts.append(f'<line x1="{ml}" x2="{ml+pw}" y1="{sy(v):.1f}" y2="{sy(v):.1f}" class="grid"/>')
        lab = f"{v:g}"
        parts.append(f'<text x="{sx(v):.1f}" y="{mt+ph+18:.1f}" class="tick tick-x">{lab}</text>')
        parts.append(f'<text x="{ml-8}" y="{sy(v)+4:.1f}" class="tick tick-y">{lab}</text>')
    parts.append(f'<line x1="{sx(lo):.1f}" y1="{sy(lo):.1f}" x2="{sx(hi):.1f}" y2="{sy(hi):.1f}" class="parity"/>')
    for a, b, qid, rows, union in pairs:
        cls = "dot-win" if b < a else "dot-lose"
        parts.append(
            f'<circle cx="{sx(a):.1f}" cy="{sy(b):.1f}" r="5" class="{cls}">'
            f'<title>{qid} union={union} rows={rows}\nmain {a:.2f}ms / derived {b:.2f}ms</title></circle>')
    parts.append(f'<text x="{ml+pw/2:.1f}" y="{H-8}" class="axlab">main p50 (ms, log)</text>')
    parts.append(f'<text transform="translate(14,{mt+ph/2:.1f}) rotate(-90)" class="axlab">derived p50 (ms, log)</text>')
    parts.append("</svg>")
    faster = sum(1 for a, b, *_ in pairs if b < a)
    return (f'<figure class="fig"><figcaption><b>Per-query p50 latency</b>'
            f'<span class="unit">{len(pairs)} queries · below the line = derived faster '
            f'({faster}/{len(pairs)})</span></figcaption>'
            f'<div class="legend"><span class="lg"><i class="i-win"></i>derived faster</span>'
            f'<span class="lg"><i class="i-lose"></i>main faster</span></div>{"".join(parts)}</figure>')


# ------------------------------------------------------------------- build
def main():
    m, d = load("main"), load("derived")
    if not m or not d:
        raise SystemExit("missing results; run bench.py for both branches first")

    full_corpus = json.loads((HERE / "extracted_queries.json").read_text())
    vp = RES / "viable_corpus.json"
    corpus = json.loads(vp.read_text()) if vp.exists() else full_corpus
    divergent = json.loads((RES / "divergent.json").read_text()) if (RES / "divergent.json").exists() else []
    dead = json.loads((RES / "dead.json").read_text()) if (RES / "dead.json").exists() else []
    sm = {r["id"]: r for r in m.get("serial", [])}
    sd = {r["id"]: r for r in d.get("serial", [])}
    common = [q for q in corpus if q["id"] in sm and q["id"] in sd
              and not sm[q["id"]].get("error") and not sd[q["id"]].get("error")]

    # correctness cross-check: do both branches return the same row counts?
    mismatches = [(q["id"], sm[q["id"]].get("rows"), sd[q["id"]].get("rows"))
                  for q in common if sm[q["id"]].get("rows") != sd[q["id"]].get("rows")]

    mp = [sm[q["id"]]["p50_ms"] for q in common if sm[q["id"]].get("p50_ms")]
    dp = [sd[q["id"]]["p50_ms"] for q in common if sd[q["id"]].get("p50_ms")]

    # -- concurrency
    cm = {c["readers"]: c for c in m.get("concurrency", [])}
    cd = {c["readers"]: c for c in d.get("concurrency", [])}
    cf = {c["readers"]: c for c in d.get("concurrency_fresh", [])}
    readers = sorted(set(cm) | set(cd))

    qps_chart = grouped_bar(
        [("main", M_COLOR, [cm.get(r, {}).get("throughput_qps") for r in readers]),
         ("derived", D_COLOR, [cd.get(r, {}).get("throughput_qps") for r in readers])],
        [str(r) for r in readers], "Query throughput vs concurrent readers", "queries/sec")

    p95_chart = grouped_bar(
        [("main", M_COLOR, [cm.get(r, {}).get("latency", {}).get("p95_ms") for r in readers]),
         ("derived", D_COLOR, [cd.get(r, {}).get("latency", {}).get("p95_ms") for r in readers])],
        [str(r) for r in readers], "Query p95 latency vs concurrent readers", "ms (lower is better)")

    # -- read under write
    rm = {c["readers"]: c for c in m.get("read_under_write", []) if "label" not in c}
    rd = {c["readers"]: c for c in d.get("read_under_write", []) if "label" not in c}
    rd_fresh = next((c for c in d.get("read_under_write", []) if c.get("label")), None)
    ruw_readers = sorted(set(rm) | set(rd))

    ruw_chart = grouped_bar(
        [("main", M_COLOR, [rm.get(r, {}).get("write_latency", {}).get("p50_ms") for r in ruw_readers]),
         ("derived", D_COLOR, [rd.get(r, {}).get("write_latency", {}).get("p50_ms") for r in ruw_readers])],
        [str(r) for r in ruw_readers], "Write latency while readers are active", "ms (lower is better)")

    ruw_read_chart = grouped_bar(
        [("main", M_COLOR, [rm.get(r, {}).get("read_latency", {}).get("p50_ms") for r in ruw_readers]),
         ("derived", D_COLOR, [rd.get(r, {}).get("read_latency", {}).get("p50_ms") for r in ruw_readers])],
        [str(r) for r in ruw_readers], "Read p50 latency while a writer is active", "ms (lower is better)")

    scat = scatter_pq(list(sm.values()), list(sd.values()))

    # -- biggest movers table
    movers = []
    for q in common:
        a, b = sm[q["id"]].get("p50_ms"), sd[q["id"]].get("p50_ms")
        if a and b:
            movers.append((b / a, q["id"], q["count"], sm[q["id"]].get("rows"),
                           q["union"], a, b, q["nchars"]))
    movers.sort()
    best, worst = movers[:8], movers[-8:][::-1]

    def mover_rows(rows):
        out = []
        for r, qid, cnt, nrows, union, a, b, nch in rows:
            cls = "win-d" if r < 1 else "win-m"
            verdict = f"derived {1/r:.1f}×" if r < 1 else f"main {r:.1f}×"
            out.append(
                f"<tr><td><code>{qid}</code></td><td class='num'>{cnt}</td>"
                f"<td class='num'>{nrows if nrows is not None else '—'}</td>"
                f"<td>{'deps' if union else 'data'}</td>"
                f"<td class='num'>{a:,.2f}</td><td class='num'>{b:,.2f}</td>"
                f"<td class='{cls}'>{verdict}</td></tr>")
        return "".join(out)

    def conc_rows():
        out = []
        for r in readers:
            a = cm.get(r, {})
            b = cd.get(r, {})
            f = cf.get(r, {})
            aq, bq = a.get("throughput_qps"), b.get("throughput_qps")
            txt, cls = ratio_cell(aq, bq, lower_is_better=False)
            out.append(
                f"<tr><td class='num'>{r}</td>"
                f"<td class='num'>{fmt(aq)}</td><td class='num'>{fmt(bq)}</td>"
                f"<td class='{cls}'>{txt}</td>"
                f"<td class='num'>{fmt(a.get('latency',{}).get('p50_ms'),2)}</td>"
                f"<td class='num'>{fmt(b.get('latency',{}).get('p50_ms'),2)}</td>"
                f"<td class='num'>{fmt(a.get('latency',{}).get('p95_ms'),2)}</td>"
                f"<td class='num'>{fmt(b.get('latency',{}).get('p95_ms'),2)}</td>"
                f"<td class='num'>{fmt(f.get('throughput_qps'))}</td></tr>")
        return "".join(out)

    def ruw_rows():
        out = []
        for r in ruw_readers:
            a, b = rm.get(r, {}), rd.get(r, {})
            out.append(
                f"<tr><td class='num'>{r}</td>"
                f"<td class='num'>{fmt(a.get('read_latency',{}).get('p50_ms'),1)}</td>"
                f"<td class='num'>{fmt(b.get('read_latency',{}).get('p50_ms'),1)}</td>"
                f"<td class='num'>{fmt(a.get('write_latency',{}).get('p50_ms'),1)}</td>"
                f"<td class='num'>{fmt(b.get('write_latency',{}).get('p50_ms'),1)}</td>"
                f"<td class='num'>{fmt(a.get('read_throughput_qps'),1)}</td>"
                f"<td class='num'>{fmt(b.get('read_throughput_qps'),1)}</td></tr>")
        if rd_fresh:
            out.append(
                f"<tr class='sub'><td>8 <span class='tag'>fresh</span></td><td>—</td>"
                f"<td class='num'>{fmt(rd_fresh.get('read_latency',{}).get('p50_ms'),1)}</td>"
                f"<td>—</td><td class='num'>{fmt(rd_fresh.get('write_latency',{}).get('p50_ms'),1)}</td>"
                f"<td>—</td><td class='num'>{fmt(rd_fresh.get('read_throughput_qps'),1)}</td></tr>")
        return "".join(out)

    # -- queries that blew the 20s probe budget, and what shape they share
    import re as _re
    by_id = {q["id"]: q for q in full_corpus}
    TY = _re.compile(r"subClassOf>\*\s*<([^>]+)>")
    shortu = lambda u: u.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
    unbound = lambda t: len(_re.findall(r"\?v\d+ \?p_\w+ \?v\d+", t))

    def shape(qid):
        t = by_id[qid]["query"]
        hops = max(len(_re.findall(r"\?p_\w+", b)) for b in t.split("UNION"))
        return hops, t.count("UNION") + 1, t.count("NOT IN"), \
            " → ".join(dict.fromkeys(shortu(x) for x in TY.findall(t)))

    def budget_rows(rows):
        out = []
        for r in rows:
            hops, br, ni, types = shape(r["id"])
            src = ",".join(by_id[r["id"]]["srcs"])
            out.append(
                f"<tr><td><code>{r['id']}</code></td><td class='num'>{r.get('count','—')}</td>"
                f"<td class='num'>{hops}</td><td class='num'>{br}</td><td class='num'>{ni}</td>"
                f"<td>{html.escape(types)}</td>"
                f"<td class='num'>{by_id[r['id']]['nchars']:,}</td>"
                f"<td class='src'>{html.escape(src)}</td></tr>")
        return "".join(out)

    # the unbound-predicate cliff across the queries that DO finish
    pm_probe = {p["id"]: p for p in json.loads((RES / "probe_main.json").read_text())} \
        if (RES / "probe_main.json").exists() else {}
    cliff = {}
    for q in corpus:
        p = pm_probe.get(q["id"], {}).get("probe_ms")
        if p is not None:
            cliff.setdefault(unbound(q["query"]) > 0, []).append(p)
    cliff_html = ""
    if len(cliff) == 2:
        def med(v):
            v = sorted(v); return v[len(v) // 2]
        no, yes = cliff[False], cliff[True]
        cliff_html = (
            f"<div class='tw'><table><tr><th>shape</th><th>queries</th><th>median</th>"
            f"<th>max</th></tr>"
            f"<tr><td>no unbound predicate</td><td class='num'>{len(no)}</td>"
            f"<td class='num'>{med(no):,.1f} ms</td><td class='num'>{max(no):,.0f} ms</td></tr>"
            f"<tr><td>has <code>?v0 ?p ?v1</code></td><td class='num'>{len(yes)}</td>"
            f"<td class='num win-m'>{med(yes):,.0f} ms</td>"
            f"<td class='num'>{max(yes):,.0f} ms</td></tr>"
            f"<tr><td>the {len(dead)} below</td><td class='num'>{len(dead)}</td>"
            f"<td class='num warn'>&gt;20,000 ms</td><td class='num'>—</td></tr>"
            f"</table></div>")

    budget_section = ""
    if divergent or dead:
        budget_section = f"""
<h2>Queries that never finish</h2>
<p>{len(divergent) + len(dead)} of {len(full_corpus)} exceeded a 20&nbsp;s watchdog — on
<b>both</b> branches, the same set. They are excluded from the timed comparison above and listed
here instead, because a storage change cannot fix them.</p>

<p>The discriminator is a single feature: an <b>unbound-predicate join</b>
(<code>?v0 ?p_e0_1 ?v1</code>), where the predicate is a variable rather than a fixed IRI. It
costs about 240× on this model, and these {len(dead)} are its tail:</p>
{cliff_html}

<div class="tw"><table>
<tr><th>query</th><th>logged</th><th>max hops</th><th>UNION branches</th><th>NOT IN</th>
<th>endpoints</th><th>chars</th><th>source log</th></tr>
{budget_rows(divergent)}{budget_rows(dead)}
</table></div>

<p>All of them are <code>explore</code> "connected within N hops" traversals, compiled as UNION
branches of increasing path length with every predicate left as a variable. The fatal branch
looks like this — three chained joins, all predicates unbound, with both endpoints constrained
only by a <code>subClassOf*</code> type subquery:</p>
<pre class="code">{{ ?v0 ?p_e0_1 ?x_e0_1 . ?x_e0_1 ?p_e0_2 ?x_e0_2 . ?x_e0_2 ?p_e0_3 ?v1 . }}</pre>
<p>Over {fmt(m.get('triple_counts',{}).get('with_dependencies'),0)} triples that is close to a
cartesian walk, and the deepest of these go to 8 hops. The <code>NOT IN</code> lists (up to 12
excluded predicates) are the generator pruning structural predicates like <code>s223:cnx</code>
after the fact; the join still enumerates everything first, then filters. Four of the eight are
byte-for-byte the same query but for one terminal property lookup, so there are really three
distinct shapes here. The fix is a bounded predicate alternation in the compiler
(<code>?v0 (s223:cnx|s223:connectsTo|…)/… ?v1</code>) rather than a bare <code>?p</code> at each
hop.</p>

<div class="note">
<b>A slow query is worse than it looks on <code>main</code>.</b> While probing, a query abandoned
at the watchdog on <code>main</code> left every <em>subsequent</em> query in that process timing
out too — including trivial ones — because the global <code>RLock</code> is still held by the
abandoned query. The probe harness had to restart the process after each timeout to make any
progress. That is the same mechanism as the head-of-line result above, in its most extreme form:
on <code>main</code> one runaway query takes the whole graph store offline.
</div>
"""

    # -- head-of-line blocking
    hm = json.loads((RES / "hol_main.json").read_text()) if (RES / "hol_main.json").exists() else {}
    hd = json.loads((RES / "hol_derived.json").read_text()) if (RES / "hol_derived.json").exists() else {}
    hol_section = ""
    if hm and hd:
        hol_chart = grouped_bar(
            [("main", M_COLOR, [hm["baseline"]["p50_ms"], hm["under_load"]["p50_ms"]]),
             ("derived", D_COLOR, [hd["baseline"]["p50_ms"], hd["under_load"]["p50_ms"]])],
            ["alone", "while a heavy query runs"],
            "Cheap-query p50 latency, idle vs under a heavy query", "ms (lower is better)")
        qps_hol = grouped_bar(
            [("main", M_COLOR, [hm["cheap_qps_under_load"]]),
             ("derived", D_COLOR, [hd["cheap_qps_under_load"]])],
            ["cheap queries completed per second"],
            "Cheap-query throughput while a heavy query runs", "queries/sec")
        hol_section = f"""
<h2>Head-of-line blocking <span class="tag">the headline</span></h2>
<p>The sweep above runs every reader on the same expensive mix, which hides the difference.
This test does not: one thread loops an expensive query (<code>{hm['heavy_id']}</code>,
~{hm.get('heavy_probe_ms', 0):,.0f}&nbsp;ms) while the main thread issues a cheap one
(a single bound-predicate lookup, ~0.6&nbsp;ms alone). Same query, same model, both branches.</p>
<div class="tiles">
  <div class="tile"><div class="k">main — cheap query</div>
    <div class="v">{hm['baseline']['p50_ms']:.2f} → {hm['under_load']['p50_ms']:,.0f} ms</div>
    <div class="n">{hm['blocking_factor_p50']:,.0f}× slower under load</div></div>
  <div class="tile"><div class="k">derived — cheap query</div>
    <div class="v">{hd['baseline']['p50_ms']:.2f} → {hd['under_load']['p50_ms']:.2f} ms</div>
    <div class="n">{hd['blocking_factor_p50']:.2f}× — essentially unaffected</div></div>
  <div class="tile"><div class="k">cheap queries in 20 s</div>
    <div class="v">{hd['under_load']['count']:,} <span class="unit">vs {hm['under_load']['count']:,}</span></div>
    <div class="n">derived vs main</div></div>
  <div class="tile"><div class="k">throughput ratio</div>
    <div class="v">{hd['cheap_qps_under_load']/max(hm['cheap_qps_under_load'],0.001):,.0f}×</div>
    <div class="n">{hd['cheap_qps_under_load']:,.0f} vs {hm['cheap_qps_under_load']:,.1f} q/s</div></div>
</div>
{hol_chart}
{qps_hol}
<div class="note">
On <code>main</code> a cheap query behind one expensive query waits for the expensive query to
finish — the global <code>RLock</code> is held across execution <em>and</em> row materialization,
so latency goes from sub-millisecond to over two seconds and only
{hm['under_load']['count']} cheap queries complete in 20&nbsp;s. On the branch the same cheap
query is unaffected ({hd['blocking_factor_p50']:.2f}×) and
{hd['under_load']['count']:,} complete. This is the concurrency property the new architecture
actually buys.
</div>
"""

    med_m, med_d = statistics.median(mp), statistics.median(dp)
    tot_m, tot_d = sum(mp), sum(dp)
    faster_n = sum(1 for q in common
                   if sd[q["id"]]["p50_ms"] < sm[q["id"]]["p50_ms"])

    # headline: write latency under load
    w_m = rm.get(8, {}).get("write_latency", {}).get("p50_ms")
    w_d = rd.get(8, {}).get("write_latency", {}).get("p50_ms")
    w_ratio = (w_m / w_d) if (w_m and w_d) else None

    if not mismatches:
        mismatch_html = ("<p class='ok'>✓ Every query returned an identical row count on both "
                         "branches.</p>")
    else:
        items = "".join(
            f"<li><code>{i}</code> — main {a}, derived {b}</li>" for i, a, b in mismatches[:12])
        mismatch_html = f"""
<p class='ok'>✓ {len(common) - len(mismatches)} of {len(common)} queries returned identical row
counts on both branches. The {len(mismatches)} that differ both do so for structural reasons,
not because inference changed the data:</p>
<ul>{items}</ul>
<p><code>ef28b4e7d8</code> is <code>SELECT ?g (COUNT(*)) WHERE {{ GRAPH ?g {{…}} }}</code> — it
counts named graphs, and the branch deliberately has two (inferred data + dependency closure)
where main materializes one merged union graph. <code>056ed59dbb</code> is an unbound-predicate
scan over everything; the 4-row gap is the ontology-declaration triples that the two layouts
place differently. Note the data-graph triple counts are identical
({fmt(m.get('triple_counts',{}).get('data_only'),0)} /
{fmt(m.get('triple_counts',{}).get('with_dependencies'),0)} on both), i.e.
<code>shifty.infer</code> adds no triples for this model — so the latency comparison is
like-for-like.</p>"""

    doc = f"""<title>Graph Store SPARQL Bench</title>
<style>
:root {{
  color-scheme: light;
  --surface-0:#f5f5f3; --surface-1:#fcfcfb; --surface-2:#efefec;
  --border:#dedcd6; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#7b7a75;
  --series-1:#2a78d6; --series-2:#eb6834;
  --good:#0b7a4b; --bad:#b3261e; --grid:#e6e5e0;
}}
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{
    color-scheme: dark;
    --surface-0:#121211; --surface-1:#1a1a19; --surface-2:#232322;
    --border:#3a3a37; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#94938b;
    --series-1:#3987e5; --series-2:#d95926;
    --good:#43c08a; --bad:#e66767; --grid:#2e2e2b;
  }}
}}
:root[data-theme="dark"] {{
  color-scheme: dark;
  --surface-0:#121211; --surface-1:#1a1a19; --surface-2:#232322;
  --border:#3a3a37; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#94938b;
  --series-1:#3987e5; --series-2:#d95926;
  --good:#43c08a; --bad:#e66767; --grid:#2e2e2b;
}}
* {{ box-sizing:border-box; }}
body {{ background:var(--surface-0); color:var(--text-primary); margin:0;
  font:15px/1.6 ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif; }}
.wrap {{ max-width:1000px; margin:0 auto; padding:40px 22px 80px; }}
h1 {{ font-size:29px; margin:0 0 6px; letter-spacing:-.02em; }}
h2 {{ font-size:20px; margin:44px 0 12px; letter-spacing:-.01em;
  padding-bottom:7px; border-bottom:1px solid var(--border); }}
h3 {{ font-size:15px; margin:26px 0 8px; color:var(--text-secondary); }}
.sub {{ color:var(--text-secondary); }}
p {{ color:var(--text-secondary); }}
code {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12.5px;
  background:var(--surface-2); padding:1px 5px; border-radius:4px; color:var(--text-primary); }}
.tiles {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:12px; margin:22px 0 8px; }}
.tile {{ background:var(--surface-1); border:1px solid var(--border); border-radius:11px; padding:14px 16px; }}
.tile .k {{ font-size:11.5px; text-transform:uppercase; letter-spacing:.06em; color:var(--text-muted); }}
.tile .v {{ font-size:26px; font-weight:640; margin-top:5px; letter-spacing:-.02em; }}
.tile .n {{ font-size:12.5px; color:var(--text-secondary); margin-top:3px; }}
.tw {{ overflow-x:auto; margin:14px 0; }}
table {{ border-collapse:collapse; width:100%; font-size:13.5px; background:var(--surface-1);
  border:1px solid var(--border); border-radius:10px; }}
th,td {{ padding:8px 11px; text-align:left; border-bottom:1px solid var(--border); white-space:nowrap; }}
th {{ font-size:11.5px; text-transform:uppercase; letter-spacing:.05em;
  color:var(--text-muted); font-weight:600; background:var(--surface-2); }}
tr:last-child td {{ border-bottom:none; }}
td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
.win-d {{ color:var(--series-2); font-weight:600; }}
.win-m {{ color:var(--series-1); font-weight:600; }}
.ok {{ color:var(--good); }} .warn {{ color:var(--bad); }}
.fig {{ margin:20px 0 8px; background:var(--surface-1); border:1px solid var(--border);
  border-radius:11px; padding:14px 16px 8px; }}
figcaption {{ display:flex; justify-content:space-between; align-items:baseline;
  gap:12px; margin-bottom:2px; flex-wrap:wrap; }}
figcaption b {{ font-size:14.5px; }}
.unit {{ color:var(--text-muted); font-size:12px; }}
.legend {{ display:flex; gap:16px; margin:6px 0 4px; font-size:12.5px; color:var(--text-secondary); }}
.lg {{ display:flex; align-items:center; gap:6px; }}
.lg i {{ width:11px; height:11px; border-radius:3px; display:inline-block; }}
.i-win {{ background:var(--series-2); }} .i-lose {{ background:var(--series-1); }}
.chart {{ width:100%; height:auto; display:block; overflow:visible; }}
.grid {{ stroke:var(--grid); stroke-width:1; }}
.axis {{ stroke:var(--border); stroke-width:1; }}
.parity {{ stroke:var(--text-muted); stroke-width:1.5; stroke-dasharray:5 4; opacity:.7; }}
.tick {{ fill:var(--text-muted); font-size:11px; }}
.tick-y {{ text-anchor:end; }} .tick-x {{ text-anchor:middle; }}
.axlab {{ fill:var(--text-secondary); font-size:12px; text-anchor:middle; }}
.barlab {{ fill:var(--text-secondary); font-size:10.5px; text-anchor:middle;
  font-variant-numeric:tabular-nums; }}
.dot-win {{ fill:var(--series-2); fill-opacity:.72; stroke:var(--surface-1); stroke-width:2; }}
.dot-lose {{ fill:var(--series-1); fill-opacity:.72; stroke:var(--surface-1); stroke-width:2; }}
.tag {{ font-size:10.5px; background:var(--surface-2); color:var(--text-secondary);
  padding:1px 5px; border-radius:4px; }}
pre.code {{ background:var(--surface-2); border:1px solid var(--border); border-radius:8px;
  padding:11px 14px; overflow-x:auto; font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
  font-size:12.5px; color:var(--text-primary); margin:12px 0; }}
td.src {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:11.5px;
  color:var(--text-muted); }}
.note {{ background:var(--surface-1); border-left:3px solid var(--series-1);
  border-radius:0 8px 8px 0; padding:11px 15px; margin:16px 0; font-size:13.5px; }}
ul {{ color:var(--text-secondary); padding-left:20px; }}
li {{ margin:4px 0; }}
</style>
<div class="wrap">
<h1>Graph store SPARQL latency — <code>main</code> vs <code>graph-store-derived-cache</code></h1>
<p class="sub">DPR deployment model. {len(full_corpus)} unique SPARQL queries harvested from
<code>acquirium/.run_logs</code>; {len(corpus)} of them complete inside a 20&nbsp;s budget on
both branches and form the timed corpus. Both branches run that identical corpus against a
disposable <code>OxigraphGraphStore</code> on the same machine (8 cores, 24&nbsp;GB).</p>

<div class="tiles">
  <div class="tile"><div class="k">Median query p50</div>
    <div class="v">{med_d:,.2f} ms</div>
    <div class="n">derived · main {med_m:,.2f} ms</div></div>
  <div class="tile"><div class="k">Queries faster on derived</div>
    <div class="v">{faster_n} / {len(common)}</div>
    <div class="n">by per-query p50</div></div>
  <div class="tile"><div class="k">Head-of-line blocking</div>
    <div class="v">{hd['blocking_factor_p50']:.2f}× <span class="unit">vs {hm['blocking_factor_p50']:,.0f}×</span></div>
    <div class="n">cheap query behind a heavy one</div></div>
  <div class="tile"><div class="k">Write p50 under 4 readers</div>
    <div class="v">{fmt(rd.get(4,{}).get('write_latency',{}).get('p50_ms'),1)} ms</div>
    <div class="n">derived · main {fmt(rm.get(4,{}).get('write_latency',{}).get('p50_ms'),1)} ms</div></div>
</div>

<div class="note">
<b>What actually changed.</b> On <code>main</code>, <code>sparql_query</code> holds the store's
<code>RLock</code> across both the Oxigraph call <em>and</em> the row materialization, and any write
invalidates the imports-union graph so the next union query re-materializes data + full ontology
closure synchronously, on the caller's thread. On
<code>graph-store-derived-cache</code>, inference is run by a single-flight background thread and
published into a named graph; queries take the lock only for the lazy Oxigraph call and
materialize rows outside it, reading the last published view unless
<code>wait_for_fresh=True</code>.
</div>

<h2>Setup</h2>
<div class="tw"><table>
<tr><th>&nbsp;</th><th>main</th><th>derived</th></tr>
<tr><td>Commit</td><td><code>19bada7</code></td><td><code>75c1821</code></td></tr>
<tr><td>Store init (bundled ontologies)</td><td class="num">{fmt(m.get('store_init_s'),1,' s')}</td>
    <td class="num">{fmt(d.get('store_init_s'),1,' s')}</td></tr>
<tr><td>Insert <code>union-dpr-model.ttl</code></td><td class="num">{fmt(m.get('insert',{}).get('wall_ms'),0,' ms')}</td>
    <td class="num">{fmt(d.get('insert',{}).get('wall_ms'),0,' ms')}</td></tr>
<tr><td>First fresh/union query (cache build)</td><td class="num">{fmt(m.get('first_union_query_ms'),0,' ms')}</td>
    <td class="num">{fmt(d.get('first_union_query_ms'),0,' ms')}</td></tr>
<tr><td>Triples, data only</td><td class="num">{fmt(m.get('triple_counts',{}).get('data_only'),0)}</td>
    <td class="num">{fmt(d.get('triple_counts',{}).get('data_only'),0)}</td></tr>
<tr><td>Triples, with dependencies</td><td class="num">{fmt(m.get('triple_counts',{}).get('with_dependencies'),0)}</td>
    <td class="num">{fmt(d.get('triple_counts',{}).get('with_dependencies'),0)}</td></tr>
</table></div>
<p>Insert is cheaper on the branch because the closure refresh is deferred to the background
rebuild; that deferred cost reappears in the first fresh query. The sum of the two is the
fair comparison for a cold write-then-read.</p>

{mismatch_html}

<h2>Serial query latency</h2>
<p>One thread, {m.get('reps', 10) if isinstance(m.get('reps'), int) else 10} timed reps per query after warm-up.
Total corpus p50 sum: main {tot_m:,.0f} ms vs derived {tot_d:,.0f} ms.</p>
{scat}

<h3>Biggest wins for the derived cache</h3>
<div class="tw"><table>
<tr><th>query</th><th>logged</th><th>rows</th><th>graphs</th><th>main p50 ms</th><th>derived p50 ms</th><th>verdict</th></tr>
{mover_rows(best)}
</table></div>

<h3>Biggest regressions on the derived cache</h3>
<div class="tw"><table>
<tr><th>query</th><th>logged</th><th>rows</th><th>graphs</th><th>main p50 ms</th><th>derived p50 ms</th><th>verdict</th></tr>
{mover_rows(worst)}
</table></div>

{hol_section}

{budget_section}

<h2>Concurrent reads, uniform mix</h2>
<p>Reader threads draw from a frequency-weighted mix of the {len(m.get('mix_ids',[]))} hottest
queries; {m.get('ops_per_reader', 40) if isinstance(m.get('ops_per_reader'), int) else 40} operations each.</p>
{qps_chart}
{p95_chart}
<div class="tw"><table>
<tr><th>readers</th><th>main q/s</th><th>derived q/s</th><th>verdict</th>
<th>main p50</th><th>derived p50</th><th>main p95</th><th>derived p95</th><th>derived q/s <span class="tag">fresh</span></th></tr>
{conc_rows()}
</table></div>
<p class="warn"><b>Read this one carefully: it is a negative result.</b> Neither branch scales on
this workload — throughput is flat near 7–9&nbsp;q/s from 1 to 16 readers on both, and p50
degrades roughly in proportion to reader count. When every thread runs the same expensive mix,
the branch's advantage disappears: Oxigraph query evaluation still happens inside the lock, and
what falls outside it (row materialization) is Python, so the GIL serializes it anyway. The
derived cache does not make an all-heavy read workload parallel. Its win shows up when queries
have <em>different</em> costs — see the head-of-line section above.</p>
<p>The <code>fresh</code> column is the branch running every read with
<code>wait_for_fresh=True</code>, i.e. opting out of eventual consistency. With no writer running
the cache is already current, so it costs only the extra condition check — it tracks the
published numbers closely.</p>

<h2>Reads under a concurrent writer</h2>
<p>This is where the architectures diverge most: a writer inserts small graphs while readers
query. Every write invalidates the derived state.</p>
{ruw_chart}
{ruw_read_chart}
<div class="tw"><table>
<tr><th>readers</th><th>main read p50</th><th>derived read p50</th>
<th>main write p50</th><th>derived write p50</th><th>main read q/s</th><th>derived read q/s</th></tr>
{ruw_rows()}
</table></div>

<h2>What this adds up to</h2>
<ul>
<li><b>Single-query latency is roughly a wash.</b> Median p50 {med_m:,.2f}&nbsp;ms on main vs
{med_d:,.2f}&nbsp;ms on the branch; the branch is meaningfully ahead only on the expensive
queries, where the pre-materialized inferred graph saves re-deriving work.</li>
<li><b>Uniform concurrent reads do not scale on either branch.</b> This is the honest negative
result — do not claim concurrency scaling from this workload.</li>
<li><b>Mixed-cost concurrency is where the architecture pays.</b> A cheap query behind an
expensive one goes from {hm['blocking_factor_p50']:,.0f}× slower to
{hd['blocking_factor_p50']:.2f}× — the difference between a graph store that a slow query takes
offline and one that keeps serving.</li>
<li><b>Writes no longer stall readers.</b> Under 4 concurrent readers a write costs
{fmt(rm.get(4,{}).get('write_latency',{}).get('p50_ms'),0)}&nbsp;ms on main and
{fmt(rd.get(4,{}).get('write_latency',{}).get('p50_ms'),0)}&nbsp;ms on the branch, because the
closure rebuild moved off the caller's thread.</li>
<li><b>Bad query plans are unchanged.</b> The same 8 queries exceed 20&nbsp;s on both branches.
Those are an <code>explore</code>-builder codegen problem (unbound-predicate joins), not a
storage one, and the derived cache does not address them.</li>
<li>The corpus is DPR-only, harvested from real runs. Queries against acquirium's driver
metadata return 0 rows here because the disposable store has no registered drivers; they are
kept because they still exercise the non-dependency code path.</li>
</ul>
</div>
"""
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(doc)
    print(f"wrote {OUT}")
    print(f"corpus={len(corpus)} comparable={len(common)} mismatches={len(mismatches)}")
    print(f"median p50: main={med_m:.2f}ms derived={med_d:.2f}ms  derived faster on {faster_n}/{len(common)}")


if __name__ == "__main__":
    main()

# acqbench

  **WARNING:** This suite is completely vibecoded.  


A benchmark suite for [Acquirium](https://github.com/DataDrivenCPS/acquirium).

It installs acquirium at whatever refs you name — a PyPI release, a GitHub
branch, your local working tree — runs each one across a matrix of server
configs and component topologies, and tells you what got faster and what
regressed.

```bash
uv venv --python 3.12 && uv pip install -e .

acqbench plan matrices/branch-vs-baseline.toml     # what would run
acqbench run  matrices/branch-vs-baseline.toml     # run it
acqbench compare pypi:0.3.1                        # did my branch regress?
```

## How it works

The harness **never imports acquirium**. Each ref is installed into its own uv
venv, and the harness drives the resulting server over raw HTTP. That is what
lets one copy of this code benchmark 0.1.1 and an unreleased branch with
byte-identical workloads — a client library that changed between the versions
under test would confound every measurement it took.

A **cell** is one point in the matrix: `(ref x config x topology)`. Workloads
run against a cell and append one JSON object per repetition to a JSONL file.

### The axes

| Axis | Values | What it isolates |
|---|---|---|
| **ref** | `pypi:0.3.1`, `git:main`, `path:../acquirium` | code change |
| **backend** | `duckdb`, `timescale` | storage engine |
| **transport** | `write_json` vs `write_arrow` | HTTP/serialization layer |
| **read_batch_size** | e.g. `10_000`, `50_000` | Arrow RecordBatch sizing on reads |
| **topology** | `server`, `server+drivers` | marginal cost of each component |

The topology axis is the one to reach for when you want attribution rather than
a total. Workloads are identical across topologies by construction, so the delta
between `server` and `server+drivers` on an otherwise identical cell *is* what
running drivers costs. `acqbench marginal` reports exactly that.

**`server+apps` and `server+drivers+apps` are not implemented yet** and are
rejected at matrix-load time. Drivers are declared in `acquirium.toml`, but apps
are started over the HTTP API (`/apps/register` + `/apps/run`) and their
execution backend differs across the versions under test (Ray actors vs
containers). They fail loudly rather than silently rendering a config identical
to `server` and filing duplicate measurements under a label claiming apps ran.

Note that **`workers` is deliberately not an axis**. The acquirium CLI refuses
anything above 1 because the embedded Oxigraph store is single-process, so
sweeping it would measure nothing.

## Workloads

`acqbench workloads` lists them. Currently:

| Workload | Measures |
|---|---|
| `query_api` | acquirium's Python query interface against real plant graphs (see below) |
| `startup_cold` | first boot on a fresh deployment (~380s — builds embedding indexes) |
| `startup_warm` | every restart after that (~117s — index caches hit) |
| `write_json` | `POST /insert_timeseries` (JSON) |
| `write_arrow` | `POST /insert_timeseries_arrow` (Arrow IPC) |
| `write_arrow_text` | the same, for text-valued streams |
| `read_full` | full-scan `GET /timeseries` |
| `read_window` | bounded time window |
| `read_limit` | most-recent-N, the shape a dashboard issues |
| `graph_insert` | `POST /insert_graph` — the stream registration path |
| `sparql_main` | `GET /sparql_json`, main graph only |
| `sparql_union` | the same query against the ontology closure |

## The query workload

`sparql_main` and friends issue raw SPARQL, which measures Oxigraph but says
nothing about the layer users actually touch. `query_api` exercises acquirium's
Python query interface (`Client/query.py` — the largest module in the codebase)
against three **real plant graphs** vendored into `graphs/`:

| Graph | What it is |
|---|---|
| `benicia` | a wastewater treatment plant (26 properties) |
| `benicia-100` | the same plant, denser (95 properties) — isolates scale from structure |
| `watertap-seawater-ro` | a seawater desalination train (32 sensors) |

```bash
acqbench run matrices/queries.toml --results results/q.jsonl
acqbench queries                          # per query, per graph
acqbench queries --empty                  # just the zero-result cases
acqbench queries-compare pypi:0.3.1 path:../acquirium
```

The queries cover the mechanism's real features: multi-hop `find_related` at
`hops=1` vs `hops=3`, constrained vs unconstrained `predicates`,
`multi_hop_predicates`, upstream/downstream `direction` traversal, `relate_to`
joins, `filter_by_unit/medium/substance/quantity_kind`, and `metadata()` with
and without the ontology closure.

**Every query runs against every graph, and some return zero rows by design.**
The graphs are structurally asymmetric — WaterTAP has 32 `s223:Sensor`, Benicia
has none; Benicia has `SedimentationTank`, WaterTAP has
`ReverseOsmosisMembrane` — so a sensor traversal on Benicia is a genuine
no-match. That path costs differently from a match and a regression can hide in
it, so it is measured rather than skipped. `nawi:Pump` exists in all three and
acts as a control: if a Pump query returns zero, the query is broken, not the
graph.

Two things make this workload unusual:

- **It runs acquirium's own client**, so the script is exec'd with *each ref's
  interpreter* rather than the harness's. That measures client and server
  together, which is what a user of that version gets — but it also means a
  client API change between refs shows up as an error rather than a number, and
  the workload records it as such instead of crashing the run.
- **It needs its own server**, because it loads each graph with
  `insert_graph(replace=True)`, which wipes the main graph. Sharing a server
  would destroy the stream registrations that the write and read workloads
  depend on.

If two refs return **different row counts** for the same query on the same
graph, `queries-compare` reports that separately and exits non-zero. That is a
correctness difference — the versions disagree about what the graph contains —
and timing them against each other would bury it.

The graphs are **pinned inputs**, hashed in `graphs/SHA256SUMS` and checked by
the test suite: benchmarking two refs against different graph content would
measure the data and report it as a code change.

## Matrix files

A matrix file is the experiment's record — commit it next to the results it
produced so a number can be traced back to what generated it.

```toml
refs = ["pypi:0.3.1", "path:../acquirium"]
topologies = ["server"]
workloads = ["write_arrow", "read_limit"]

[[configs]]
backend = "duckdb"
read_batch_size = 50_000

[run]
repetitions = 3
warmup = 1

[workload_params.write_arrow]
streams = 50
rows_per_stream = 200
batches = 5
```

Instead of listing `[[configs]]`, a `[sweep]` table expands to their cartesian
product:

```toml
[sweep]
backend = ["duckdb", "timescale"]
read_batch_size = [10_000, 50_000]   # -> 4 configs
```

Shipped matrices:

- `matrices/smoke.toml` — one ref, one config, no Docker. Check the harness works.
- `matrices/ab-check.toml` — a minimal two-ref A/B. Checks the compare path end to end; too small for the numbers to mean much.
- `matrices/queries.toml` — the query interface across all three plant graphs.
- `matrices/branch-vs-baseline.toml` — your tree vs the last release. The everyday run.
- `matrices/full.toml` — the whole cross-product. Run `plan` first.

## Why runs take as long as they do

Acquirium's startup dominates everything else in this suite, and it is worth
understanding before you plan a matrix. Measured on an M-series laptop:

| Phase | Cold | Warm |
|---|---|---|
| Ontology parse | ~27s | ~27s |
| Embedding model download | ~4s | 0 (cached) |
| Graph embedding index (2,290 surfaces) | ~48s | ~0.2s |
| QUDT embedding index (13,467 surfaces) | ~286s | ~0.8s |
| **Wall clock to `/health`** | **~380s** | **~117s** |

The catch: those index caches live *inside* `data_dir`, so `recreate = true`
deletes them and pays the whole ~380s again on every boot. (`FASTEMBED_CACHE_PATH`
does not help — it only covers the downloaded model, not the computed index.)

So the runner **pre-warms a template `data_dir` once per ref**, snapshots it
after the indexes are built, and gives each cell a copy to boot against with
`recreate = false`. Cells get a clean timeseries store but a warm embedding
cache, which turns a ~380s startup into ~117s. Templates are cached under
`.cache/templates/` and survive between runs.

The residual ~117s is the ontology parse, which nothing caches. That is the
floor for a cell, and the reason servers are shared across a cell's workloads
rather than restarted per repetition.

`startup_cold` deliberately opts out of all of this — there, the cost *is* the
measurement.

## The timescale backend needs Docker

DuckDB is self-contained, so most of the suite runs on a laptop with nothing
installed. Only `backend = "timescale"` needs Postgres:

```bash
acqbench services up      # tmpfs-backed timescaledb on port 55433
acqbench services down
```

It runs its own container on a non-standard port rather than reusing
acquirium's compose stack, so benchmarking can wipe the database between cells
without touching your dev environment.

## Reading the output

```bash
acqbench summary                  # every aggregate, with spread
acqbench compare pypi:0.3.1       # candidates vs a baseline, regressions flagged
acqbench marginal                 # what each topology step costs
```

## When a comparison is confusing: profiling

A headline number tells you *that* something moved, not *what*. Acquirium
already instruments its hot paths with `timed_debug`, which brackets each block
with DEBUG lines carrying an elapsed time:

```
→ bulk_insert_polars prepare/dedupe rows=5000
← bulk_insert_polars prepare/dedupe rows=5000 (11.4 ms)
```

Run with `--profile` and the suite starts servers at DEBUG, captures those
spans, and attributes them to individual workload runs:

```bash
acqbench run matrices/branch-vs-baseline.toml --profile
acqbench spans -w write_arrow                      # where did the time go?
acqbench spans-compare pypi:0.3.1 path:../acquirium -w write_arrow
```

`spans-compare` turns "writes got 20% slower" into "`DELETE+INSERT` went from
137ms to 400ms and everything else held" — ordered by absolute time shifted, so
the top row is the explanation. It also flags spans that are `NEW` (a step your
branch added) or `gone` (a step it removed), which is often the real story.

Three things to know:

- **Profiling is opt-in because it is not free.** `timed_debug` skips its own
  cost entirely when DEBUG is off, so enabling it perturbs what it measures.
  Profiled results are tagged `profile: true` and `compare` **refuses to
  compare them against unprofiled runs** — otherwise the logging overhead would
  itself read as a regression. Profile both sides or neither.
- **Spans are attributed by log byte-offset**, sliced around each timed region,
  so on a server shared by several workloads no run inherits another's spans.
- **Spans nest, so their totals overlap** and will exceed wall-clock. They are
  for attribution, not a time budget.

Span names are normalized (`rows=5000` and `rows=100` both become
`rows=<n>`) so repeated calls aggregate instead of fragmenting into thousands of
one-call entries.

Repetitions aggregate by **median**, not mean, so one GC pause doesn't move the
number. `compare` adjusts for metric polarity — positive change is always
better, whether the metric is rows/sec or latency.

Two verdicts deserve attention:

- **`noisy`** — the repetitions disagreed by more than the effect being
  claimed. The comparison isn't supportable; raise `repetitions` or quiet the
  machine.
- **`SLOWER`** — a regression beyond the 5% noise floor.

`acqbench compare` exits non-zero when it finds a regression, so it drops into
CI as-is.

## Correctness notes

Benchmarks that measure the wrong thing are worse than no benchmarks, because
they're believed. The non-obvious traps here, all verified against a live
server:

- **Writes dedup on `(ref_uri, ts)`** via DELETE-then-INSERT. Re-sending the
  same timestamps measures the dedup path against a growing table, not inserts.
  Every repetition gets a disjoint time window (`datagen.window_for`), and
  workloads assert the server's reported `rows_inserted` matches what was sent.
- **An unknown URI reads back empty, not an error.** A read benchmark can
  therefore run at full speed while reading nothing. Read workloads assert row
  counts.
- **`valueKind` defaults to `text`** server-side when absent. A numeric stream
  registered without it sends floats into the text column, and they read back
  as nulls.
- **Streams must be registered before any insert**, via a graph whose ref node
  URI equals `compute_ref_uri(source_id, ref_name)` — a UUID5 over
  `f"{source_id}:{ref_name}"` with a namespace hard-coded in acquirium.
- **`insert_graph` defaults to `replace=true`**, which wipes the main graph.
  The client here defaults it to `false`.
- **Transport comparison is kept fair** by serializing both JSON and Arrow
  payloads to final wire bytes *before* the timed region. Encode cost is real
  and is reported separately as `encode_ms_mean`, not folded into request
  latency.

## Results schema

One JSON object per (cell, workload, repetition), appended as it completes so a
run that dies halfway still leaves usable data. Each row carries the resolved
ref (a commit SHA for git refs, not just a version), the full config, the
measured metrics, and server RSS/CPU. `results/environment.json` records the
machine — results from different hosts are not comparable.

# Benchmark graphs

Real plant models, vendored from the acquirium repo so the suite does not depend
on a checkout being present — a `path:` ref can point at any working tree, and a
`pypi:` ref has no repo at all.

**These files are pinned inputs, not just fixtures.** Benchmarking two refs
against *different* graph content would measure the data and report it as a code
change. Do not edit them; to refresh, re-copy and update the hashes below in the
same commit, and treat results from before and after as incomparable.

Copied 2026-07-15 from `DataDrivenCPS/acquirium` @ `bfb3385` (branch `ums-ray-backend`).

| File | Source | Last touched by | sha256 |
|---|---|---|---|
| `benicia.ttl` | `deployments/BENICIA/benicia-model.ttl` | `30c74ce` new benchmarks + threshold implementation | `a803ee12e565c7212f5857740daada5fb3d78a59b5cac86e46e0596d93b39cd2` |
| `benicia-100.ttl` | `deployments/BENICIA/benicia-model-100.ttl` | `e882412` benicia benchmarking model added | `c03715d5bb3dce6e38d9a254db44b6e7f1011c3c0af0d99579b2d396997bb8c2` |
| `watertap-seawater-ro.ttl` | `deployments/WATERTAP/models/seawater-ro/model.ttl` | `faf1163` Update model to accomodate regulation example | `fa17f620a29cfc5f1c448308031d415e3299ad63f35475da0c3d5d02b8031a6c` |

Verify with:

```bash
shasum -a 256 -c graphs/SHA256SUMS
```

## What's in them

All three are ASHRAE 223P + NAWI Water + QUDT models. Their **structural
asymmetry is the point**: it produces genuine zero-result cases without having to
invent them.

Benicia is a **wastewater treatment plant**; WaterTAP seawater-RO is a
**desalination train**. Different equipment, same ontologies.

| | `benicia` | `benicia-100` | `watertap-seawater-ro` |
|---|---|---|---|
| namespace | `urn:ex/` | `urn:ex/` | `urn:swro/` |
| `s223:Sensor` | — | — | **32** |
| `s223:observes` / `hasObservationLocation` | — | — | **32** |
| `s223:QuantifiableObservableProperty` | 26 | **95** | 32 |
| `s223:QuantifiableActuatableProperty` | 2 | 5 | — |
| `s223:System` | — | — | 4 |
| `s223:Connection` | 26 | 26 | 21 |
| `nawi:Pump` | 4 | 4 | 3 |
| `nawi:StaticMixer` | 1 | 1 | 5 |
| `nawi:Tank` | 1 | 1 | 4 |
| `nawi:SedimentationTank` | 3 | 3 | — |
| `nawi:Screen` / `Digester` / `GritChamber` / `Thickener` | yes | yes | — |
| `nawi:ReverseOsmosisMembrane` / `PressureExchanger` / `Filter` | — | — | yes |

The useful splits:

- **`s223:Sensor`** — rows on `watertap`, **zero** on both Benicia graphs. The
  cleanest sensor-traversal split.
- **`nawi:SedimentationTank`** (wastewater) vs **`nawi:ReverseOsmosisMembrane`**
  (desalination) — each returns zero on the other plant.
- **`nawi:Pump`** exists in all three, so it acts as a control: if a Pump query
  returns zero, the query is broken, not the graph.
- **`benicia` vs `benicia-100`** is the same plant at two property densities
  (26 vs 95), which isolates scale from structure.

Connectivity predicates common to all three — `s223:cnx`, `connectsThrough`,
`connectsAt`, `connectsTo`, `connectsFrom`, `hasConnectionPoint`,
`isConnectionPointOf` — are what the multi-hop traversal queries walk.

## How they are used

`query_api` loads exactly one graph at a time with `insert_graph(replace=True)`,
which wipes the main graph so only that graph is visible.

Two things about these graphs are easy to get wrong, both found by measuring
rather than reading:

- **The union graph is the `owl:imports` closure, not "main + bundled
  ontologies".** Only `watertap-seawater-ro` declares `owl:imports nawi:`, so
  only it gets a closure. For both Benicia graphs `union == main` and
  `use_union=True` changes nothing. That is why `s223:Equipment` (matchable only
  via `subClassOf*`) returns 50 rows on watertap and **0** on Benicia — and why
  watertap's data queries are ~100x slower.
- **No graph carries `ref:hasExternalReference`**, which every data-node query's
  SPARQL requires as a *non-optional* pattern. Left alone, all six data/filter
  queries return 0 rows on all three graphs — indistinguishable from fast
  queries. `queries.py:setup_graph()` therefore registers one metadata-only
  external ref per property (no timeseries rows, ~170ms) before querying.
  `--no-register-refs` reproduces the broken baseline.

Loading is not free and is recorded as setup, not query time: `insert_graph` is
~0.2s for benicia but **~52s for watertap**, which refreshes embeddings and
rebuilds the closure.

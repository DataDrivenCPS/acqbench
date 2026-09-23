# resolve_bench — free-text resolution accuracy

200 fixed examples with ground-truth URIs, for re-testing `/resolve_text` across
refs and matching modes.

## Where the truth comes from

`examples.json` is generated from acquirium's own embedding-cache metadata
(`data_dir/embedding_cache/{graph,qudt}/*_meta.json`), which lists every indexed
`(uri, kind, label, surface)`. Each example takes one concept's surface and
perturbs it the way a person plausibly would type it; the expected answer is the
URI that surface came from. No model output decides what is correct, so the set
stays valid for any ref, and for exact-only mode.

Guards applied at generation time: a surface shared by more than one URI is never
used, and a perturbation whose normalized form equals another concept's surface
is rejected — so every example has exactly one defensible answer.

Perturbations: `exact_lower`, `case_noise`, `plural`, `reorder_of` ("A B" → "B for
A"), `typo_swap`, `typo_drop`, `filler` ("the …", "measured in …"), `hyphen_space`,
`spelling` (meter/metre), `per_slash` ("per" → "/"), `drop_word`.

Composition: 60 class, 50 unit, 30 substance, 20 predicate, 20 process,
20 quantity_kind. Seed `20260917` — regenerating reproduces the same 200.

## Regenerate / run

```bash
python make_examples.py <graph_meta.json> <qudt_meta.json> examples.json
# on a capped container, against a warm data_dir that has embeddings:
VENV_DIR=~/acq_startup/venv python3 run_eval.py \
    --template ~/acq_startup/templates/tplE --label "main d894861" --out results_main.json
```

`run_eval.py` asks every example twice: once with the `kind` hint a caller may
know, once without. It records the full ranked answer, the match stage and the
latency, then stops the server.

## Results, 2026-09-17 (warm data_dir with embeddings, 4 vCPU / 16 GiB container)

Identical on all three refs, to the example — same index contents, same matcher
code. `ums-embedding-improvement` swaps the FP16 model export for FP32: on the 200
examples that moves scores by at most 0.00045 (median +0.00003), changes the top-1
URI on 1 of 400 queries (a tie-break between two already-wrong answers for the typo
"mllisecond": Milliliter vs Megaliter), and flips no answer's correctness.

| ref | pass | top-1 | top-3 | top-5 | no match | median | p95 |
|---|---|---|---|---|---|---|---|
| main `d894861` | with kind hint | 185/200 (92.5%) | 94.5% | 96.5% | 0 | 209 ms | 897 ms |
| main `d894861` | no kind hint | 187/200 (93.5%) | 95.5% | 97.0% | 0 | 491 ms | 598 ms |
| local-runtime-init `a9d4f65` | with kind hint | 185/200 (92.5%) | 94.5% | 96.5% | 0 | 252 ms | 895 ms |
| local-runtime-init `a9d4f65` | no kind hint | 187/200 (93.5%) | 95.5% | 97.0% | 0 | 450 ms | 602 ms |
| ums-embedding-improvement `84b1c7a` | with kind hint | 185/200 (92.5%) | 94.5% | 96.5% | 0 | 239 ms | 868 ms |
| ums-embedding-improvement `84b1c7a` | no kind hint | 187/200 (93.5%) | 95.5% | 97.0% | 0 | 495 ms | 601 ms |

By kind (top-1, with hint, identical on all three refs): substance 30/30, process 19/20, quantity_kind 19/20,
predicate 18/20, class 55/60, unit 44/50.

By perturbation (top-1, with hint, identical on all three refs): case_noise 26/26, hyphen_space 30/30,
plural 22/22, reorder_of 19/19, exact_lower 3/3, spelling 3/3, filler 30/31,
typo_drop 23/28, typo_swap 20/25, drop_word 7/9, **per_slash 2/4**.

All 15 misses are genuine: 10 are typos, 2 drop a word that carries the meaning
("kilonewton per square" → Kilonewton per Square Meter, not …Millimeter), 1 is a
filler case, and 2 are the unit-slash bug below.

## Finding: slash-composed unit text resolves to the wrong unit, as "exact"

`ConceptResolver` puts a deterministic `QUDTUnitConverter` tier ahead of the
embedding index; it returns at most one result at score 1.0 / `match_stage:
"exact"`, and the cascade stops there ("an exact hit from an earlier source
cannot be beaten"). When a slash form is not itself an indexed surface, that tier
answers with a *component* of the compound unit and wins:

| query | answer | correct answer |
|---|---|---|
| `kg/m3` | **Cubic Metre** (1.0, exact) | Kilogram per Cubic Metre |
| `decibel/meter` | **Metre** (1.0, exact) | Decibel per Metre |
| `kilogram/mega british thermal unit (international definition)` | **Mega British Thermal Unit …** (1.0, exact) | Kilogram per Mega BTU … |
| `mg/L` | Milligram per Litre (1.0, exact) | ✓ — `mg/L` is an indexed ucum surface |
| `dB/m` | Decibel per Metre (1.0, exact) | ✓ — indexed ucum surface |

So `mg/L` and `dB/m` work because QUDT ships them as `ucumCode` surfaces, not
because the slash is understood. `kg/m3` is the same shape and silently returns
the denominator, with full confidence and `matched_surface` echoing the input.
Reproduced on all three refs.


---

# Set v2 — 500 examples, generated without the matchers

`examples_v2.json` (sha256 `b46f8ea8147020df717868bcd5d3f94bfd7f20970df66a2875fa03c22f1c02b9`,
seed `5002026`) is an independent second set. `make_examples_v2.py` parses the four
bundled ontology TTLs with rdflib and nothing else — no embedding index, no
`EmbeddingMatcher`, no `ConceptResolver`, no server, no model. A concept's name comes
from its own `rdfs:label` / `skos:prefLabel` / `skos:altLabel` / `qudt:symbol` /
`qudt:ucumCode`; the expected answer is the subject that name belongs to. The file was
generated once and not edited afterwards.

Composition: 200 unit, 170 class, 90 quantity_kind, 40 predicate (per-kind quotas, since
QUDT alone could fill the set many times over). Sources: 422 rdfs:label, 39 ucumCode,
37 symbol, 2 altLabel. Graphs: qudt-unit 200, s223 139, qudt-qk 90, water 71.

`expected_kind` is null for water/s223 concepts: acquirium splits those into
class/substance/process by rules this generator deliberately does not consult, so a
wrong hint would fail them unfairly. Those 170 are asked without a hint in both passes,
which is why the two passes differ on only 330 of the 500.

## Results, 2026-09-17 (same warm templates, one capped container per ref)

| ref | pass | top-1 | top-3 | top-5 | no match | median |
|---|---|---|---|---|---|---|
| main `d894861` | no kind hint | **430/500 (86.0%)** | 92.6% | 93.4% | 0 | 493 ms |
| main `d894861` | with hint where known | 423/500 (84.6%) | 90.2% | 90.8% | 0 | 492 ms |
| local-runtime-init `a9d4f65` | no kind hint | **430/500 (86.0%)** | 92.6% | 93.4% | 0 | 491 ms |
| local-runtime-init `a9d4f65` | with hint where known | 423/500 (84.6%) | 90.4% | 91.0% | 0 | 481 ms |
| ums-embedding-improvement `84b1c7a` | no kind hint | **430/500 (86.0%)** | 92.6% | 93.4% | 0 | 496 ms |
| ums-embedding-improvement `84b1c7a` | with hint where known | 423/500 (84.6%) | 90.4% | 91.0% | 0 | 497 ms |

By ontology kind (no hint, same on all three): unit 174/200 (87%), class 145/170 (85%),
quantity_kind 79/90 (88%), predicate 32/40 (80%).

By perturbation (no hint): case_noise 81/81, exact_lower 13/13, spelling 7/7,
filler 80/81, plural 64/65, hyphen_space 63/64, reorder_of 32/34, per_slash 9/10,
drop_word 19/28, **typo_swap 32/59, typo_drop 30/58**.

### Why this set scores lower than the 200 (86% vs 93.5%)

Typos, and only typos. Set v1 always perturbed a concept's *longest* surface, so a typo
sat in a long, redundant phrase ("negatvie 12 vdc and positive 12 vdc electricity").
Set v2 picks a name at random, and many ontology labels are one short word, where a
single transposed letter destroys most of the signal — "Turbne", "Fnuction", "Radisoity".
Typo accuracy drops from 43/53 (81%) in v1 to 62/117 (53%) here. On every non-typo
category v2 matches v1: 368/383 (96.1%), against 144/147 (98.0%) in v1.

### The three refs stay equivalent

Top-1 differs on 12 of 3,000 query/ref combinations, and every one is a tie:
either two candidates separated by <0.0002 (FP16 vs FP32 rounding — "Propertys" 0.9089
vs 0.9090), or several compound units all returned at exactly 1.0 by the converter tier.
Net correctness is identical (430/500 each); `ums-embedding-improvement` wins
"EM connecion" and loses "Propertys".

### The converter-tier bug again, in a new shape

Truncated unit text gets a confident, arbitrary answer at score 1.0 / `exact`:

| query (a word dropped) | expected | main | lri | emb |
|---|---|---|---|---|
| `Square Metre per` | Square Metre per Metre | Coulomb Square Metre per Kilogram | Kilopascal Square Metre per Gram | Square Metre per Second Kelvin |
| `Percent per` | Percent per Bar | Percent per Decakelvin | Percent per Ten Thousand | Percent per Hectobar |
| `per Us Gallon` | Grain per Us Gallon | Pound Mass Hour per Us Gallon … | Pound Mass Minute per Us Gallon … | Pound Mass Square Inch per Us Gallon … |

Each returns `score 1.0, match_stage "exact"` for a unit that is not what was asked for,
and *which* wrong unit comes back is unstable across refs — the tier picks one of many
matches without a deterministic order. Same root cause as the `kg/m3` case above.


---

# ums-embedding-improvement-2 `441dc16` (2026-09-21)

Run with both frozen sets, unchanged, on the same warm-template protocol.

| set | pass | main | local-runtime-init | …-improvement | …-improvement-2 |
|---|---|---|---|---|---|
| v1 (200) | no kind hint | 93.5% | 93.5% | 93.5% | **92.0%** |
| v1 (200) | with hint | 92.5% | 92.5% | 92.5% | **91.0%** |
| v2 (500) | no kind hint | 86.0% | 86.0% | 86.0% | **77.0%** |
| v2 (500) | with hint | 84.6% | 84.6% | 84.6% | **77.4%** |

**The regression is entirely in unit symbols and UCUM codes**, which this branch
deliberately stops embedding (`950b70a` "Resolve unit expressions by canonical key;
stop embedding unit symbols", `4490da2` "Drop quantity-kind symbols"). The QUDT index
goes from 13,467 embedded surfaces to 5,340. On set v2, by the predicate the query text
came from:

| source of the text | …-improvement | …-improvement-2 |
|---|---|---|
| `rdfs:label` (422) | 354/422 | 353/422 |
| `qudt:symbol` (37) | 36/37 | **15/37** |
| `qudt:ucumCode` (39) | 38/39 | **15/39** |

Exact symbols still resolve by key (`mg/L`, `dB/m` → score 1.0 exact). What breaks is a
symbol that is *decorated or altered* and so misses the key: "the °api", "in [ft_i]",
"nvars", "the bar/k" → Bar instead of Bar per Kelvin. Previously the embedding index
caught those; now there is no fallback for them. Whether that matters depends on whether
callers send tidy symbols (it works) or human-typed ones (it does not).

**In exchange, confidently wrong answers are nearly gone.** Counting answers returned at
score 1.0 / `match_stage: "exact"` that are not the expected concept, with a kind hint:

| set | main | lri | emb | emb2 |
|---|---|---|---|---|
| v1 (200) | 2 | 2 | 2 | **0** |
| v2 (500) | 15 | 14 | 14 | **4** |

The `kg/m3` family is fixed as a *false-exact*: `decibel/meter` now returns Decibel per
Meter (0.947 semantic) instead of "Metre" at 1.0 exact, and `kg/m3` returns "Kilogram"
(0.815 semantic) — still not Kilogram per Cubic Metre, but no longer stated with
certainty. emb2's 4 remaining false-exacts are near-misses (`Gal{uk}/min` → Us Gallon per
Minute, `cm 3` → Cubic Centimetre).

Caveat: this branch also merges newer main (v0.4.0a8), so its deltas are not a pure
consequence of the embedding commits.

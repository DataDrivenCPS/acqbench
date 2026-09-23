"""Generate 500 resolution examples straight from the bundled ontology files.

Independence: this reads the shipped TTLs (water, s223, QUDT unit, QUDT
quantitykind) with rdflib and nothing else. No embedding index, no
EmbeddingMatcher, no ConceptResolver, no server — so what counts as the right
answer is fixed by the ontologies, not by anything acquirium's matching code
decided to index or by any model output.

A concept's name is taken from its own rdfs:label / skos:prefLabel /
skos:altLabel (and, for QUDT, qudt:symbol and qudt:ucumCode), then perturbed
the way a person plausibly would type it. The expected answer is the subject
that name belongs to.

  python make_examples_v2.py <ontology_dir> <out.json> [n]
"""
from __future__ import annotations

import hashlib
import json
import random
import re
import sys
from collections import defaultdict
from pathlib import Path

from rdflib import Graph, RDF, RDFS, OWL, URIRef
from rdflib.namespace import SKOS

sys.path.insert(0, str(Path(__file__).resolve().parent))
from make_examples import PERTURBATIONS, norm  # perturbation functions only

SEED = 5002026
N = 500
# per-kind caps, applied before sampling (see quota below)

QUDT = "http://qudt.org/schema/qudt/"
FILES = [
    ("water.ttl", "water"),
    ("s223.ttl", "s223"),
    ("qudt_unit.ttl", "qudt-unit"),
    ("qudt_qk.ttl", "qudt-qk"),
]
# kinds acquirium's API accepts; water/s223 concepts are left unkinded because
# acquirium splits them further (substance / process) by its own rules, which
# this generator deliberately does not consult.
KIND_BY_GRAPH = {"qudt-unit": "unit", "qudt-qk": "quantity_kind"}


def label_sources(g: Graph, subject) -> list[tuple[str, str]]:
    out = []
    for pred, tag in ((RDFS.label, "rdfs:label"), (SKOS.prefLabel, "skos:prefLabel"),
                      (SKOS.altLabel, "skos:altLabel"),
                      (URIRef(QUDT + "symbol"), "qudt:symbol"),
                      (URIRef(QUDT + "ucumCode"), "qudt:ucumCode")):
        for o in g.objects(subject, pred):
            lang = getattr(o, "language", None)
            if lang and not lang.startswith("en"):
                continue
            text = str(o).strip()
            if text:
                out.append((text, tag))
    return out


def main() -> None:
    onto_dir = Path(sys.argv[1])
    out = Path(sys.argv[2])
    n_target = int(sys.argv[3]) if len(sys.argv) > 3 else N

    concepts: dict[str, dict] = {}
    for fname, graph_tag in FILES:
        g = Graph()
        g.parse(onto_dir / fname, format="turtle")
        for subject in set(g.subjects()):
            if not isinstance(subject, URIRef):
                continue
            names = label_sources(g, subject)
            if not names:
                continue
            types = {str(t) for t in g.objects(subject, RDF.type)}
            if graph_tag in KIND_BY_GRAPH:
                kind = KIND_BY_GRAPH[graph_tag]
            elif types & {str(OWL.ObjectProperty), str(OWL.DatatypeProperty),
                          str(RDF.Property), str(OWL.AnnotationProperty)} \
                    or any(t.endswith(("#Relation", "#RelationWithInverse")) for t in types):
                kind = "predicate"
            elif types & {str(OWL.Class), str(RDFS.Class)} \
                    or any(t.endswith(("#Class", "#AbstractClass")) for t in types) \
                    or (subject, RDFS.subClassOf, None) in g:
                # water and s223 type their concepts with their own #Class, and
                # SHACL NodeShape; subClassOf is the reliable fallback
                kind = "class"
            else:
                continue
            c = concepts.setdefault(str(subject), {
                "uri": str(subject), "kind": kind, "graph": graph_tag, "names": []})
            c["names"].extend(names)

    # a name shared by more than one concept cannot identify one answer
    owners: dict[str, set[str]] = defaultdict(set)
    for c in concepts.values():
        for text, _ in c["names"]:
            owners[norm(text)].add(c["uri"])

    rng = random.Random(SEED)
    by_kind: dict[str, list[dict]] = defaultdict(list)
    for c in concepts.values():
        if any(len(owners[norm(t)]) == 1 and len(norm(t)) >= 3 for t, _ in c["names"]):
            by_kind[c["kind"]].append(c)
    for kind in by_kind:
        by_kind[kind].sort(key=lambda c: c["uri"])
        rng.shuffle(by_kind[kind])
    # QUDT alone could fill the set many times over; hold it to a share that
    # leaves the plant vocabulary properly represented
    quota = {"class": 170, "predicate": 40, "unit": 200, "quantity_kind": 90}
    pool = []
    for kind, want in quota.items():
        pool.extend(by_kind.get(kind, [])[:want])
    rng.shuffle(pool)

    examples, seen = [], set()
    for c in pool:
        if len(examples) >= n_target:
            break
        usable = [(t, src) for t, src in c["names"]
                  if len(owners[norm(t)]) == 1 and len(norm(t)) >= 3]
        base, source_pred = rng.choice(usable)
        order = PERTURBATIONS[:]
        rng.shuffle(order)
        if rng.random() > 1 / 6:
            order = [p for p in order if p[0] != "exact_lower"] + [PERTURBATIONS[0]]
        for name, fn in order:
            text = fn(base, rng, c["kind"])
            if not text or norm(text) in seen:
                continue
            other = owners.get(norm(text))
            if other and other != {c["uri"]}:
                continue
            seen.add(norm(text))
            examples.append({
                "id": f"v2-{len(examples)+1:03d}", "text": text, "perturbation": name,
                "expected_uri": c["uri"],
                "expected_kind": c["kind"] if c["graph"].startswith("qudt") or c["kind"] == "predicate" else None,
                "ontology_kind": c["kind"], "graph": c["graph"],
                "expected_label": base, "source_predicate": source_pred,
                "source_surface": base,
            })
            break

    payload = {
        "seed": SEED, "n": len(examples),
        "built_from": "bundled ontology TTLs parsed with rdflib only — no embedding index, "
                      "no matcher, no model, no server",
        "note": "expected_uri is the subject whose own label/symbol the text was derived from. "
                "expected_kind is null for water/s223 concepts because acquirium splits those "
                "into class/substance/process by rules this generator does not consult; those "
                "examples are asked without a kind hint.",
        "examples": examples,
    }
    body = json.dumps(payload, indent=1)
    out.write_text(body)
    from collections import Counter
    print(f"{len(examples)} examples -> {out}")
    print("sha256:", hashlib.sha256(body.encode()).hexdigest())
    print(Counter(e["ontology_kind"] for e in examples))
    print(Counter(e["graph"] for e in examples))
    print(Counter(e["perturbation"] for e in examples))
    print(Counter(e["source_predicate"] for e in examples))


if __name__ == "__main__":
    main()

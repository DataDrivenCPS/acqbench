"""Generate free-text resolution examples with ground-truth URIs.

The truth comes from the index itself: acquirium's embedding cache meta files
list every indexed (uri, kind, label, surface). We sample concepts, perturb one
of their surfaces the way a person plausibly would type it, and keep the source
URI as the expected answer. No model output is involved in deciding what is
correct, so the set stays valid for any ref or matching mode.

  python make_examples.py <graph_meta.json> <qudt_meta.json> [out.json] [n]
"""
from __future__ import annotations

import json
import random
import re
import sys
from collections import defaultdict
from pathlib import Path

SEED = 20260917
# how many examples per kind (sums to N)
QUOTA = {"class": 60, "substance": 30, "process": 20, "predicate": 20,
         "unit": 50, "quantity_kind": 20}

UK_US = [("meter", "metre"), ("liter", "litre"), ("gram", "gramme")]


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


# ---- perturbations: each returns None when it does not apply to the surface

def p_exact_lower(s, rng, kind=None):
    return s.lower()


def p_case_noise(s, rng, kind=None):
    return "".join(c.upper() if rng.random() < 0.5 else c.lower() for c in s)


def p_plural(s, rng, kind=None):
    words = s.split()
    last = words[-1]
    if not last.isalpha() or len(last) < 3 or last.endswith("s"):
        return None
    words[-1] = last + ("es" if last.endswith(("ch", "sh", "x", "z")) else "s")
    return " ".join(words)


def p_reorder_of(s, rng, kind=None):
    words = s.split()
    if len(words) < 2 or not all(w.isalpha() and len(w) >= 3 for w in words):
        return None
    joiner = rng.choice(["for", "of"])
    return f"{words[-1]} {joiner} {' '.join(words[:-1])}"


def p_typo_swap(s, rng, kind=None):
    words = s.split()
    idx = [i for i, w in enumerate(words) if len(w) >= 5 and w.isalpha()]
    if not idx:
        return None
    i = rng.choice(idx)
    w = words[i]
    j = rng.randrange(1, len(w) - 2)
    words[i] = w[:j] + w[j + 1] + w[j] + w[j + 2:]
    return " ".join(words)


def p_typo_drop(s, rng, kind=None):
    words = s.split()
    idx = [i for i, w in enumerate(words) if len(w) >= 6 and w.isalpha()]
    if not idx:
        return None
    i = rng.choice(idx)
    w = words[i]
    j = rng.randrange(1, len(w) - 1)
    words[i] = w[:j] + w[j + 1:]
    return " ".join(words)


def p_filler(s, rng, kind=None):
    opts = ["the {}", "our {}", "a {}"]
    if kind in ("unit", "quantity_kind"):
        opts += ["{} unit", "in {}", "measured in {}"]
    return rng.choice(opts).format(s.lower())


def p_hyphen_space(s, rng, kind=None):
    if "-" in s:
        return s.replace("-", " ")
    if " " in s:
        return s.replace(" ", "-")
    return None


def p_spelling(s, rng, kind=None):
    low = s.lower()
    for us, uk in UK_US:
        if us in low:
            return low.replace(us, uk)
        if uk in low:
            return low.replace(uk, us)
    return None


def p_per_slash(s, rng, kind=None):
    return s.lower().replace(" per ", "/") if " per " in s.lower() else None


def p_drop_word(s, rng, kind=None):
    words = s.split()
    if len(words) < 3:
        return None
    i = rng.randrange(len(words))
    return " ".join(words[:i] + words[i + 1:])


PERTURBATIONS = [
    ("exact_lower", p_exact_lower), ("case_noise", p_case_noise), ("plural", p_plural),
    ("reorder_of", p_reorder_of), ("typo_swap", p_typo_swap), ("typo_drop", p_typo_drop),
    ("filler", p_filler), ("hyphen_space", p_hyphen_space), ("spelling", p_spelling),
    ("per_slash", p_per_slash), ("drop_word", p_drop_word),
]


def main() -> None:
    graph_meta, qudt_meta = Path(sys.argv[1]), Path(sys.argv[2])
    out = Path(sys.argv[3]) if len(sys.argv) > 3 else Path(__file__).parent / "examples.json"
    rows = json.loads(graph_meta.read_text()) + json.loads(qudt_meta.read_text())

    # surface -> uris, to skip surfaces that cannot identify one concept
    owners: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        owners[norm(r["surface"])].add(r["uri"])

    concepts: dict[tuple[str, str], dict] = {}
    for r in rows:
        c = concepts.setdefault((r["uri"], r["kind"]),
                                {"uri": r["uri"], "kind": r["kind"], "label": r["label"],
                                 "surfaces": []})
        c["surfaces"].append(r["surface"])

    rng = random.Random(SEED)
    pool = defaultdict(list)
    for c in concepts.values():
        usable = [s for s in c["surfaces"] if len(owners[norm(s)]) == 1 and len(norm(s)) >= 3]
        if usable:
            pool[c["kind"]].append({**c, "usable": usable})
    for kind in pool:
        pool[kind].sort(key=lambda c: c["uri"])
        rng.shuffle(pool[kind])

    examples, seen_text = [], set()
    for kind, quota in QUOTA.items():
        picked, i = 0, 0
        candidates = pool.get(kind, [])
        while picked < quota and i < len(candidates):
            c = candidates[i]
            i += 1
            # longest surface first: the most descriptive name of the concept
            base = max(c["usable"], key=len)
            order = PERTURBATIONS[:]
            rng.shuffle(order)
            # keep one in six as a plain exact lookup, otherwise a real variant
            if rng.random() > 1 / 6:
                order = [p for p in order if p[0] != "exact_lower"] + [("exact_lower", p_exact_lower)]
            for name, fn in order:
                text = fn(base, rng, kind)
                if not text or norm(text) in seen_text:
                    continue
                # a perturbation that lands on another concept's surface is not a fair test
                other = owners.get(norm(text))
                if other and other != {c["uri"]}:
                    continue
                seen_text.add(norm(text))
                examples.append({
                    "id": f"{kind}-{picked+1:03d}", "text": text, "perturbation": name,
                    "expected_uri": c["uri"], "expected_kind": kind,
                    "expected_label": c["label"], "source_surface": base,
                })
                picked += 1
                break
    rng.shuffle(examples)
    out.write_text(json.dumps({
        "seed": SEED, "n": len(examples),
        "built_from": "acquirium embedding_cache meta (graph + qudt) of main d894861 / local-runtime-init a9d4f65 — identical in both",
        "note": "expected_uri is the concept whose indexed surface the text was derived from; "
                "texts whose normalized form is another concept's surface were rejected at generation time",
        "examples": examples,
    }, indent=1))
    from collections import Counter
    print(f"{len(examples)} examples -> {out}")
    print(Counter(e["expected_kind"] for e in examples))
    print(Counter(e["perturbation"] for e in examples))


if __name__ == "__main__":
    main()

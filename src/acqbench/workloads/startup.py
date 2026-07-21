"""Server startup cost.

Measured on a reference machine, a cold boot is ~380s and breaks down roughly
as: ontology parse ~27s, model download ~4s, graph embedding index ~48s, QUDT
embedding index ~286s. A warm boot over an existing data_dir is ~117s — the
index caches hit (sub-second) and what remains is the ontology parse, which
nothing caches.

So the two are genuinely different measurements and both are kept:

* `startup_cold` — first boot on a fresh deployment. Dominated by index
  construction, and the thing a change to embedding or ontology bundling moves.
* `startup_warm` — every restart after that. What an operator actually
  experiences, and the more commonly relevant number.

The runner has already waited for /health by the time run() is called, so the
wall-clock figure comes from the ServerHandle. What this adds is the breakdown:
which phase actually took the time.
"""

from __future__ import annotations

import re
from typing import Any

import httpx

from .base import Context, Workload, register

# Phase markers scraped from the server log. Each entry is (phase, compiled regex).
# These are best-effort: acquirium's log lines are not a stable API, so a miss
# yields a null phase rather than a failed run.
_PHASE_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("ontology_load", re.compile(r"(?:loaded|loading).*(?:ontolog|triples)", re.I)),
    ("embedding_start", re.compile(r"Embedding \d+ surfaces", re.I)),
    ("embedding_cache_saved", re.compile(r"Saved embedding cache", re.I)),
    ("embedding_built", re.compile(r"Embedding index built", re.I)),
    ("graph_store_open", re.compile(r"oxigraph|graph store", re.I)),
    ("uvicorn_ready", re.compile(r"uvicorn running on", re.I)),
]

#: "Saved embedding cache" only appears when an index was actually *built*. Its
#: absence is how a warm start proves the cache hit, so it is reported rather
#: than inferred from timing alone.
_CACHE_SAVED = re.compile(r"Saved embedding cache", re.I)

_TIMESTAMP = re.compile(r"^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[.,]?\d*)")


class _StartupBase(Workload):
    """Time-to-healthy, attributed to startup phases."""

    requires_cold_server = True
    expect_index_build: bool = True

    def run(self, ctx: Context) -> dict[str, Any]:
        metrics: dict[str, Any] = {
            "time_to_healthy_ms": ctx.server.startup_seconds * 1000.0,
        }

        log = _read_log(ctx)
        metrics["log_lines"] = len(log)
        metrics["phases"] = _phase_hits(log)

        built_index = any(_CACHE_SAVED.search(line) for line in log)
        metrics["built_embedding_index"] = built_index
        # A warm start that rebuilt its index is not a warm start, and a cold
        # start that skipped it was seeded from somewhere. Either way the number
        # would be attributed to the wrong workload, so fail rather than report.
        if built_index != self.expect_index_build:
            raise RuntimeError(
                f"{self.name}: expected built_embedding_index="
                f"{self.expect_index_build} but observed {built_index}; "
                "the data_dir was not in the state this workload assumes"
            )

        triples = _extract_triple_count(log)
        if triples is not None:
            metrics["ontology_triples"] = triples

        # These distinguish "fast because it did less" from "fast".
        metrics.update(_probe_state(ctx.base_url))
        metrics.update(_resource_snapshot(ctx))
        return metrics


@register("startup_cold")
class ColdStartupWorkload(_StartupBase):
    """First boot on a fresh deployment — pays the full embedding index build."""

    expect_index_build = True


@register("startup_warm")
class WarmStartupWorkload(_StartupBase):
    """Restart over an existing data_dir — index caches hit.

    The delta against startup_cold is what the embedding cache is worth.
    """

    expect_index_build = False


def _read_log(ctx: Context) -> list[str]:
    try:
        return ctx.server.log_path.read_text(errors="replace").splitlines()
    except OSError:
        return []


def _phase_hits(log: list[str]) -> dict[str, Any]:
    """First matching line per phase, with its offset from the first log line.

    Absolute timestamps are unreliable to compare across processes, so this
    reports offsets within a single server's own log.
    """
    t0 = None
    hits: dict[str, Any] = {}
    for line in log:
        ts = _parse_ts(line)
        if ts is not None and t0 is None:
            t0 = ts
        for phase, pat in _PHASE_PATTERNS:
            if phase in hits:
                continue
            if pat.search(line):
                offset = None
                if ts is not None and t0 is not None:
                    offset = round((ts - t0) * 1000.0, 2)
                hits[phase] = {"offset_ms": offset, "line": line.strip()[:200]}
    return hits


def _parse_ts(line: str) -> float | None:
    m = _TIMESTAMP.match(line)
    if not m:
        return None
    from datetime import datetime

    raw = m.group(1).replace(",", ".").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).timestamp()
        except ValueError:
            continue
    return None


def _extract_triple_count(log: list[str]) -> int | None:
    for line in log:
        m = re.search(r"([\d,_]{3,})\s+triples", line, re.I)
        if m:
            try:
                return int(m.group(1).replace(",", "").replace("_", ""))
            except ValueError:
                continue
    return None


def _probe_state(base_url: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for endpoint, key in (("/graph_version", "graph_version"), ("/embedding_status", "embedding_status")):
        try:
            r = httpx.get(f"{base_url}{endpoint}", timeout=30.0)
            if r.status_code == 200:
                out[key] = r.json()
        except (httpx.HTTPError, ValueError):
            out[key] = None
    return out


def _resource_snapshot(ctx: Context) -> dict[str, Any]:
    res = ctx.server.resources()
    return {f"post_startup_{k}": v for k, v in res.items()}

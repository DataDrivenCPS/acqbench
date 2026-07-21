"""Extract server-side span timings from acquirium's DEBUG log.

Acquirium instruments its hot paths with `timed_debug`, which brackets a block
with a pair of DEBUG lines::

    2026-07-15 19:33:01,950 DEBUG acquirium.storage → bulk_insert_polars prepare/dedupe rows=100
    2026-07-15 19:33:01,962 DEBUG acquirium.storage ← bulk_insert_polars prepare/dedupe rows=100 (12.3 ms)

The `←` lines are what we want: a named span and its elapsed time, measured
inside the server. Parsing them turns "the write got 20% slower" into "the
dedupe step got 3x slower and everything else held", which is the difference
between a number and a diagnosis.

Two things make this honest:

* **It is opt-in.** `timed_debug` skips its own cost when DEBUG is off, so
  enabling it perturbs what it measures. Runs recorded with profiling on are
  tagged as such and are never compared against runs recorded without it.
* **Spans are attributed by log byte-offset**, sliced around the timed region
  of a single workload run, so a shared server's other work never leaks in.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .metrics import summarize

#: Exit lines: "<ts> DEBUG <logger> ← <message> (<n> ms)"
_EXIT_RE = re.compile(
    r"^(?P<ts>\S+ \S+)\s+DEBUG\s+(?P<logger>\S+)\s+←\s+(?P<msg>.*?)\s+\((?P<ms>[\d.]+)\s*ms\)\s*$"
)

#: Interpolated arguments (row counts, ids, paths) would fragment a span into
#: thousands of unique names, so they are collapsed to placeholders. The order
#: matters: paths and UUIDs before bare numbers.
_NORMALIZERS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"urn:acquirium#[0-9a-f-]{36}"), "<ref>"),
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"), "<uuid>"),
    (re.compile(r"(/[^\s/]+){2,}"), "<path>"),
    (re.compile(r"\b\d[\d,_.]*\b"), "<n>"),
]


@dataclass(frozen=True)
class Span:
    name: str
    logger: str
    ms: float


def normalize(msg: str) -> str:
    """Collapse a span message to a stable name.

    'bulk_insert_polars prepare/dedupe rows=100' and '...rows=5000' are the
    same span and must aggregate together.
    """
    out = msg.strip()
    for pat, repl in _NORMALIZERS:
        out = pat.sub(repl, out)
    return out


def parse(text: str) -> list[Span]:
    spans: list[Span] = []
    for line in text.splitlines():
        m = _EXIT_RE.match(line)
        if not m:
            continue
        try:
            ms = float(m.group("ms"))
        except ValueError:
            continue
        spans.append(
            Span(name=normalize(m.group("msg")), logger=m.group("logger"), ms=ms)
        )
    return spans


def parse_slice(log_path: Path, start: int, end: int) -> list[Span]:
    """Parse only the bytes a single workload run produced.

    Offsets come from the log's size before and after the timed region, so on a
    server shared by several workloads each one sees only its own spans.
    """
    if end <= start:
        return []
    try:
        with log_path.open("rb") as f:
            f.seek(start)
            raw = f.read(end - start)
    except OSError:
        return []
    return parse(raw.decode("utf-8", errors="replace"))


def log_size(log_path: Path) -> int:
    try:
        return log_path.stat().st_size
    except OSError:
        return 0


def aggregate(spans: Iterable[Span], *, top: int = 40) -> dict[str, dict]:
    """Summarize spans by name, keeping the most expensive ones.

    Ranked by total time rather than call count: a span called once for 400ms
    matters more than one called 10,000 times for 0.01ms.
    """
    by_name: dict[str, list[float]] = defaultdict(list)
    loggers: dict[str, str] = {}
    for s in spans:
        by_name[s.name].append(s.ms)
        loggers.setdefault(s.name, s.logger)

    ranked = sorted(by_name.items(), key=lambda kv: sum(kv[1]), reverse=True)
    out: dict[str, dict] = {}
    for name, values in ranked[:top]:
        stats = summarize(values)
        out[name] = {
            "logger": loggers[name],
            "count": len(values),
            "total_ms": round(sum(values), 3),
            "mean_ms": round(stats["mean_ms"], 3),
            "median_ms": round(stats["median_ms"], 3),
            "p95_ms": round(stats["p95_ms"], 3),
            "max_ms": round(stats["max_ms"], 3),
        }
    return out


def total_ms(spans: Iterable[Span]) -> float:
    """Sum of all span time.

    Spans nest, so this double-counts and is not a wall-clock figure. It is
    useful only as a denominator for relative attribution.
    """
    return sum(s.ms for s in spans)

"""Result records and their JSONL store.

One JSON object per (cell, workload, repetition). Append-only: a run that
crashes halfway still leaves usable data, and reports are built by re-reading
the file rather than by holding state in memory.
"""

from __future__ import annotations

import json
import platform
import subprocess
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import psutil

SCHEMA_VERSION = 1


@dataclass
class Result:
    """A single measured run."""

    # identity
    cell_id: str
    ref_spec: str
    ref_version: str
    ref_resolved: str
    backend: str
    topology: str
    read_batch_size: int
    driver_count: int
    app_count: int

    workload: str
    repetition: int
    params: dict[str, Any] = field(default_factory=dict)

    # outcome
    ok: bool = True
    error: str | None = None

    # measurements
    metrics: dict[str, Any] = field(default_factory=dict)
    resources: dict[str, float] = field(default_factory=dict)

    #: Whether the server ran at DEBUG with span capture. Profiled runs carry
    #: logging overhead, so they must never be compared against unprofiled ones.
    profile: bool = False
    #: Server-side span timings scraped from the DEBUG log, keyed by span name.
    spans: dict[str, Any] = field(default_factory=dict)

    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    duration_seconds: float = 0.0
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


class ResultWriter:
    """Append-only JSONL sink."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = None

    def __enter__(self) -> "ResultWriter":
        self._fh = self.path.open("a")
        return self

    def __exit__(self, *exc) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None

    def write(self, result: Result) -> None:
        if not self._fh:
            raise RuntimeError("ResultWriter used outside its context manager")
        self._fh.write(result.to_json() + "\n")
        self._fh.flush()  # survive a crash mid-matrix


def read_results(path: Path) -> Iterator[dict]:
    if not path.exists():
        return
    with path.open() as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # A torn final line from a killed run shouldn't poison the report.
                print(f"warning: skipping malformed result at {path}:{line_no}")


def environment_manifest() -> dict[str, Any]:
    """Machine facts that make results comparable (or not) across hosts."""
    cpu_freq = None
    try:
        f = psutil.cpu_freq()
        cpu_freq = f.max if f else None
    except (OSError, NotImplementedError, AttributeError):
        pass

    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "cpu_count_logical": psutil.cpu_count(logical=True),
        "cpu_count_physical": psutil.cpu_count(logical=False),
        "cpu_freq_max_mhz": cpu_freq,
        "memory_total_gb": round(psutil.virtual_memory().total / 1e9, 2),
        "hostname": platform.node(),
        "docker": _docker_version(),
    }


def _docker_version() -> str | None:
    try:
        p = subprocess.run(
            ["docker", "--version"], capture_output=True, text=True, timeout=10
        )
        return p.stdout.strip() if p.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None

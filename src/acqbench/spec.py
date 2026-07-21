"""Benchmark matrix vocabulary: refs, configs, topologies, and their cross-product.

A *cell* is one (ref x config x topology) combination. Workloads run against a
cell and emit measurements. The matrix is the set of cells to visit.
"""

from __future__ import annotations

import hashlib
import itertools
import re
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from typing import Any, Iterator

GIT_URL = "git+https://github.com/DataDrivenCPS/acquirium.git"


class RefKind(str, Enum):
    PYPI = "pypi"
    GIT = "git"
    PATH = "path"


@dataclass(frozen=True)
class Ref:
    """A resolvable acquirium build.

    Spec syntax accepted by :meth:`parse`::

        pypi:0.3.1          a released version from PyPI
        pypi:latest         whatever PyPI currently resolves to
        git:main            a branch (or tag, or full SHA) from GitHub
        git:ums-ray-backend
        path:../acquirium   a local working tree, installed editable-off

    ``extras`` are acquirium extras (mqtt/xlsx/watertap) to install alongside.
    """

    kind: RefKind
    target: str
    extras: tuple[str, ...] = ()

    @classmethod
    def parse(cls, spec: str, extras: tuple[str, ...] = ()) -> "Ref":
        if ":" not in spec:
            raise ValueError(
                f"ref {spec!r} must be '<kind>:<target>', e.g. 'pypi:0.3.1' or 'git:main'"
            )
        kind_s, target = spec.split(":", 1)
        try:
            kind = RefKind(kind_s)
        except ValueError:
            raise ValueError(
                f"unknown ref kind {kind_s!r} in {spec!r}; expected one of "
                f"{', '.join(k.value for k in RefKind)}"
            ) from None
        if not target:
            raise ValueError(f"ref {spec!r} has an empty target")
        return cls(kind=kind, target=target, extras=tuple(extras))

    @property
    def spec(self) -> str:
        return f"{self.kind.value}:{self.target}"

    def install_spec(self, project_root: Path) -> str:
        """The argument to hand `uv pip install`."""
        suffix = f"[{','.join(self.extras)}]" if self.extras else ""
        if self.kind is RefKind.PYPI:
            if self.target == "latest":
                return f"acquirium{suffix}"
            return f"acquirium{suffix}=={self.target}"
        if self.kind is RefKind.GIT:
            # PEP 508 direct reference. The `acquirium[extras] @ git+...` form is
            # what lets extras ride along with a VCS install.
            base = f"{GIT_URL}@{self.target}"
            return f"acquirium{suffix} @ {base}" if suffix else base
        if self.kind is RefKind.PATH:
            p = Path(self.target)
            if not p.is_absolute():
                p = (project_root / p).resolve()
            if not (p / "pyproject.toml").exists():
                raise ValueError(f"path ref {self.target!r} has no pyproject.toml at {p}")
            return f"acquirium{suffix} @ {p.as_uri()}" if suffix else str(p)
        raise AssertionError(f"unhandled ref kind {self.kind}")

    @property
    def slug(self) -> str:
        """Filesystem-safe identifier, stable across runs."""
        base = f"{self.kind.value}-{_slugify(self.target)}"
        if self.extras:
            base += "-" + "-".join(sorted(self.extras))
        return base


class Backend(str, Enum):
    DUCKDB = "duckdb"
    TIMESCALE = "timescale"


class Topology(str, Enum):
    """Which acquirium components are live during a run.

    This is the marginal-cost axis: the delta between SERVER and SERVER_DRIVERS
    is what drivers cost you, and so on. Workload measurements are identical
    across topologies by design, so the difference is attributable.
    """

    SERVER = "server"
    SERVER_DRIVERS = "server+drivers"
    SERVER_APPS = "server+apps"
    SERVER_DRIVERS_APPS = "server+drivers+apps"

    @property
    def has_drivers(self) -> bool:
        return self in (Topology.SERVER_DRIVERS, Topology.SERVER_DRIVERS_APPS)

    @property
    def has_apps(self) -> bool:
        return self in (Topology.SERVER_APPS, Topology.SERVER_DRIVERS_APPS)


@dataclass(frozen=True)
class DriverSpec:
    """A [[drivers]] entry for the rendered acquirium.toml."""

    spec: str
    interval: float = 10.0
    options: dict[str, Any] = field(default_factory=dict)

    def to_toml_table(self) -> dict[str, Any]:
        return {"spec": self.spec, "interval": self.interval, **self.options}


# The stock driver every acquirium ships with; cheap, no external services.
SYSTEM_METRICS_DRIVER = DriverSpec(
    spec="acquirium.BuiltinDrivers.system_metrics:SystemMetricsDriver",
    interval=5.0,
)


@dataclass(frozen=True)
class ServerConfig:
    """Server knobs worth sweeping.

    Deliberately excludes `workers`: the acquirium CLI hard-refuses >1 because
    the embedded Oxigraph store is single-process, so it is not a real axis.
    """

    backend: Backend = Backend.DUCKDB
    read_batch_size: int = 50_000
    embedding_model: str | None = None
    ontology_sources: tuple[str, ...] = ()
    driver_count: int = 0
    driver_interval: float = 5.0
    app_count: int = 0
    #: When set, the rendered [[drivers]] are benchmark drivers ingesting this
    #: many rows per tick (across `bench_streams` streams) rather than the stock
    #: system-metrics driver. This is what the driver_tick workload observes.
    bench_rows_per_tick: int | None = None
    bench_streams: int = 4
    #: App-scalability mode: bind the server to 0.0.0.0 and export the app
    #: execution env so main's per-app worker containers can reach the host
    #: server (ums-ray ignores it). The app image for main's container path.
    app_scale: bool = False
    app_image: str = "acquirium_test-acquirium:latest"

    @property
    def slug(self) -> str:
        parts = [self.backend.value, f"rbs{self.read_batch_size}"]
        if self.embedding_model:
            parts.append(_slugify(self.embedding_model.split("/")[-1]))
        if self.bench_rows_per_tick is not None:
            parts.append(
                f"bench-d{self.driver_count}-r{self.bench_rows_per_tick}-p{int(self.driver_interval)}"
            )
        return "-".join(parts)

    def needs_postgres(self) -> bool:
        return self.backend is Backend.TIMESCALE


@dataclass(frozen=True)
class Cell:
    """One point in the matrix: a specific build, configured a specific way."""

    ref: Ref
    config: ServerConfig
    topology: Topology

    @property
    def slug(self) -> str:
        return f"{self.ref.slug}__{self.config.slug}__{_slugify(self.topology.value)}"

    @property
    def cell_id(self) -> str:
        """Short stable hash, for joining result rows without long paths."""
        return hashlib.sha256(self.slug.encode()).hexdigest()[:12]

    def effective_config(self) -> ServerConfig:
        """Config with driver/app counts forced to agree with the topology.

        A topology without drivers must have zero drivers regardless of what the
        matrix said, otherwise the marginal-cost comparison is meaningless.
        """
        cfg = self.config
        if not self.topology.has_drivers:
            cfg = replace(cfg, driver_count=0)
        elif cfg.driver_count == 0:
            cfg = replace(cfg, driver_count=1)
        if not self.topology.has_apps:
            cfg = replace(cfg, app_count=0)
        elif cfg.app_count == 0:
            cfg = replace(cfg, app_count=1)
        return cfg


@dataclass
class Matrix:
    """The full experiment definition."""

    refs: list[Ref]
    configs: list[ServerConfig]
    topologies: list[Topology]
    workloads: list[str]
    repetitions: int = 3
    warmup: int = 1
    #: Per-workload parameter overrides, e.g. {"write_arrow": {"streams": 500}}.
    #: Applied identically to every cell — a parameter that varied by ref would
    #: destroy the comparison the matrix exists to make.
    workload_params: dict[str, dict[str, Any]] = field(default_factory=dict)

    def cells(self) -> Iterator[Cell]:
        for ref, config, topology in itertools.product(self.refs, self.configs, self.topologies):
            yield Cell(ref=ref, config=config, topology=topology)

    def cell_count(self) -> int:
        return len(self.refs) * len(self.configs) * len(self.topologies)

    def run_count(self) -> int:
        return self.cell_count() * len(self.workloads) * self.repetitions


def _slugify(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()

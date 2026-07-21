"""Load a Matrix from a TOML file.

The matrix file is the experiment's record: it should be committable next to
the results it produced, so that a number can always be traced back to the
exact refs, configs and parameters that generated it.
"""

from __future__ import annotations

import itertools
import tomllib
from pathlib import Path
from typing import Any

from .spec import Backend, Matrix, Ref, ServerConfig, Topology


class MatrixError(ValueError):
    pass


def load(path: Path) -> Matrix:
    try:
        with path.open("rb") as f:
            doc = tomllib.load(f)
    except FileNotFoundError:
        raise MatrixError(f"no matrix file at {path}") from None
    except tomllib.TOMLDecodeError as e:
        raise MatrixError(f"{path}: invalid TOML: {e}") from None
    return from_dict(doc, source=str(path))


def from_dict(doc: dict[str, Any], *, source: str = "<dict>") -> Matrix:
    refs = _refs(doc, source)
    configs = _configs(doc, source)
    topologies = _topologies(doc, source)

    wl = doc.get("workloads")
    if not wl:
        raise MatrixError(f"{source}: 'workloads' is required and must be non-empty")
    if not isinstance(wl, list) or not all(isinstance(w, str) for w in wl):
        raise MatrixError(f"{source}: 'workloads' must be a list of strings")

    # Validate names now rather than failing three cells into a long run.
    from . import workloads as _w

    unknown = [w for w in wl if w not in _w.available()]
    if unknown:
        raise MatrixError(
            f"{source}: unknown workload(s) {', '.join(unknown)}; "
            f"available: {', '.join(_w.available())}"
        )

    run = doc.get("run", {})
    reps = int(run.get("repetitions", 3))
    warmup = int(run.get("warmup", 1))
    if reps < 1:
        raise MatrixError(f"{source}: run.repetitions must be >= 1")
    if warmup < 0:
        raise MatrixError(f"{source}: run.warmup must be >= 0")

    params = doc.get("workload_params", {})
    if not isinstance(params, dict):
        raise MatrixError(f"{source}: 'workload_params' must be a table")
    for k in params:
        if k not in _w.available():
            raise MatrixError(f"{source}: workload_params names unknown workload {k!r}")

    return Matrix(
        refs=refs,
        configs=configs,
        topologies=topologies,
        workloads=wl,
        repetitions=reps,
        warmup=warmup,
        workload_params=params,
    )


def _refs(doc: dict, source: str) -> list[Ref]:
    raw = doc.get("refs")
    if not raw:
        raise MatrixError(f"{source}: 'refs' is required and must be non-empty")
    out: list[Ref] = []
    for entry in raw:
        if isinstance(entry, str):
            out.append(Ref.parse(entry))
        elif isinstance(entry, dict):
            spec = entry.get("spec")
            if not spec:
                raise MatrixError(f"{source}: a refs entry is missing 'spec'")
            out.append(Ref.parse(spec, extras=tuple(entry.get("extras", ()))))
        else:
            raise MatrixError(f"{source}: refs entries must be strings or tables")
    if len({r.slug for r in out}) != len(out):
        raise MatrixError(f"{source}: duplicate refs would collide in results")
    return out


def _configs(doc: dict, source: str) -> list[ServerConfig]:
    """Configs come either as an explicit list, or as a sweep to expand."""
    if "configs" in doc and "sweep" in doc:
        raise MatrixError(f"{source}: use either 'configs' or 'sweep', not both")

    if "sweep" in doc:
        return _expand_sweep(doc["sweep"], source)

    raw = doc.get("configs")
    if not raw:
        return [ServerConfig()]
    return [_one_config(c, source) for c in raw]


def _one_config(c: dict, source: str) -> ServerConfig:
    try:
        backend = Backend(c.get("backend", "duckdb"))
    except ValueError:
        raise MatrixError(
            f"{source}: unknown backend {c.get('backend')!r}; "
            f"expected one of {', '.join(b.value for b in Backend)}"
        ) from None
    bench_rows = c.get("bench_rows_per_tick")
    return ServerConfig(
        backend=backend,
        read_batch_size=int(c.get("read_batch_size", 50_000)),
        embedding_model=c.get("embedding_model"),
        ontology_sources=tuple(c.get("ontology_sources", ())),
        driver_count=int(c.get("driver_count", 0)),
        driver_interval=float(c.get("driver_interval", 5.0)),
        app_count=int(c.get("app_count", 0)),
        bench_rows_per_tick=int(bench_rows) if bench_rows is not None else None,
        bench_streams=int(c.get("bench_streams", 4)),
        app_scale=bool(c.get("app_scale", False)),
        app_image=c.get("app_image", "acquirium_test-acquirium:latest"),
    )


def _expand_sweep(sweep: dict, source: str) -> list[ServerConfig]:
    """Cartesian product of the listed knobs.

    e.g. backend=["duckdb","timescale"], read_batch_size=[10000,50000]
    expands to four configs.
    """
    keys = list(sweep.keys())
    values = []
    for k in keys:
        v = sweep[k]
        if not isinstance(v, list) or not v:
            raise MatrixError(f"{source}: sweep.{k} must be a non-empty list")
        values.append(v)

    out: list[ServerConfig] = []
    for combo in itertools.product(*values):
        out.append(_one_config(dict(zip(keys, combo)), source))
    return out


def _topologies(doc: dict, source: str) -> list[Topology]:
    raw = doc.get("topologies", ["server"])
    out: list[Topology] = []
    for t in raw:
        try:
            out.append(Topology(t))
        except ValueError:
            raise MatrixError(
                f"{source}: unknown topology {t!r}; "
                f"expected one of {', '.join(x.value for x in Topology)}"
            ) from None
    # Reject up front rather than partway through a long run.
    unsupported = [t.value for t in out if t.has_apps]
    if unsupported:
        raise MatrixError(
            f"{source}: topology {', '.join(unsupported)} is not implemented yet "
            "(apps are started over the HTTP API, not via config). "
            "Use 'server' or 'server+drivers'."
        )
    return out

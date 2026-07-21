"""Render a Cell into an on-disk acquirium.toml + environment."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import tomli_w

from .spec import Backend, Cell, SYSTEM_METRICS_DRIVER, ServerConfig

# The benchmark driver ships with the harness; the server resolves this file
# path and (on the Ray backend) propagates it to worker PYTHONPATH.
BENCH_DRIVER = Path(__file__).resolve().parent / "bench_driver.py"


def bench_tick_dir(workdir: Path) -> Path:
    """Where the bench drivers write their per-tick timing files."""
    return workdir / "driver_ticks"


def _bench_drivers(cfg: ServerConfig, workdir: Path) -> list[dict[str, Any]]:
    """One [[drivers]] entry per benchmark driver, each with a distinct
    source_id / output file / time base so they never collide."""
    out_dir = bench_tick_dir(workdir)
    # Clear stale tick files: drivers append, so a re-boot of the same cell
    # (resume, or a second rep) would otherwise mix a prior boot's ticks — a
    # prior seq-0 in particular corrupts the driver-online-spread measurement.
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    spec = f"{BENCH_DRIVER}:BenchDriver"
    entries = []
    for i in range(cfg.driver_count):
        name = f"bench_{i}"
        entries.append(
            {
                "spec": spec,
                # Unique registry name per driver — the server keys drivers by
                # `name` (defaulting to the class name), so without this every
                # entry collides on "BenchDriver" and only one runs.
                "name": name,
                "interval": cfg.driver_interval,
                "source_id": name,
                "driver_id": name,
                "rows_per_tick": cfg.bench_rows_per_tick,
                "streams": cfg.bench_streams,
                "out_dir": str(out_dir),
                "base_epoch_s": i,  # a distinct day per driver → no shared timestamps
            }
        )
    return entries


def render_config(
    cell: Cell,
    workdir: Path,
    *,
    port: int,
    pg_dsn: str | None = None,
    recreate: bool = True,
    data_dir: Path | None = None,
) -> Path:
    """Write the acquirium.toml for this cell and return its path.

    `data_dir` defaults to workdir/data. It is separable because the embedding
    cache lives *inside* data_dir, and `recreate = true` wipes it — costing a
    full index rebuild (minutes) on every boot. The runner therefore seeds a
    pre-warmed data_dir and boots with recreate=false instead.
    """
    cfg: ServerConfig = cell.effective_config()

    # Apps are started through the HTTP API (/apps/register + /apps/run), not
    # via acquirium.toml, and their execution backend differs across the
    # versions under test (Ray actors vs containers). Until that is implemented
    # and verified, an apps topology must fail here: rendering it would produce
    # a config identical to `server` and quietly file duplicate measurements
    # under a label claiming apps were running.
    if cell.topology.has_apps:
        raise NotImplementedError(
            f"topology {cell.topology.value!r} is not implemented yet — apps must be "
            "started via /apps/register + /apps/run and verified through /apps/list. "
            "Use 'server' or 'server+drivers'."
        )

    data_dir = data_dir or (workdir / "data")
    data_dir.mkdir(parents=True, exist_ok=True)

    server: dict[str, Any] = {
        "host": "127.0.0.1",
        "port": port,
        "data_dir": str(data_dir),
        "timeseries_backend": cfg.backend.value,
        "recreate": recreate,
        "read_batch_size": cfg.read_batch_size,
    }

    if cfg.backend is Backend.DUCKDB:
        server["duckdb_path"] = str(data_dir / "timeseries.duckdb")
    else:
        if not pg_dsn:
            raise ValueError(
                f"{cell.slug}: timescale backend needs a pg_dsn "
                "(start one with `acqbench services up`)"
            )
        server["pg_dsn"] = pg_dsn

    if cfg.embedding_model:
        server["embedding_model"] = cfg.embedding_model

    doc: dict[str, Any] = {"server": server}

    if cfg.ontology_sources:
        doc["ontologies"] = {"sources": list(cfg.ontology_sources)}

    doc["driver"] = {
        "server_url": "127.0.0.1",
        "server_port": port,
        "use_ssl": False,
        "interval": cfg.driver_interval,
    }

    # Topology drives this: a `server`-only cell renders zero [[drivers]].
    if cfg.driver_count > 0:
        if cfg.bench_rows_per_tick is not None:
            doc["drivers"] = _bench_drivers(cfg, workdir)
        else:
            doc["drivers"] = [
                {**SYSTEM_METRICS_DRIVER.to_toml_table(), "interval": cfg.driver_interval}
                for _ in range(cfg.driver_count)
            ]

    path = workdir / "acquirium.toml"
    with path.open("wb") as f:
        tomli_w.dump(doc, f)
    return path


def render_env(
    cell: Cell,
    workdir: Path,
    *,
    fastembed_cache: Path | None = None,
    port: int | None = None,
) -> dict[str, str]:
    """Extra environment for the server process.

    Note that FASTEMBED_CACHE_PATH only covers the downloaded *model*, not the
    computed index: the manager passes model_cache_dir to TextEmbedding
    explicitly, and the index cache lives under data_dir. Keeping index cost out
    of a run is the template data_dir's job, not this one's.
    """
    env: dict[str, str] = {}
    if fastembed_cache:
        fastembed_cache.mkdir(parents=True, exist_ok=True)
        env["FASTEMBED_CACHE_PATH"] = str(fastembed_cache)
    # Keep ontology resolution off the network and shared between runs.
    env["ACQUIRIUM_ONTOENV_ROOT"] = str(workdir / "ontoenv")

    cfg = cell.effective_config()
    if cfg.app_scale and port is not None:
        # main runs each app as a worker container that must reach the host
        # server (via host.docker.internal) and read the shipped app source
        # from the bind-mounted data_dir. ums-ray (Ray actors) ignores all of
        # this, so it is safe to set unconditionally.
        env["ACQUIRIUM_DEFAULT_APP_IMAGE"] = cfg.app_image
        env["ACQUIRIUM_APP_SERVER_URL"] = "host.docker.internal"
        env["ACQUIRIUM_APP_SERVER_PORT"] = str(port)
        env["ACQUIRIUM_APP_VOLUME"] = str((workdir / "data").resolve())
    return env

"""Matrix execution.

Two costs shape this:

**Startup is brutally expensive.** A true cold boot measures ~380s, almost all
of it building embedding indexes (~48s graph, ~286s QUDT). Because that cache
lives inside data_dir, `recreate = true` throws it away every time. So cells
boot from a pre-warmed template (see template.py) with recreate=false, paying
only the ~117s that isn't cacheable. Only the `startup_cold` workload boots
genuinely cold, because there the cost *is* the measurement.

**Servers are therefore shared.** Workloads are grouped by whether they need
their own server:

* `requires_cold_server` workloads get one per repetition.
* Everything else shares one server per cell. State accumulates across
  repetitions within a cell; workloads keep that honest by using a distinct
  source_id per repetition, so no repetition reads or overwrites another's
  rows. Table growth is still visible to later repetitions, which is what
  `--fresh-per-rep` is for when that matters more than wall-clock.
"""

from __future__ import annotations

import shutil
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console

from . import logparse, services, workloads
from .logparse import log_size
from .provision import Install, provision
from .render import render_config, render_env
from .results import Result, ResultWriter
from .server import ServerHandle, free_port, running_server
from .spec import Cell, Matrix
from .template import TemplateCache, seed_from_template
from .workloads.base import Context


@dataclass
class RunOptions:
    workdir: Path
    venv_root: Path
    project_root: Path
    results_path: Path
    fastembed_cache: Path
    template_root: Path
    fresh_per_rep: bool = False
    # A genuine cold boot has been measured at ~380s, so the default has to
    # clear that with room to spare or the first template build times out.
    startup_timeout: float = 900.0
    keep_going: bool = True
    verbose_server: bool = False
    force_provision: bool = False
    #: Run servers at DEBUG and capture per-span timings from their logs.
    #: Off by default: acquirium's timed_debug skips its own cost unless DEBUG
    #: is on, so profiling perturbs the very thing being measured.
    profile: bool = False


class Runner:
    def __init__(self, matrix: Matrix, opts: RunOptions, console: Console | None = None):
        self.matrix = matrix
        self.opts = opts
        self.console = console or Console()
        self._pg_dsn: str | None = None
        self.templates = TemplateCache(
            opts.template_root, opts.fastembed_cache, console=self.console
        )

    # -- lifecycle --------------------------------------------------------

    def _ensure_postgres(self) -> str:
        if self._pg_dsn is None:
            self.console.print("[cyan]starting postgres for the timescale backend[/]")
            pg = services.up()
            self._pg_dsn = pg.dsn
        return self._pg_dsn

    def _needs_postgres(self) -> bool:
        return any(c.config.needs_postgres() for c in self.matrix.cells())

    def run(self) -> int:
        """Execute the whole matrix. Returns the number of failed runs."""
        cells = list(self.matrix.cells())
        self.console.print(
            f"[bold]matrix[/]: {len(cells)} cells x {len(self.matrix.workloads)} workloads "
            f"x {self.matrix.repetitions} reps = {self.matrix.run_count()} runs"
        )

        if self._needs_postgres():
            self._ensure_postgres()

        failures = 0
        with ResultWriter(self.opts.results_path) as writer:
            for i, cell in enumerate(cells, 1):
                self.console.rule(f"[bold cyan]cell {i}/{len(cells)}[/] {cell.slug}")
                try:
                    failures += self._run_cell(cell, writer)
                except Exception as e:
                    failures += 1
                    # Print the whole error: a truncated install or startup
                    # failure is near-impossible to diagnose after the fact,
                    # and a skipped cell silently shrinks the comparison.
                    self.console.print(f"[red]cell failed[/] ({cell.slug}):\n{e}")
                    if not self.opts.keep_going:
                        raise
        return failures

    def _run_cell(self, cell: Cell, writer: ResultWriter) -> int:
        install = self._provision(cell)
        self.console.print(f"  [dim]{cell.ref.spec} -> {install.resolved}[/]")

        cold_names = [w for w in self.matrix.workloads if _is_cold(w)]
        warm_names = [w for w in self.matrix.workloads if not _is_cold(w)]

        failures = 0
        # Cold workloads: a fresh server per repetition, since startup is the
        # thing being measured.
        for name in cold_names:
            for rep in range(self.matrix.repetitions):
                failures += self._run_isolated(cell, install, name, rep, writer)

        if warm_names:
            if self.opts.fresh_per_rep:
                for rep in range(self.matrix.repetitions):
                    for name in warm_names:
                        failures += self._run_isolated(cell, install, name, rep, writer)
            else:
                failures += self._run_shared(cell, install, warm_names, writer)
        return failures

    def _provision(self, cell: Cell) -> Install:
        return provision(
            cell.ref,
            self.opts.venv_root,
            self.opts.project_root,
            force=self.opts.force_provision,
        )

    def _cell_dir(self, cell: Cell, suffix: str = "") -> Path:
        d = self.opts.workdir / cell.slug / suffix if suffix else self.opts.workdir / cell.slug
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _server_for(
        self, cell: Cell, install: Install, workdir: Path, *, cold: bool = False
    ) -> Any:
        """Start a server for this cell.

        `cold` boots from an empty data_dir with recreate=true, paying the full
        embedding build. Otherwise the cell is seeded from a pre-warmed template
        and boots with recreate=false, which skips minutes of index construction
        that has nothing to do with what is being measured.
        """
        port = free_port()
        pg_dsn = None
        if cell.config.needs_postgres():
            pg_dsn = self._ensure_postgres()
            services.reset_database()

        data_dir = workdir / "data"
        if cold:
            if data_dir.exists():
                shutil.rmtree(data_dir)
        else:
            template = self.templates.get(
                cell, install, startup_timeout=self.opts.startup_timeout
            )
            seed_from_template(template, data_dir)

        config_path = render_config(
            cell,
            workdir,
            port=port,
            pg_dsn=pg_dsn,
            recreate=cold,
            data_dir=data_dir,
        )
        env = render_env(
            cell, workdir, fastembed_cache=self.opts.fastembed_cache, port=port
        )
        # App-scale runs need the server reachable from worker containers, which
        # requires binding all interfaces rather than loopback only.
        bind_host = "0.0.0.0" if cell.effective_config().app_scale else "127.0.0.1"
        return running_server(
            install,
            config_path,
            workdir,
            port=port,
            env_extra=env,
            startup_timeout=self.opts.startup_timeout,
            # Profiling needs DEBUG logs, which is what -v turns on.
            verbose=self.opts.verbose_server or self.opts.profile,
            bind_host=bind_host,
        )

    # -- execution --------------------------------------------------------

    def _run_isolated(
        self, cell: Cell, install: Install, name: str, rep: int, writer: ResultWriter
    ) -> int:
        """One workload, one repetition, on a server of its own."""
        wd = self._cell_dir(cell, f"{name}-rep{rep}")

        # startup_cold is the only workload that pays the full embedding build;
        # startup_warm restarts over a template-seeded data_dir, which is what a
        # real restart looks like.
        if name == "startup_cold":
            try:
                with self._server_for(cell, install, wd, cold=True) as srv:
                    return self._execute(cell, install, srv, name, rep, wd, writer)
            except Exception as e:
                return self._fail(cell, install, name, rep, writer, e)

        # startup_warm needs no special casing: the default path seeds a
        # template — itself a data_dir a real server left behind, with its
        # indexes built — and boots with recreate=false. That *is* a warm start.
        # The workload asserts the index caches actually hit, so a broken
        # template surfaces as a failure rather than a wrong number.
        try:
            with self._server_for(cell, install, wd) as srv:
                return self._execute(cell, install, srv, name, rep, wd, writer)
        except Exception as e:
            return self._fail(cell, install, name, rep, writer, e)

    def _run_shared(
        self, cell: Cell, install: Install, names: list[str], writer: ResultWriter
    ) -> int:
        """All warm workloads x all repetitions against one server."""
        wd = self._cell_dir(cell, "warm")
        failures = 0
        try:
            with self._server_for(cell, install, wd) as srv:
                self.console.print(
                    f"  [dim]server up in {srv.startup_seconds:.1f}s on {srv.base_url}[/]"
                )
                for rep in range(self.matrix.repetitions):
                    for name in names:
                        if not srv.is_alive():
                            raise RuntimeError(
                                f"server died mid-cell; log tail:\n{srv.tail()}"
                            )
                        failures += self._execute(cell, install, srv, name, rep, wd, writer)
        except Exception as e:
            if not self.opts.keep_going:
                raise
            self.console.print(f"[red]  shared server run failed:[/] {e}")
            failures += 1
        return failures

    def _execute(
        self,
        cell: Cell,
        install: Install,
        srv: ServerHandle,
        name: str,
        rep: int,
        wd: Path,
        writer: ResultWriter,
    ) -> int:
        params = _params_for(name, self.matrix)
        wl = workloads.get(name)(**params)
        ctx = Context(
            cell=cell, server=srv, workdir=wd, repetition=rep, params=params, install=install
        )

        started = time.perf_counter()
        spans: dict[str, dict] = {}
        try:
            wl.setup(ctx)
            try:
                # Warmup runs are discarded: the first call into a path pays for
                # connection setup, lazy imports and cache population.
                for _ in range(self.matrix.warmup if not _is_cold(name) else 0):
                    wl.run(ctx)
                # Slice the log around only the measured run, so a shared
                # server's other workloads never leak into these spans.
                mark = log_size(srv.log_path) if self.opts.profile else 0
                metrics = wl.run(ctx)
                if self.opts.profile:
                    spans = logparse.aggregate(
                        logparse.parse_slice(srv.log_path, mark, log_size(srv.log_path))
                    )
            finally:
                wl.teardown(ctx)
        except Exception as e:
            self.console.print(f"  [red]FAIL[/] {name} rep{rep}: {e}")
            if not self.opts.keep_going:
                raise
            return self._fail(cell, install, name, rep, writer, e, wl.describe())

        duration = time.perf_counter() - started
        writer.write(
            _result(
                cell, install, name, rep, wl.describe(), ok=True, metrics=metrics,
                resources=srv.resources(), duration=duration,
                profile=self.opts.profile, spans=spans,
            )
        )
        self.console.print(f"  [green]ok[/] {name} rep{rep} [dim]{_headline(metrics)}[/]")
        return 0

    def _fail(
        self,
        cell: Cell,
        install: Install,
        name: str,
        rep: int,
        writer: ResultWriter,
        exc: Exception,
        params: dict | None = None,
    ) -> int:
        writer.write(
            _result(
                cell, install, name, rep, params or {}, ok=False,
                error=f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=3)}",
            )
        )
        return 1


def _is_cold(name: str) -> bool:
    try:
        return bool(getattr(workloads.get(name), "requires_cold_server", False))
    except KeyError:
        return False


def _params_for(name: str, matrix: Matrix) -> dict[str, Any]:
    return dict(getattr(matrix, "workload_params", {}).get(name, {}))


def _result(
    cell: Cell,
    install: Install,
    workload: str,
    rep: int,
    params: dict,
    *,
    ok: bool,
    metrics: dict | None = None,
    resources: dict | None = None,
    error: str | None = None,
    duration: float = 0.0,
    profile: bool = False,
    spans: dict | None = None,
) -> Result:
    cfg = cell.effective_config()
    return Result(
        profile=profile,
        spans=spans or {},
        cell_id=cell.cell_id,
        ref_spec=cell.ref.spec,
        ref_version=install.version,
        ref_resolved=install.resolved,
        backend=cfg.backend.value,
        topology=cell.topology.value,
        read_batch_size=cfg.read_batch_size,
        driver_count=cfg.driver_count,
        app_count=cfg.app_count,
        workload=workload,
        repetition=rep,
        params=params,
        ok=ok,
        error=error,
        metrics=metrics or {},
        resources=resources or {},
        duration_seconds=duration,
    )


def _headline(metrics: dict) -> str:
    """One-line gist for the console."""
    if "rows_per_second" in metrics:
        lat = metrics.get("latency", {})
        return f"{metrics['rows_per_second']:,.0f} rows/s  p50={lat.get('median_ms', 0):.1f}ms"
    if "queries_per_second" in metrics:
        lat = metrics.get("latency", {})
        return f"{metrics['queries_per_second']:,.1f} q/s  p50={lat.get('median_ms', 0):.1f}ms"
    if "time_to_healthy_ms" in metrics:
        return f"healthy in {metrics['time_to_healthy_ms'] / 1000:.1f}s"
    return ""

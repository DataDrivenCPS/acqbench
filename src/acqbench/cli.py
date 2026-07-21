"""acqbench command line."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from . import services, workloads
from .matrixfile import MatrixError, load as load_matrix
from .provision import ProvisionError, provision, which_uv
from .report import (
    app_scale_ceilings,
    app_scale_points,
    compare as do_compare,
    compare_driver_grid,
    compare_queries,
    compare_spans,
    dead_queries,
    driver_grid,
    marginal_cost,
    query_row_mismatches,
    query_table,
    span_table,
    summary_rows,
)
from .results import environment_manifest
from .runner import Runner, RunOptions
from .spec import Ref

app = typer.Typer(
    add_completion=False,
    help="Benchmark acquirium across versions, configs, and topologies.",
    no_args_is_help=True,
)
console = Console()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WORK = PROJECT_ROOT / "runs"
DEFAULT_VENVS = PROJECT_ROOT / "venvs"
DEFAULT_RESULTS = PROJECT_ROOT / "results" / "results.jsonl"
DEFAULT_CACHE = PROJECT_ROOT / ".cache" / "fastembed"
DEFAULT_TEMPLATES = PROJECT_ROOT / ".cache" / "templates"


@app.command()
def run(
    matrix: Path = typer.Argument(..., help="Path to a matrix TOML file"),
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o", help="JSONL output path"),
    workdir: Path = typer.Option(DEFAULT_WORK, "--workdir", help="Scratch space for servers"),
    venvs: Path = typer.Option(DEFAULT_VENVS, "--venvs", help="Where per-ref venvs live"),
    fresh_per_rep: bool = typer.Option(
        False, "--fresh-per-rep", help="Restart the server between repetitions (slow, less drift)"
    ),
    force_provision: bool = typer.Option(
        False, "--force-provision", help="Rebuild venvs even if cached"
    ),
    startup_timeout: float = typer.Option(
        900.0, "--startup-timeout", help="Seconds to wait for /health (a cold boot is ~380s)"
    ),
    fail_fast: bool = typer.Option(False, "--fail-fast", help="Stop at the first failure"),
    verbose_server: bool = typer.Option(False, "-v", "--verbose-server", help="Run servers with -v"),
    profile: bool = typer.Option(
        False,
        "--profile",
        help="Run servers at DEBUG and capture per-span timings (slower; see `acqbench spans`)",
    ),
) -> None:
    """Execute a matrix and append results to JSONL."""
    try:
        which_uv()
        m = load_matrix(matrix)
    except (MatrixError, ProvisionError) as e:
        console.print(f"[red]error:[/] {e}")
        raise typer.Exit(2)

    console.print(f"[bold]refs[/]: {', '.join(r.spec for r in m.refs)}")
    console.print(f"[bold]workloads[/]: {', '.join(m.workloads)}")
    if profile:
        console.print(
            "[yellow]profiling on:[/] servers run at DEBUG and span timings are captured. "
            "This adds logging overhead, so these results are tagged `profile` and are "
            "only comparable to other profiled runs."
        )

    results.parent.mkdir(parents=True, exist_ok=True)
    manifest = results.parent / "environment.json"
    manifest.write_text(json.dumps(environment_manifest(), indent=2))
    console.print(f"[dim]environment manifest -> {manifest}[/]")

    opts = RunOptions(
        workdir=workdir,
        venv_root=venvs,
        project_root=PROJECT_ROOT,
        results_path=results,
        fastembed_cache=DEFAULT_CACHE,
        template_root=DEFAULT_TEMPLATES,
        fresh_per_rep=fresh_per_rep,
        startup_timeout=startup_timeout,
        keep_going=not fail_fast,
        verbose_server=verbose_server,
        force_provision=force_provision,
        profile=profile,
    )
    failures = Runner(m, opts, console).run()

    console.print()
    if failures:
        console.print(f"[yellow]completed with {failures} failed run(s)[/] -> {results}")
        raise typer.Exit(1)
    console.print(f"[green]all runs ok[/] -> {results}")


@app.command("provision")
def provision_cmd(
    refs: list[str] = typer.Argument(..., help="Ref specs, e.g. pypi:0.3.1 git:main"),
    venvs: Path = typer.Option(DEFAULT_VENVS, "--venvs"),
    force: bool = typer.Option(False, "--force", help="Rebuild even if cached"),
) -> None:
    """Install acquirium at each ref into its own venv, without benchmarking."""
    table = Table("ref", "version", "resolved", "venv")
    for spec in refs:
        try:
            r = Ref.parse(spec)
            inst = provision(r, venvs, PROJECT_ROOT, force=force)
        except (ValueError, ProvisionError) as e:
            console.print(f"[red]{spec}:[/] {e}")
            raise typer.Exit(1)
        table.add_row(r.spec, inst.version, inst.resolved, str(inst.venv))
    console.print(table)


@app.command("workloads")
def workloads_cmd() -> None:
    """List available workloads."""
    table = Table("workload", "cold server", "description")
    for name in workloads.available():
        cls = workloads.get(name)
        doc = (cls.__doc__ or "").strip().splitlines()
        table.add_row(
            name,
            "yes" if getattr(cls, "requires_cold_server", False) else "",
            doc[0] if doc else "",
        )
    console.print(table)


@app.command("plan")
def plan(matrix: Path = typer.Argument(...)) -> None:
    """Show what a matrix would run, without running it."""
    try:
        m = load_matrix(matrix)
    except MatrixError as e:
        console.print(f"[red]error:[/] {e}")
        raise typer.Exit(2)

    table = Table("#", "ref", "backend", "topology", "read_batch_size")
    for i, cell in enumerate(m.cells(), 1):
        cfg = cell.effective_config()
        table.add_row(
            str(i), cell.ref.spec, cfg.backend.value, cell.topology.value, str(cfg.read_batch_size)
        )
    console.print(table)
    console.print(
        f"\n[bold]{m.cell_count()}[/] cells x [bold]{len(m.workloads)}[/] workloads "
        f"x [bold]{m.repetitions}[/] reps = [bold]{m.run_count()}[/] runs"
    )
    if any(c.config.needs_postgres() for c in m.cells()):
        console.print("[yellow]note:[/] this matrix needs Docker (timescale backend)")


@app.command("summary")
def summary(
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o"),
) -> None:
    """Aggregate results into a table."""
    rows = summary_rows(results)
    if not rows:
        console.print(f"[yellow]no results in {results}[/]")
        raise typer.Exit(1)

    table = Table("workload", "backend", "topology", "ref", "metric", "median", "n", "spread", "fail")
    for r in rows:
        spread = f"{r['spread_pct'] * 100:.1f}%"
        table.add_row(
            r["workload"], r["backend"], r["topology"], r["ref"], r["metric"],
            f"{r['median']:,.2f}", str(r["n"]),
            f"[yellow]{spread}[/]" if r["spread_pct"] > 0.1 else spread,
            f"[red]{r['failures']}[/]" if r["failures"] else "",
        )
    console.print(table)


@app.command("compare")
def compare_cmd(
    baseline: str = typer.Argument(..., help="Baseline ref spec, e.g. pypi:0.3.1"),
    candidates: Optional[list[str]] = typer.Argument(None, help="Refs to compare (default: all others)"),
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o"),
) -> None:
    """Compare refs against a baseline and flag regressions."""
    try:
        comparisons = do_compare(results, baseline, candidates or None)
    except ValueError as e:
        console.print(f"[red]error:[/] {e}")
        raise typer.Exit(2)

    if not comparisons:
        console.print("[yellow]nothing comparable — refs share no (workload, config) cells[/]")
        raise typer.Exit(1)

    table = Table("workload", "backend", "topology", "candidate", "metric", baseline, "candidate", "change", "verdict")
    regressions = 0
    for c in comparisons:
        colour = {"faster": "green", "SLOWER": "red", "same": "dim", "noisy": "yellow"}[c.verdict]
        if c.verdict == "SLOWER":
            regressions += 1
        table.add_row(
            c.key.workload, c.key.backend, c.key.topology, c.candidate_ref, c.metric,
            f"{c.baseline:,.2f}", f"{c.candidate:,.2f}",
            f"{c.improvement * 100:+.1f}%",
            f"[{colour}]{c.verdict}[/]",
        )
    console.print(table)
    console.print("\n[dim]change is polarity-adjusted: positive is always better. "
                  "'noisy' means repetitions disagreed too much to call.[/]")
    if regressions:
        console.print(f"[red]{regressions} regression(s)[/]")
        raise typer.Exit(1)


@app.command("marginal")
def marginal(
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o"),
) -> None:
    """Show what each topology step costs relative to server-only."""
    rows = marginal_cost(results)
    if not rows:
        console.print("[yellow]no comparable topologies — run a matrix with more than one[/]")
        raise typer.Exit(1)

    table = Table("workload", "backend", "ref", "topology", "metric", "server", "with", "change")
    for r in rows:
        colour = "red" if r["rel_change"] < -0.05 else "dim"
        table.add_row(
            r["workload"], r["backend"], r["ref"], r["topology"], r["metric"],
            f"{r['server_only']:,.2f}", f"{r['with_components']:,.2f}",
            f"[{colour}]{r['rel_change'] * 100:+.1f}%[/]",
        )
    console.print(table)


@app.command("queries")
def queries_cmd(
    ref: Optional[str] = typer.Option(None, "--ref", help="Filter to one ref"),
    graph: Optional[str] = typer.Option(None, "--graph", "-g", help="Filter to one graph"),
    empty: bool = typer.Option(
        False, "--empty", help="Show only the zero-result cases"
    ),
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o"),
) -> None:
    """Per-query, per-graph latencies from the query_api workload."""
    rows = query_table(results, ref=ref, graph=graph)
    if empty:
        rows = [r for r in rows if r["empty"]]
    if not rows:
        console.print(
            "[yellow]no query results[/] — run a matrix including the "
            "[bold]query_api[/] workload (see matrices/queries.toml)."
        )
        raise typer.Exit(1)

    table = Table("query", "graph", "ref", "rows", "median ms", "reps")
    for r in rows:
        rows_cell = "[dim]0[/]" if r["empty"] else f"{r['rows']:,}"
        table.add_row(
            r["query"], r["graph"], r["ref"], rows_cell,
            f"{r['median_ms']:,.2f}", str(r["reps"]),
        )
    console.print(table)
    n_empty = sum(1 for r in rows if r["empty"])
    console.print(
        f"\n[dim]{len(rows)} rows, {n_empty} zero-result. Empty results are measured "
        "on purpose — the no-match path costs differently from a match.[/]"
    )

    dead = dead_queries(results, ref=ref)
    if dead:
        console.print(
            f"\n[yellow]warning:[/] {len(dead)} quer{'y' if len(dead) == 1 else 'ies'} "
            f"returned zero rows on *every* graph: {', '.join(dead)}.\n"
            "[dim]Zero on one graph is a deliberate no-match measurement; zero on all "
            "of them means the query matches nothing anywhere, so its low latency "
            "reflects a broken query rather than a fast one.[/]"
        )


@app.command("queries-compare")
def queries_compare_cmd(
    baseline: str = typer.Argument(..., help="Baseline ref spec"),
    candidate: str = typer.Argument(..., help="Candidate ref spec"),
    top: int = typer.Option(30, "--top"),
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o"),
) -> None:
    """Compare two refs query by query, per graph."""
    mismatches = query_row_mismatches(results, baseline, candidate)
    deltas = compare_queries(results, baseline, candidate)

    if not deltas and not mismatches:
        console.print("[yellow]nothing comparable — no query_api results for those refs[/]")
        raise typer.Exit(1)

    # Correctness first: a row-count disagreement outranks any timing.
    if mismatches:
        console.print(
            "[red bold]row-count mismatches[/] — these refs disagree about what the "
            "graph contains. This is a correctness difference, not a perf one:"
        )
        mt = Table("query", "graph", f"{baseline} rows", f"{candidate} rows")
        for m in mismatches:
            mt.add_row(m["query"], m["graph"], str(m["baseline_rows"]), str(m["candidate_rows"]))
        console.print(mt)
        console.print()

    if deltas:
        table = Table("query", "graph", "rows", f"{baseline} ms", f"{candidate} ms", "change", "verdict")
        for d in deltas[:top]:
            colour = {"SLOWER": "red", "faster": "green", "same": "dim", "noisy": "yellow"}[d.verdict]
            table.add_row(
                d.query, d.graph,
                "[dim]0[/]" if d.empty else f"{d.rows:,}",
                f"{d.baseline_ms:,.2f}", f"{d.candidate_ms:,.2f}",
                f"{d.rel_change * 100:+.1f}%",
                f"[{colour}]{d.verdict}[/]",
            )
        console.print(table)
        console.print(
            "\n[dim]change is on raw latency: negative is faster. 'noisy' means a "
            "ref's own repetitions disagreed by more than the delta, so it isn't a "
            "real effect. Only queries whose row counts agree are timed here.[/]"
        )
    if mismatches:
        raise typer.Exit(1)


_DRIVER_METRICS = {
    "tick": ("tick_latency_ms.median_ms", False, "tick latency (median ms)"),
    "tick-p95": ("tick_latency_ms.p95_ms", False, "tick latency (p95 ms)"),
    "jitter": ("jitter_ms.median_ms", False, "scheduling jitter vs period (median ms)"),
    "online": ("driver_online_complete_s", False, "time until all drivers complete first tick / fully online (s)"),
    "online-start": ("driver_online_spread_s", False, "stagger between drivers *starting* first tick / actor-create (s)"),
    "throughput": ("tick_ingest_rps_median", True, "in-tick ingest rows/sec (median)"),
    "overrun": ("period_overrun", False, "tick duration / period (>1 = can't keep up)"),
}


@app.command("driver-grid")
def driver_grid_cmd(
    ref: str = typer.Argument(..., help="Ref spec to show"),
    metric: str = typer.Option("tick", "--metric", "-m", help=f"one of: {', '.join(_DRIVER_METRICS)}"),
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o"),
) -> None:
    """Show one ref's driver_tick grid for a chosen metric."""
    if metric not in _DRIVER_METRICS:
        console.print(f"[red]unknown metric[/]; choose from: {', '.join(_DRIVER_METRICS)}")
        raise typer.Exit(2)
    dotted, _, label = _DRIVER_METRICS[metric]
    rows = driver_grid(results, ref=ref, metric=dotted)
    if not rows:
        console.print("[yellow]no driver_tick results — run matrices/driver-tick.toml[/]")
        raise typer.Exit(1)
    table = Table("drivers", "rows/tick", "period s", label, "reps")
    for r in rows:
        table.add_row(
            str(r["drivers"]), f"{r['rows_per_tick']:,}", f"{r['period_s']:.0f}",
            f"{r['value']:,.2f}", str(r["reps"]),
        )
    console.print(table)


@app.command("driver-compare")
def driver_compare_cmd(
    baseline: str = typer.Argument(..., help="Baseline ref"),
    candidate: str = typer.Argument(..., help="Candidate ref"),
    metric: str = typer.Option("tick", "--metric", "-m", help=f"one of: {', '.join(_DRIVER_METRICS)}"),
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o"),
) -> None:
    """Compare two refs at every driver_tick grid point for a metric."""
    if metric not in _DRIVER_METRICS:
        console.print(f"[red]unknown metric[/]; choose from: {', '.join(_DRIVER_METRICS)}")
        raise typer.Exit(2)
    dotted, hib, label = _DRIVER_METRICS[metric]
    deltas = compare_driver_grid(results, baseline, candidate, metric=dotted, higher_is_better=hib)
    if not deltas:
        console.print("[yellow]nothing comparable — no driver_tick results for those refs[/]")
        raise typer.Exit(1)
    console.print(f"[bold]{label}[/] — {'higher is better' if hib else 'lower is better'}\n")
    table = Table("drivers", "rows/tick", "period", f"{baseline}", f"{candidate}", "change", "verdict")
    for d in deltas:
        colour = {"faster": "green", "SLOWER": "red", "same": "dim", "noisy": "yellow"}[d.verdict]
        table.add_row(
            str(d.drivers), f"{d.rows_per_tick:,}", f"{d.period_s:.0f}s",
            f"{d.baseline:,.2f}", f"{d.candidate:,.2f}",
            f"{d.improvement * 100:+.1f}%", f"[{colour}]{d.verdict}[/]",
        )
    console.print(table)
    console.print(
        "\n[dim]change is polarity-adjusted: positive is always better. 'noisy' = a "
        "ref's reps disagreed by more than the delta.[/]"
    )


@app.command("app-scale")
def app_scale_cmd(
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o"),
) -> None:
    """Show the app-scalability escalation per branch and each branch's ceiling."""
    ceilings = app_scale_ceilings(results)
    rows = app_scale_points(results)
    if not rows:
        console.print("[yellow]no app_scale results — run matrices/app-scale.toml[/]")
        raise typer.Exit(1)

    table = Table("ref", "N", "online", "startup s", "run ms (p50)", "dispatch ms",
                  "thru/s", "app mem MB", "per-app MB", "status")
    for r in rows:
        status = "[green]ok[/]" if r["complete"] else f"[red]CEILING[/] ({(r['failure'] or '')[:40]})"
        def f(v, fmt="{:,.1f}"):
            return fmt.format(v) if isinstance(v, (int, float)) else "—"
        table.add_row(
            r["ref"], str(r["n"]), f"{r['apps_online']}/{r['n']}",
            f(r["startup_s"]), f(r["run_ms_median"]), f(r["dispatch_ms_median"]),
            f(r["throughput_per_s"]), f(r["app_memory_mb"]), f(r["per_app_memory_mb"]),
            status,
        )
    console.print(table)
    console.print()
    for ref, c in ceilings.items():
        fail = c.get("first_failure")
        boundary = (
            f"broke at {fail}" if fail is not None
            else "did not break within the counts run"
        )
        console.print(
            f"[bold]{ref}[/] ({c['branch']}): ran [bold]{c['ceiling']}[/] apps, {boundary}  "
            f"[dim]— {c['reason']}[/]"
        )
    console.print(
        "\n[dim]startup = time until all N apps online; run/dispatch/throughput are "
        "steady-state only (post all-online). app mem = total RSS of the app runtime "
        "(containers on main, Ray actors on ums-ray).[/]"
    )


@app.command("spans")
def spans_cmd(
    workload: Optional[str] = typer.Option(None, "--workload", "-w", help="Filter to one workload"),
    ref: Optional[str] = typer.Option(None, "--ref", help="Filter to one ref"),
    top: int = typer.Option(25, "--top", help="How many spans to show"),
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o"),
) -> None:
    """Show where the server spent its time, per span.

    Only populated for runs recorded with `--profile`.
    """
    rows = span_table(results, workload=workload, ref=ref)
    if not rows:
        console.print(
            "[yellow]no span data[/] — re-run with [bold]--profile[/] to capture "
            "server-side timings."
        )
        raise typer.Exit(1)

    table = Table("workload", "ref", "span", "total ms", "mean ms", "calls")
    for r in rows[:top]:
        table.add_row(
            r["workload"], r["ref"], r["span"][:60],
            f"{r['total_ms']:,.1f}", f"{r['mean_ms']:,.3f}", f"{r['calls']:,.0f}",
        )
    console.print(table)
    console.print(
        "\n[dim]Spans nest, so totals overlap and will exceed wall-clock. "
        "Use them for attribution, not as a time budget.[/]"
    )


@app.command("spans-compare")
def spans_compare_cmd(
    baseline: str = typer.Argument(..., help="Baseline ref spec"),
    candidate: str = typer.Argument(..., help="Candidate ref spec"),
    workload: Optional[str] = typer.Option(None, "--workload", "-w"),
    top: int = typer.Option(25, "--top"),
    results: Path = typer.Option(DEFAULT_RESULTS, "--results", "-o"),
) -> None:
    """Attribute a ref-to-ref difference to individual server-side spans.

    Reach for this when a headline number moved and it isn't obvious why: it
    says which internal step shifted, not just that the total did.
    """
    deltas = compare_spans(results, baseline, candidate, workload=workload)
    if not deltas:
        console.print(
            "[yellow]no span data for those refs[/] — re-run with [bold]--profile[/]."
        )
        raise typer.Exit(1)

    table = Table("workload", "span", f"{baseline} ms", f"{candidate} ms", "delta ms", "change", "calls", "verdict")
    for d in deltas[:top]:
        colour = {
            "SLOWER": "red", "faster": "green", "same": "dim", "NEW": "yellow", "gone": "cyan"
        }[d.verdict]
        calls = (
            f"{d.baseline_calls:,.0f}"
            if d.baseline_calls == d.candidate_calls
            else f"{d.baseline_calls:,.0f}->{d.candidate_calls:,.0f}"
        )
        rel = "—" if d.verdict in ("NEW", "gone") else f"{d.rel_change * 100:+.1f}%"
        table.add_row(
            d.workload, d.span[:50],
            f"{d.baseline_ms:,.1f}", f"{d.candidate_ms:,.1f}",
            f"{d.delta_ms:+,.1f}", rel, calls,
            f"[{colour}]{d.verdict}[/]",
        )
    console.print(table)
    console.print(
        "\n[dim]Ordered by absolute time shifted — the top rows are what explain "
        "the headline change. A changed call count often matters more than a "
        "changed per-call cost.[/]"
    )


services_app = typer.Typer(help="Manage the Postgres the timescale backend needs.")
app.add_typer(services_app, name="services")


@services_app.command("up")
def services_up() -> None:
    try:
        pg = services.up()
    except services.ServiceError as e:
        console.print(f"[red]error:[/] {e}")
        raise typer.Exit(1)
    console.print(f"[green]postgres ready[/] {pg.dsn}")


@services_app.command("down")
def services_down() -> None:
    services.down()
    console.print("[green]postgres removed[/]")


@app.command("env")
def env() -> None:
    """Print the environment manifest that gets recorded with results."""
    console.print_json(json.dumps(environment_manifest()))


def main() -> None:
    app()


if __name__ == "__main__":
    sys.exit(app())

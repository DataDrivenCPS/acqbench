"""Report logic — where a sign error would quietly invert a regression call."""

from __future__ import annotations

import json

import pytest

from acqbench.report import NOISE_FLOOR, aggregate, compare, marginal_cost


def _row(**kw):
    base = dict(
        cell_id="abc123",
        ref_spec="pypi:0.3.1",
        ref_version="0.3.1",
        ref_resolved="0.3.1",
        backend="duckdb",
        topology="server",
        read_batch_size=50_000,
        driver_count=0,
        app_count=0,
        workload="write_arrow",
        repetition=0,
        params={},
        ok=True,
        error=None,
        metrics={"rows_per_second": 1000.0},
        resources={},
        started_at="2026-01-01T00:00:00Z",
        duration_seconds=1.0,
        schema_version=1,
    )
    base.update(kw)
    return base


def _write(path, rows):
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return path


def test_aggregate_takes_median_not_mean(tmp_path):
    # One pathological repetition must not move the reported figure.
    p = _write(
        tmp_path / "r.jsonl",
        [
            _row(repetition=0, metrics={"rows_per_second": 1000.0}),
            _row(repetition=1, metrics={"rows_per_second": 1010.0}),
            _row(repetition=2, metrics={"rows_per_second": 1.0}),  # outlier
        ],
    )
    agg = next(iter(aggregate(p).values()))
    assert agg.median == 1000.0  # mean would be ~670


def test_failed_runs_are_counted_not_averaged(tmp_path):
    p = _write(
        tmp_path / "r.jsonl",
        [
            _row(repetition=0),
            _row(repetition=1, ok=False, error="boom", metrics={}),
        ],
    )
    agg = next(iter(aggregate(p).values()))
    assert agg.failures == 1
    assert len(agg.values) == 1


def test_higher_is_better_metric_improvement(tmp_path):
    p = _write(
        tmp_path / "r.jsonl",
        [
            _row(ref_spec="pypi:0.3.1", metrics={"rows_per_second": 1000.0}),
            _row(ref_spec="git:main", metrics={"rows_per_second": 2000.0}),
        ],
    )
    c = compare(p, "pypi:0.3.1")[0]
    assert c.improvement == pytest.approx(1.0)
    assert c.verdict == "faster"


def test_lower_is_better_metric_polarity_is_inverted(tmp_path):
    # read_limit is judged on latency: a smaller number is an improvement.
    p = _write(
        tmp_path / "r.jsonl",
        [
            _row(workload="read_limit", ref_spec="pypi:0.3.1",
                 metrics={"latency": {"median_ms": 100.0}}),
            _row(workload="read_limit", ref_spec="git:main",
                 metrics={"latency": {"median_ms": 50.0}}),
        ],
    )
    c = compare(p, "pypi:0.3.1")[0]
    assert c.rel_change == pytest.approx(-0.5)   # raw metric halved
    assert c.improvement == pytest.approx(0.5)   # ...which is better
    assert c.verdict == "faster"


def test_regression_is_flagged(tmp_path):
    p = _write(
        tmp_path / "r.jsonl",
        [
            _row(ref_spec="pypi:0.3.1", metrics={"rows_per_second": 1000.0}),
            _row(ref_spec="git:main", metrics={"rows_per_second": 500.0}),
        ],
    )
    assert compare(p, "pypi:0.3.1")[0].verdict == "SLOWER"


def test_small_change_is_within_noise_floor(tmp_path):
    p = _write(
        tmp_path / "r.jsonl",
        [
            _row(ref_spec="pypi:0.3.1", metrics={"rows_per_second": 1000.0}),
            _row(ref_spec="git:main", metrics={"rows_per_second": 1000.0 * (1 + NOISE_FLOOR / 2)}),
        ],
    )
    assert compare(p, "pypi:0.3.1")[0].verdict == "same"


def test_wide_spread_is_reported_as_noisy(tmp_path):
    # If a ref's own repetitions disagree wildly, no verdict is supportable.
    p = _write(
        tmp_path / "r.jsonl",
        [
            _row(ref_spec="pypi:0.3.1", repetition=0, metrics={"rows_per_second": 1000.0}),
            _row(ref_spec="pypi:0.3.1", repetition=1, metrics={"rows_per_second": 5000.0}),
            _row(ref_spec="git:main", repetition=0, metrics={"rows_per_second": 2000.0}),
        ],
    )
    assert compare(p, "pypi:0.3.1")[0].verdict == "noisy"


def test_refs_are_only_compared_like_for_like(tmp_path):
    # A duckdb run must never be compared against a timescale run.
    p = _write(
        tmp_path / "r.jsonl",
        [
            _row(ref_spec="pypi:0.3.1", backend="duckdb"),
            _row(ref_spec="git:main", backend="timescale"),
        ],
    )
    assert compare(p, "pypi:0.3.1") == []


def test_unknown_baseline_raises(tmp_path):
    p = _write(tmp_path / "r.jsonl", [_row()])
    with pytest.raises(ValueError, match="no results"):
        compare(p, "git:nonexistent")


def test_marginal_cost_measures_topology_delta(tmp_path):
    p = _write(
        tmp_path / "r.jsonl",
        [
            _row(topology="server", metrics={"rows_per_second": 1000.0}),
            _row(topology="server+drivers", metrics={"rows_per_second": 900.0}),
        ],
    )
    rows = marginal_cost(p)
    assert len(rows) == 1
    # Drivers cost 10% of write throughput; higher-is-better, so it reads negative.
    assert rows[0]["rel_change"] == pytest.approx(-0.1)
    assert rows[0]["topology"] == "server+drivers"


def test_malformed_line_does_not_poison_the_report(tmp_path, capsys):
    p = tmp_path / "r.jsonl"
    p.write_text(json.dumps(_row()) + "\n{ this is not json\n")
    assert len(aggregate(p)) == 1
    assert "malformed" in capsys.readouterr().out


def _span_row(ref, spans, workload="write_arrow", **kw):
    return _row(ref_spec=ref, workload=workload, profile=True, spans=spans, **kw)


def test_profiled_and_unprofiled_runs_are_never_compared(tmp_path):
    # DEBUG logging is not free, so mixing them would report logging overhead
    # as a regression.
    p = _write(
        tmp_path / "r.jsonl",
        [
            _row(ref_spec="pypi:0.3.1", profile=False),
            _row(ref_spec="git:main", profile=True),
        ],
    )
    assert compare(p, "pypi:0.3.1") == []


def test_compare_spans_attributes_a_slowdown(tmp_path):
    from acqbench.report import compare_spans

    p = _write(
        tmp_path / "r.jsonl",
        [
            _span_row("pypi:0.3.1", {
                "dedupe rows=<n>": {"logger": "s", "count": 10, "total_ms": 100.0, "mean_ms": 10.0,
                                    "median_ms": 10.0, "p95_ms": 10.0, "max_ms": 10.0},
                "insert rows=<n>": {"logger": "s", "count": 10, "total_ms": 50.0, "mean_ms": 5.0,
                                    "median_ms": 5.0, "p95_ms": 5.0, "max_ms": 5.0},
            }),
            _span_row("git:main", {
                "dedupe rows=<n>": {"logger": "s", "count": 10, "total_ms": 400.0, "mean_ms": 40.0,
                                    "median_ms": 40.0, "p95_ms": 40.0, "max_ms": 40.0},
                "insert rows=<n>": {"logger": "s", "count": 10, "total_ms": 50.0, "mean_ms": 5.0,
                                    "median_ms": 5.0, "p95_ms": 5.0, "max_ms": 5.0},
            }),
        ],
    )
    deltas = compare_spans(p, "pypi:0.3.1", "git:main")
    # Ordered by absolute time shifted, so the culprit leads.
    assert deltas[0].span == "dedupe rows=<n>"
    assert deltas[0].delta_ms == pytest.approx(300.0)
    assert deltas[0].verdict == "SLOWER"
    assert deltas[1].verdict == "same"


def test_compare_spans_flags_new_and_removed_spans(tmp_path):
    from acqbench.report import compare_spans

    stat = lambda ms: {"logger": "s", "count": 1, "total_ms": ms, "mean_ms": ms,
                       "median_ms": ms, "p95_ms": ms, "max_ms": ms}
    p = _write(
        tmp_path / "r.jsonl",
        [
            _span_row("pypi:0.3.1", {"old_step": stat(10.0)}),
            _span_row("git:main", {"new_step": stat(20.0)}),
        ],
    )
    verdicts = {d.span: d.verdict for d in compare_spans(p, "pypi:0.3.1", "git:main")}
    assert verdicts == {"new_step": "NEW", "old_step": "gone"}


def test_span_table_ignores_unprofiled_rows(tmp_path):
    from acqbench.report import span_table

    p = _write(tmp_path / "r.jsonl", [_row(profile=False, spans={})])
    assert span_table(p) == []


def _query_row(ref, graphs, **kw):
    # Mirrors the real on-disk schema: each graph carries {queries, setup}.
    wrapped = {g: {"queries": qs, "setup": {"insert_ms": 1.0}} for g, qs in graphs.items()}
    return _row(ref_spec=ref, workload="query_api", metrics={"graphs": wrapped}, **kw)


def _qstat(rows, median_ms):
    return {"rows": rows, "empty": rows == 0, "median_ms": median_ms, "count": 5}


def test_query_table_keeps_zero_result_cases(tmp_path):
    from acqbench.report import query_table

    # A Sensor query returns 0 on benicia by design. That measurement is the
    # no-match path and must not be filtered away.
    p = _write(tmp_path / "r.jsonl", [
        _query_row("pypi:0.3.1", {
            "benicia": {"sensors_by_class": _qstat(0, 3.0)},
            "watertap": {"sensors_by_class": _qstat(32, 9.0)},
        }),
    ])
    rows = query_table(p)
    by_graph = {r["graph"]: r for r in rows}
    assert by_graph["benicia"]["empty"] is True
    assert by_graph["benicia"]["median_ms"] == 3.0
    assert by_graph["watertap"]["empty"] is False


def test_compare_queries_matches_like_for_like(tmp_path):
    from acqbench.report import compare_queries

    p = _write(tmp_path / "r.jsonl", [
        _query_row("pypi:0.3.1", {"benicia": {"pumps": _qstat(4, 10.0)}}),
        _query_row("git:main", {"benicia": {"pumps": _qstat(4, 20.0)}}),
    ])
    d = compare_queries(p, "pypi:0.3.1", "git:main")[0]
    assert d.query == "pumps" and d.graph == "benicia"
    assert d.rel_change == pytest.approx(1.0)
    assert d.verdict == "SLOWER"


def test_row_count_disagreement_is_a_correctness_signal_not_a_timing_one(tmp_path):
    from acqbench.report import compare_queries, query_row_mismatches

    # If two refs disagree about what the graph contains, timing them against
    # each other would bury the real finding.
    p = _write(tmp_path / "r.jsonl", [
        _query_row("pypi:0.3.1", {"benicia": {"pumps": _qstat(4, 10.0)}}),
        _query_row("git:main", {"benicia": {"pumps": _qstat(2, 10.0)}}),
    ])
    assert compare_queries(p, "pypi:0.3.1", "git:main") == []
    m = query_row_mismatches(p, "pypi:0.3.1", "git:main")
    assert m == [{"query": "pumps", "graph": "benicia",
                  "baseline_rows": 4, "candidate_rows": 2}]


def test_query_table_aggregates_repetitions_by_median(tmp_path):
    from acqbench.report import query_table

    p = _write(tmp_path / "r.jsonl", [
        _query_row("pypi:0.3.1", {"benicia": {"pumps": _qstat(4, 10.0)}}, repetition=0),
        _query_row("pypi:0.3.1", {"benicia": {"pumps": _qstat(4, 12.0)}}, repetition=1),
        _query_row("pypi:0.3.1", {"benicia": {"pumps": _qstat(4, 500.0)}}, repetition=2),
    ])
    r = query_table(p)[0]
    assert r["median_ms"] == 12.0  # outlier ignored
    assert r["reps"] == 3


def test_dead_queries_flags_queries_empty_on_every_graph(tmp_path):
    from acqbench.report import dead_queries

    p = _write(tmp_path / "r.jsonl", [
        _query_row("pypi:0.3.1", {
            # Empty on benicia only — a deliberate no-match measurement.
            "benicia":  {"sensors": _qstat(0, 3.0), "broken": _qstat(0, 0.5)},
            "watertap": {"sensors": _qstat(32, 9.0), "broken": _qstat(0, 0.5)},
        }),
    ])
    # `sensors` is a valid zero-result case; `broken` matches nothing anywhere.
    assert dead_queries(p) == ["broken"]


def test_query_delta_flagged_noisy_when_reps_disagree_more_than_effect(tmp_path):
    from acqbench.report import compare_queries

    # main's 3 reps swing wildly (10 / 30 / 12 -> median 12, spread ~167%);
    # ums's are tight. A +10% median delta is well inside main's own jitter.
    p = _write(tmp_path / "r.jsonl", [
        _query_row("git:main", {"ingested": {"scan": _qstat(2_000_000, 10.0)}}, repetition=0),
        _query_row("git:main", {"ingested": {"scan": _qstat(2_000_000, 30.0)}}, repetition=1),
        _query_row("git:main", {"ingested": {"scan": _qstat(2_000_000, 12.0)}}, repetition=2),
        _query_row("git:ums", {"ingested": {"scan": _qstat(2_000_000, 13.2)}}, repetition=0),
        _query_row("git:ums", {"ingested": {"scan": _qstat(2_000_000, 13.3)}}, repetition=1),
        _query_row("git:ums", {"ingested": {"scan": _qstat(2_000_000, 13.1)}}, repetition=2),
    ])
    d = compare_queries(p, "git:main", "git:ums")[0]
    assert d.verdict == "noisy", f"got {d.verdict} (rel_change={d.rel_change:.2f})"


def test_query_delta_real_when_effect_exceeds_spread(tmp_path):
    from acqbench.report import compare_queries

    # Both refs tight; candidate is 2x slower. That's real.
    p = _write(tmp_path / "r.jsonl", [
        _query_row("git:main", {"ingested": {"scan": _qstat(100, 10.0)}}, repetition=0),
        _query_row("git:main", {"ingested": {"scan": _qstat(100, 10.1)}}, repetition=1),
        _query_row("git:ums", {"ingested": {"scan": _qstat(100, 20.0)}}, repetition=0),
        _query_row("git:ums", {"ingested": {"scan": _qstat(100, 20.1)}}, repetition=1),
    ])
    d = compare_queries(p, "git:main", "git:ums")[0]
    assert d.verdict == "SLOWER"


def test_app_scale_points_and_ceiling(tmp_path):
    from acqbench.report import app_scale_points, app_scale_ceilings
    row = {
        "workload": "app_scale", "ref_spec": "git:main", "ok": True,
        "metrics": {"branch": "main", "ceiling": 25, "ceiling_reason": "only 40/50 online",
            "points": [
                {"n": 25, "complete": True, "apps_online": 25, "startup_s": 30.0,
                 "received_to_completed_ms": {"median_ms": 45.0, "p95_ms": 60.0},
                 "completed_to_endpoint_ms": {"median_ms": 8.0},
                 "steady_throughput_per_s": 12.0, "app_memory_mb": 3000.0, "per_app_memory_mb": 120.0},
                {"n": 50, "complete": False, "apps_online": 40, "failure": "only 40/50 online"},
            ]}}
    p = tmp_path / "r.jsonl"; p.write_text(json.dumps(row) + "\n")
    pts = app_scale_points(p)
    assert [x["n"] for x in pts] == [25, 50]
    assert pts[0]["complete"] and not pts[1]["complete"]
    assert pts[0]["per_app_memory_mb"] == 120.0
    assert app_scale_ceilings(p)["git:main"]["ceiling"] == 25

"""driver_tick grid reporting: each cell is one (drivers, rows, period) point."""

from __future__ import annotations

import json

import pytest

from acqbench.report import driver_grid, compare_driver_grid


def _cell(ref, drivers, rows, period, tick_median, spread_reps=None):
    m = {
        "drivers": drivers, "rows_per_tick": rows, "period_s": period,
        "tick_latency_ms": {"median_ms": tick_median, "count": 4},
        "jitter_ms": {"median_ms": 10.0}, "driver_online_spread_s": drivers * 0.1,
        "tick_ingest_rps_median": 5000.0, "period_overrun": 0.01,
    }
    return {
        "workload": "driver_tick", "ref_spec": ref, "ok": True, "metrics": m,
    }


def _write(path, rows):
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return path


def test_driver_grid_one_point_per_cell(tmp_path):
    p = _write(tmp_path / "r.jsonl", [
        _cell("git:main", 1, 100, 30, 12.0),
        _cell("git:main", 100, 100, 30, 45.0),
    ])
    rows = driver_grid(p, ref="git:main")
    pts = {r["point"]: r["value"] for r in rows}
    assert pts == {"d1_r100_p30": 12.0, "d100_r100_p30": 45.0}


def test_driver_grid_selects_metric_by_dotted_path(tmp_path):
    p = _write(tmp_path / "r.jsonl", [_cell("git:main", 10, 1000, 60, 20.0)])
    rows = driver_grid(p, ref="git:main", metric="driver_online_spread_s")
    assert rows[0]["value"] == pytest.approx(1.0)  # 10 * 0.1


def test_driver_compare_flags_regression_and_respects_noise(tmp_path):
    # main fast, ums 3x slower at d100 — a real regression on tick latency.
    p = _write(tmp_path / "r.jsonl", [
        _cell("git:main", 100, 10000, 30, 15.0),
        _cell("git:ums", 100, 10000, 30, 45.0),
    ])
    d = compare_driver_grid(p, "git:main", "git:ums", metric="tick_latency_ms.median_ms")[0]
    assert d.drivers == 100
    assert d.verdict == "SLOWER"  # candidate slower, lower-is-better


def test_driver_compare_higher_is_better_for_throughput(tmp_path):
    p = _write(tmp_path / "r.jsonl", [
        {"workload": "driver_tick", "ref_spec": "git:main", "ok": True,
         "metrics": {"drivers": 10, "rows_per_tick": 1000, "period_s": 30,
                     "tick_ingest_rps_median": 4000.0}},
        {"workload": "driver_tick", "ref_spec": "git:ums", "ok": True,
         "metrics": {"drivers": 10, "rows_per_tick": 1000, "period_s": 30,
                     "tick_ingest_rps_median": 8000.0}},
    ])
    d = compare_driver_grid(p, "git:main", "git:ums",
                            metric="tick_ingest_rps_median", higher_is_better=True)[0]
    assert d.verdict == "faster"  # candidate 2x throughput

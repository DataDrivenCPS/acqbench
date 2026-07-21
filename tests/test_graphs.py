"""The shipped graphs are pinned benchmark inputs.

If their content changes, every previously-recorded result silently becomes
incomparable — the numbers would reflect different data, not different code.
These tests exist to make that change loud.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

GRAPHS_DIR = Path(__file__).resolve().parents[1] / "graphs"

# Recorded when the graphs were vendored from acquirium @ bfb3385.
EXPECTED = {
    "benicia.ttl": "a803ee12e565c7212f5857740daada5fb3d78a59b5cac86e46e0596d93b39cd2",
    "benicia-100.ttl": "c03715d5bb3dce6e38d9a254db44b6e7f1011c3c0af0d99579b2d396997bb8c2",
    "watertap-seawater-ro.ttl": "fa17f620a29cfc5f1c448308031d415e3299ad63f35475da0c3d5d02b8031a6c",
}


def _sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


@pytest.mark.parametrize("name,digest", sorted(EXPECTED.items()))
def test_graph_content_is_pinned(name, digest):
    p = GRAPHS_DIR / name
    assert p.exists(), f"{name} is missing from graphs/"
    assert _sha256(p) == digest, (
        f"{name} changed. Results recorded against the old content are no longer "
        "comparable. If this is intentional, update EXPECTED, graphs/SHA256SUMS "
        "and graphs/README.md in the same commit."
    )


def test_sha256sums_file_matches():
    sums = (GRAPHS_DIR / "SHA256SUMS").read_text().split()
    recorded = dict(zip(sums[1::2], sums[0::2]))
    assert recorded == EXPECTED


def test_workload_graph_map_points_at_real_files():
    from acqbench.workloads.query import GRAPHS, GRAPHS_DIR as WL_DIR

    for key, filename in GRAPHS.items():
        assert (WL_DIR / filename).exists(), f"graph {key} -> {filename} not found"


def test_graphs_have_the_asymmetry_the_zero_result_cases_rely_on():
    # A Sensor query returning 0 rows on benicia is only a meaningful
    # measurement if benicia genuinely has no sensors. Guard the premise —
    # if a graph is ever refreshed and gains sensors, the "zero-result" case
    # would quietly become a populated one and nobody would notice.
    benicia = (GRAPHS_DIR / "benicia.ttl").read_text()
    watertap = (GRAPHS_DIR / "watertap-seawater-ro.ttl").read_text()

    # Sensors: the cleanest split.
    assert "a s223:Sensor" not in benicia
    assert "a s223:Sensor" in watertap

    # Wastewater-only equipment.
    assert "a nawi:SedimentationTank" in benicia
    assert "a nawi:SedimentationTank" not in watertap

    # Desalination-only equipment.
    assert "a nawi:ReverseOsmosisMembrane" in watertap
    assert "a nawi:ReverseOsmosisMembrane" not in benicia

    # Both have pumps — so a Pump query is the control that returns rows
    # everywhere, distinguishing "query is broken" from "graph lacks the class".
    assert "a nawi:Pump" in benicia
    assert "a nawi:Pump" in watertap


def test_benicia_100_is_denser_than_benicia():
    # Same plant, more properties: isolates scale from structure.
    b = (GRAPHS_DIR / "benicia.ttl").read_text()
    b100 = (GRAPHS_DIR / "benicia-100.ttl").read_text()
    n = b.count("a s223:QuantifiableObservableProperty")
    n100 = b100.count("a s223:QuantifiableObservableProperty")
    assert n100 > n, f"benicia-100 ({n100}) should have more properties than benicia ({n})"

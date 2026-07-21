from __future__ import annotations

import pytest

from acqbench.matrixfile import MatrixError, from_dict
from acqbench.spec import Backend, Topology


def test_minimal_matrix():
    m = from_dict({"refs": ["pypi:0.3.1"], "workloads": ["write_arrow"]})
    assert len(m.refs) == 1
    assert m.topologies == [Topology.SERVER]  # defaults to server-only
    assert len(m.configs) == 1


def test_sweep_expands_to_cartesian_product():
    m = from_dict(
        {
            "refs": ["pypi:0.3.1"],
            "workloads": ["write_arrow"],
            "sweep": {"backend": ["duckdb", "timescale"], "read_batch_size": [1000, 50_000]},
        }
    )
    assert len(m.configs) == 4
    assert {c.backend for c in m.configs} == {Backend.DUCKDB, Backend.TIMESCALE}
    assert {c.read_batch_size for c in m.configs} == {1000, 50_000}


def test_sweep_and_configs_are_mutually_exclusive():
    with pytest.raises(MatrixError, match="not both"):
        from_dict(
            {
                "refs": ["pypi:0.3.1"],
                "workloads": ["write_arrow"],
                "configs": [{"backend": "duckdb"}],
                "sweep": {"backend": ["duckdb"]},
            }
        )


def test_unknown_workload_fails_before_the_run_starts():
    # Better to fail here than three cells into an hour-long matrix.
    with pytest.raises(MatrixError, match="unknown workload"):
        from_dict({"refs": ["pypi:0.3.1"], "workloads": ["write_arrow", "nope"]})


def test_unknown_backend_is_rejected():
    with pytest.raises(MatrixError, match="unknown backend"):
        from_dict(
            {"refs": ["pypi:0.3.1"], "workloads": ["write_arrow"],
             "configs": [{"backend": "sqlite"}]}
        )


def test_unknown_topology_is_rejected():
    with pytest.raises(MatrixError, match="unknown topology"):
        from_dict(
            {"refs": ["pypi:0.3.1"], "workloads": ["write_arrow"], "topologies": ["cluster"]}
        )


def test_missing_required_fields():
    with pytest.raises(MatrixError, match="refs"):
        from_dict({"workloads": ["write_arrow"]})
    with pytest.raises(MatrixError, match="workloads"):
        from_dict({"refs": ["pypi:0.3.1"]})


def test_duplicate_refs_are_rejected():
    with pytest.raises(MatrixError, match="duplicate"):
        from_dict({"refs": ["pypi:0.3.1", "pypi:0.3.1"], "workloads": ["write_arrow"]})


def test_refs_accept_extras_table_form():
    m = from_dict(
        {"refs": [{"spec": "git:main", "extras": ["mqtt"]}], "workloads": ["write_arrow"]}
    )
    assert m.refs[0].extras == ("mqtt",)


def test_workload_params_must_name_real_workloads():
    with pytest.raises(MatrixError, match="unknown workload"):
        from_dict(
            {"refs": ["pypi:0.3.1"], "workloads": ["write_arrow"],
             "workload_params": {"bogus": {"streams": 1}}}
        )


def test_invalid_repetitions():
    with pytest.raises(MatrixError, match="repetitions"):
        from_dict(
            {"refs": ["pypi:0.3.1"], "workloads": ["write_arrow"], "run": {"repetitions": 0}}
        )


def test_shipped_matrices_all_load():
    from pathlib import Path
    from acqbench.matrixfile import load

    root = Path(__file__).resolve().parents[1] / "matrices"
    for f in sorted(root.glob("*.toml")):
        m = load(f)
        assert m.refs and m.workloads, f"{f.name} is empty"


def test_apps_topologies_are_rejected_until_implemented():
    # Rendering them would silently produce a config identical to `server`,
    # filing duplicate data under a label claiming apps ran.
    for t in ("server+apps", "server+drivers+apps"):
        with pytest.raises(MatrixError, match="not implemented"):
            from_dict({"refs": ["pypi:0.3.1"], "workloads": ["write_arrow"], "topologies": [t]})

"""The query_api workload straddles two venvs, which is where it can go wrong."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from acqbench.workloads.query import GRAPHS, GRAPHS_DIR, QueryApi, _script_path

QUERIES_PY = Path(__file__).resolve().parents[1] / "src" / "acqbench" / "queries.py"


def test_script_path_resolves_without_importing_queries():
    # queries.py imports acquirium, which exists only in a *ref's* venv. If this
    # helper ever imports it instead of locating it by path, every run dies here
    # with ModuleNotFoundError.
    p = _script_path()
    assert p.exists()
    assert p.name == "queries.py"


def test_queries_module_is_never_imported_by_the_harness():
    # Guard the rule at the source level so a future refactor can't reintroduce
    # the import.
    src = (Path(__file__).resolve().parents[1] / "src" / "acqbench" / "workloads" / "query.py").read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in ("queries", "acqbench.queries"):
            pytest.fail("workloads/query.py must not import acqbench.queries")
        if isinstance(node, ast.Import):
            for a in node.names:
                assert "queries" not in a.name, "workloads/query.py must not import queries"


def test_queries_script_only_depends_on_acquirium_and_stdlib():
    # It is exec'd by a ref's interpreter, which has acquirium but not httpx,
    # polars-the-harness-pin, rich, or anything else acqbench depends on.
    tree = ast.parse(QUERIES_PY.read_text())
    third_party = set()
    stdlib_ok = {
        "argparse", "json", "time", "dataclasses", "pathlib", "typing",
        "__future__", "sys", "os", "statistics", "traceback", "collections",
    }
    for node in ast.walk(tree):
        mods = []
        if isinstance(node, ast.Import):
            mods = [a.name.split(".")[0] for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            mods = [node.module.split(".")[0]]
        for m in mods:
            if m not in stdlib_ok and m != "acquirium":
                third_party.add(m)
    assert not third_party, (
        f"queries.py imports {third_party}, which a ref's venv is not guaranteed to have"
    )


def test_queries_script_exposes_the_contract_the_workload_calls():
    src = QUERIES_PY.read_text()
    tree = ast.parse(src)
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "main" in names
    # The workload passes exactly these flags.
    for flag in ("--base-url", "--graph", "--repeat", "--json-out"):
        assert flag in src, f"queries.py must accept {flag}"


def test_every_shipped_graph_is_mapped_and_present():
    for key, filename in GRAPHS.items():
        assert (GRAPHS_DIR / filename).exists(), f"{key} -> {filename} missing"


def test_unknown_graph_is_rejected_at_setup():
    wl = QueryApi(graphs=["benicia", "atlantis"])
    with pytest.raises(ValueError, match="atlantis"):
        wl.setup(_ctx())


def _ctx():
    class _C:
        pass

    return _C()


REAL_OUTPUT = Path(__file__).resolve().parent / "fixtures" / "queries-benicia-real.json"


def test_workload_parses_the_scripts_real_output_schema():
    """Pin the contract against output the script actually produced.

    This test exists because it caught a real bug: the workload iterated the
    top-level dict as if it were the query map, hit the `graph` string, and blew
    up with AttributeError. Hand-written fixtures had happily agreed with the
    wrong shape, so only real output could catch it.
    """
    import json

    payload = json.loads(REAL_OUTPUT.read_text())
    assert set(payload) >= {"graph", "setup", "queries"}

    # The shape the workload relies on.
    for qname, r in payload["queries"].items():
        assert isinstance(r, dict), f"{qname} should be a dict"
        assert "rows" in r and "times_ms" in r
    assert "insert_ms" in payload["setup"]


def test_real_output_has_no_dead_queries_and_real_zero_cases():
    import json

    payload = json.loads(REAL_OUTPUT.read_text())
    q = payload["queries"]
    # benicia has no sensors: a deliberate no-match measurement, still timed.
    assert q["entity_sensor_class"]["rows"] == 0
    assert q["entity_sensor_class"]["times_ms"], "zero-result cases must still be timed"
    # The control: pumps exist, so a zero here would mean a broken query.
    assert q["entity_by_class_pump"]["rows"] == 4

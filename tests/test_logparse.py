"""Log parsing — the lines here are real acquirium output, not invented."""

from __future__ import annotations

from acqbench import logparse

# Verbatim from an acquirium server running with -v.
REAL_LOG = """\
2026-07-15 19:33:01,950 INFO acquirium.embedding_matcher Embedding 2290 surfaces from 1381 concepts...
2026-07-15 19:33:01,951 DEBUG acquirium.storage → bulk_insert_polars prepare/dedupe rows=5000
2026-07-15 19:33:01,962 DEBUG acquirium.storage ← bulk_insert_polars prepare/dedupe rows=5000 (11.4 ms)
2026-07-15 19:33:01,963 DEBUG acquirium.storage → bulk_insert_polars DELETE+INSERT rows=5000
2026-07-15 19:33:02,100 DEBUG acquirium.storage ← bulk_insert_polars DELETE+INSERT rows=5000 (137.2 ms)
2026-07-15 19:33:02,101 DEBUG acquirium.manager → insert_timeseries_batch source=bench streams=50
2026-07-15 19:33:02,300 DEBUG acquirium.manager ← insert_timeseries_batch source=bench streams=50 (199.0 ms)
2026-07-15 19:33:02,301 INFO acquirium.api /insert_timeseries wrote 5000 rows
"""


def test_parses_only_exit_lines_with_elapsed():
    spans = logparse.parse(REAL_LOG)
    # Three spans; the → entry lines and the INFO lines are not spans.
    assert len(spans) == 3
    assert {s.ms for s in spans} == {11.4, 137.2, 199.0}


def test_captures_logger_name():
    spans = logparse.parse(REAL_LOG)
    assert {s.logger for s in spans} == {"acquirium.storage", "acquirium.manager"}


def test_interpolated_args_are_normalized_so_spans_aggregate():
    # 'rows=5000' and 'rows=100' are the same span; without normalizing they'd
    # fragment into thousands of one-call entries.
    a = logparse.normalize("bulk_insert_polars prepare/dedupe rows=5000")
    b = logparse.normalize("bulk_insert_polars prepare/dedupe rows=100")
    assert a == b == "bulk_insert_polars prepare/dedupe rows=<n>"


def test_ref_uris_are_normalized():
    n = logparse.normalize("resolve urn:acquirium#3f2a1b4c-5d6e-7f80-9012-3a4b5c6d7e8f done")
    assert n == "resolve <ref> done"


def test_empty_log_yields_nothing():
    assert logparse.parse("") == []
    assert logparse.parse("no spans here\njust text\n") == []


def test_aggregate_ranks_by_total_time_not_call_count():
    # One 400ms call matters more than 10k x 0.01ms.
    spans = [logparse.Span("slow_once", "lg", 400.0)]
    spans += [logparse.Span("fast_often", "lg", 0.01) for _ in range(10_000)]
    agg = logparse.aggregate(spans)
    assert list(agg)[0] == "slow_once"
    assert agg["fast_often"]["count"] == 10_000


def test_aggregate_stats():
    spans = [logparse.Span("s", "lg", v) for v in (10.0, 20.0, 30.0)]
    agg = logparse.aggregate(spans)["s"]
    assert agg["count"] == 3
    assert agg["total_ms"] == 60.0
    assert agg["mean_ms"] == 20.0
    assert agg["median_ms"] == 20.0
    assert agg["max_ms"] == 30.0


def test_aggregate_keeps_only_top_n():
    spans = [logparse.Span(f"s{i}", "lg", float(i)) for i in range(100)]
    assert len(logparse.aggregate(spans, top=5)) == 5


def test_parse_slice_reads_only_the_requested_bytes(tmp_path):
    # This is what keeps one workload's spans out of another's on a shared server.
    p = tmp_path / "server.log"
    p.write_text(REAL_LOG)

    first_line_end = REAL_LOG.index("\n") + 1
    mark = len(REAL_LOG[:first_line_end].encode())

    assert len(logparse.parse_slice(p, 0, len(REAL_LOG.encode()))) == 3
    assert len(logparse.parse_slice(p, mark, mark)) == 0  # empty slice


def test_parse_slice_on_missing_file_is_empty(tmp_path):
    assert logparse.parse_slice(tmp_path / "nope.log", 0, 100) == []


def test_log_size_of_missing_file_is_zero(tmp_path):
    assert logparse.log_size(tmp_path / "nope.log") == 0


def test_malformed_elapsed_is_skipped():
    assert logparse.parse("2026-01-01 00:00:00,0 DEBUG lg ← x (abc ms)\n") == []

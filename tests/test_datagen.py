"""The properties datagen must hold, because violating them corrupts results
silently rather than loudly."""

from __future__ import annotations

from acqbench import datagen


def test_windows_never_overlap_across_repetitions():
    # Storage dedups on (ref_uri, ts). Overlapping windows would turn inserts
    # into upserts and the write benchmark would measure the wrong path.
    rows = 10_000
    windows = [datagen.window_for(rep, rows) for rep in range(50)]
    windows.sort(key=lambda w: w.start)
    for a, b in zip(windows, windows[1:]):
        assert a.end <= b.start, f"window {a} overlaps {b}"


def test_windows_never_overlap_across_slots():
    # Two workloads in the same cell (json and arrow) must not collide either.
    a = datagen.window_for(0, 10_000, slot=0)
    b = datagen.window_for(0, 10_000, slot=1)
    assert a.end <= b.start or b.end <= a.start


def test_window_end_matches_row_count():
    w = datagen.window_for(3, 100)
    span = (w.end - w.start) / datagen.SAMPLE_INTERVAL
    assert span == 100


def test_generation_is_deterministic():
    # Two refs must see byte-identical input or the delta measures the data.
    names = datagen.stream_names(5)
    w = datagen.window_for(1, 10)
    assert datagen.generate("src", names, w) == datagen.generate("src", names, w)


def test_numeric_values_are_stable_and_bounded():
    v = [datagen.numeric_value(si, ri) for si in range(5) for ri in range(20)]
    assert all(isinstance(x, float) for x in v)
    assert all(20.0 <= x <= 85.0 for x in v)
    assert datagen.numeric_value(2, 7) == datagen.numeric_value(2, 7)


def test_stream_names_are_unique_and_sorted():
    names = datagen.stream_names(1000)
    assert len(set(names)) == 1000
    assert names == sorted(names)  # zero-padded, so lexical order == numeric


def test_generate_shape():
    names = datagen.stream_names(3)
    w = datagen.window_for(0, 7)
    data = datagen.generate("src", names, w)
    assert len(data) == 3
    assert all(len(rows) == 7 for rows in data.values())
    assert datagen.total_rows(data) == 21


def test_generated_timestamps_are_tz_aware():
    # A naive ts raises server-side on the Arrow path.
    data = datagen.generate("src", datagen.stream_names(1), datagen.window_for(0, 3))
    for rows in data.values():
        assert all(ts.tzinfo is not None for ts, _ in rows)


def test_text_values_cycle_known_states():
    v = {datagen.text_value(i, j) for i in range(4) for j in range(4)}
    assert v == {"ok", "warn", "alarm", "offline"}

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
from acquirium.Drivers.BuiltInDrivers.tabular_base import _safe_name
from acquirium.Drivers.BuiltInDrivers.csv_ingest import CSVIngestDriver

SOURCE_ID = "dpr-trailer"
DATE_COL = "Date"
CLOCK_COL = "Time"
DATE_FMT = "%m/%d/%Y %I:%M:%S %p"

_THIS_DIR = Path(__file__).resolve().parent
MODEL_PATH = _THIS_DIR / "union-dpr-model.ttl"
COLUMN_LOOKUP_PATH = _THIS_DIR / "property_to_column.json"


def _build_stream_bindings(column_lookup: Path) -> list[dict[str, object]]:
    """Read a ``{point_uri: ref_name}`` JSON map and return a ``register_streams`` payload."""
    with open(column_lookup) as f:
        mapping: dict[str, str] = json.load(f)
    return [
        {"point_uri": point_uri, "source_id": SOURCE_ID, "ref_name": _safe_name(ref_name)}
        for point_uri, ref_name in mapping.items()
    ]


class DPRTrailerCSVDriver(CSVIngestDriver):
    """CSV driver for DPR trailer SCADA exports.

    File shape:
    - zero or more metadata banner lines, then the CSV header — the header is
      located by content via ``header_contains_for``, so files with and
      without a banner both work
    - ``Date`` column: MM/DD/YYYY string
    - ``Time`` column: HH:MM:SS AM/PM string
    - remaining columns: wide-format numeric streams

    ``read_frame`` combines the split timestamp into a UTC datetime series and
    returns a normalized ``(ts, ref_name, value)`` frame directly, so none of
    the config hooks (``time_col``, ``ingest_format``, ``date_format``) need
    
    to be overridden.

    The only TOML keys needed are ``watch_dir`` and ``interval``.
    """

    def setup(self) -> None:
        super().setup()
        # insert_graph gained a required keyword-only source_id in later
        # revisions; probe the signature so one script drives every branch.
        import inspect
        kw = {}
        if "source_id" in inspect.signature(self.aq.insert_graph).parameters:
            kw["source_id"] = SOURCE_ID
        self.aq.insert_graph(str(MODEL_PATH), replace=False, **kw)
        self.source_id = SOURCE_ID

        self._register_streams_from_lookup()

    def on_graph_change(self) -> None:
        # Re-register stream bindings whenever someone else mutates the graph,
        # so point_uri ↔ ref_uri mappings survive model replacements. The
        # registered-version guard prevents a feedback loop on our own write.
        print("on_graph_change fired", flush=True)
        self._register_streams_from_lookup()

    def _register_streams_from_lookup(self) -> None:
        streams = _build_stream_bindings(column_lookup=COLUMN_LOOKUP_PATH)
        if streams:
            self.aq.register_streams(streams)
        self._last_registered_version = self.aq.graph_version()

    def header_contains_for(self, path: Path) -> tuple[str, ...]:
        # Exports sometimes carry a metadata banner before the header and
        # sometimes don't; find the header by content, not by row position.
        return (DATE_COL, CLOCK_COL)

    def source_id_for(self, path: Path) -> str:
        # All files in the watch directory belong to one trailer, so they share
        # a single datasource rather than getting one datasource per file path.
        return self.source_id

    def read_frame(self, path: Path, row_offset: int = 0) -> tuple[pl.DataFrame, int]:
        # Force both timestamp columns to string so Polars doesn't coerce them
        # to Date/Time types before we can concatenate them.
        df = self.read_df(path, row_offset, schema_overrides={DATE_COL: pl.Utf8, CLOCK_COL: pl.Utf8})
        rows_read = len(df)
        if rows_read == 0:
            return df, 0

        for col in (DATE_COL, CLOCK_COL):
            if col not in df.columns:
                raise ValueError(f"column '{col}' not found in {df.columns}")

        date_expr = pl.col(DATE_COL)
        if df[DATE_COL].dtype == pl.Date:
            # Polars can coerce the column to Date despite schema_overrides on
            # some code paths; reformat it to the string shape DATE_FMT expects.
            date_expr = date_expr.dt.strftime("%m/%d/%Y")
        else:
            date_expr = date_expr.cast(pl.String)

        combined_ts = self.normalize_timestamps(
            df.with_columns(
                pl.concat_str([date_expr, pl.lit(" "), pl.col(CLOCK_COL).cast(pl.String)]).alias("__ts")
            )["__ts"],
            date_format=DATE_FMT,
        )

        # Vectorised wide→long reshape: a pure-Python double loop over
        # ~261 cols × ~3000 rows × 21 files (~16M iterations) holds the
        # GIL long enough to starve the asyncio loop, making /health
        # hang and Ctrl-C undeliverable. Cast to Utf8 up-front so unpivot
        # has a single supertype for mixed-dtype stream columns; downstream
        # normalize_observations would cast to Utf8 anyway.
        stream_cols = [c for c in df.columns if c not in (DATE_COL, CLOCK_COL)]
        wide = df.with_columns(combined_ts.alias("ts")).select(
            ["ts", *[pl.col(c).cast(pl.Utf8, strict=False).alias(c) for c in stream_cols]]
        )
        long = (
            wide.unpivot(on=stream_cols, index="ts", variable_name="ref_name", value_name="value")
                .drop_nulls(subset=["ts", "value"])
        )
        return long, rows_read

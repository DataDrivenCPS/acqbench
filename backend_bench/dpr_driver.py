"""DPR trailer SCADA CSV driver for acquirium main (2026-08, post-#77).

Subclasses the built-in CSVIngestDriver: the config carries the file shape
(wide, Date+Time split timestamp, header located by content) and this class
only adds the deployment model and the point_uri bindings. value_kind is
inferred by the framework from the first observed values.
"""
from __future__ import annotations

import json
from pathlib import Path

from acquirium.Drivers.BuiltInDrivers.csv_ingest import CSVIngestDriver

SOURCE_ID = "dpr-trailer"
_HERE = Path(__file__).resolve().parent
MODEL_PATH = _HERE / "union-dpr-model.ttl"
LOOKUP_PATH = _HERE / "property_to_column.json"


class DPRDriver(CSVIngestDriver):
    def setup(self) -> None:
        super().setup()
        with open(LOOKUP_PATH) as f:
            # point_uri -> column name; invert to column -> point_uri
            self._point_for = {col: uri for uri, col in json.load(f).items()}
        self.aq.insert_graph_file(str(MODEL_PATH), replace=False, source_id=SOURCE_ID)

    def declare_stream(self, ref_name: str) -> None:
        self.declare(ref_name, point_uri=self._point_for.get(ref_name))

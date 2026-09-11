"""Schema and fixed-width column ranges for ghcnd-stations.txt."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Character ranges are 1-indexed inclusive as in the GHCN README / brief.
# substring() in Spark is 1-indexed with a length argument.
STATION_COLUMNS = [
    # (name, start_1idx, end_1idx, spark_type, description)
    ("ID", 1, 11, StringType(), "Station identification code"),
    ("LATITUDE", 13, 20, DoubleType(), "Latitude in decimal degrees"),
    ("LONGITUDE", 22, 30, DoubleType(), "Longitude in decimal degrees"),
    ("ELEVATION", 32, 37, DoubleType(), "Elevation in metres (−999.9 = missing)"),
    ("STATE", 39, 40, StringType(), "US/Canada state or province code"),
    ("NAME", 42, 71, StringType(), "Station name"),
    ("GSN_FLAG", 73, 75, StringType(), "GSN membership flag"),
    ("HCN_CRN_FLAG", 77, 79, StringType(), "HCN or CRN membership flag"),
    ("WMO_ID", 81, 85, StringType(), "WMO station number"),
]


def station_schema() -> StructType:
    return StructType(
        [StructField(name, dtype, True) for name, _, _, dtype, _ in STATION_COLUMNS]
    )

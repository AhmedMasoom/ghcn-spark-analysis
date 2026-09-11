"""Schema and fixed-width ranges for ghcnd-inventory.txt."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

INVENTORY_COLUMNS = [
    ("ID", 1, 11, StringType(), "Station identification code"),
    ("LATITUDE", 13, 20, DoubleType(), "Latitude in decimal degrees"),
    ("LONGITUDE", 22, 30, DoubleType(), "Longitude in decimal degrees"),
    ("ELEMENT", 32, 35, StringType(), "Element type code"),
    ("FIRSTYEAR", 37, 40, IntegerType(), "First year of unflagged data"),
    ("LASTYEAR", 42, 45, IntegerType(), "Last year of unflagged data"),
]


def inventory_schema() -> StructType:
    return StructType(
        [StructField(name, dtype, True) for name, _, _, dtype, _ in INVENTORY_COLUMNS]
    )

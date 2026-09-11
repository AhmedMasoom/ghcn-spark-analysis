"""Schemas for countries and states metadata tables."""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

COUNTRY_COLUMNS = [
    ("CODE", 1, 2, StringType(), "FIPS country code"),
    ("NAME", 4, 64, StringType(), "Country or territory name"),
]

STATE_COLUMNS = [
    ("CODE", 1, 2, StringType(), "State / province postal code"),
    ("NAME", 4, 50, StringType(), "State / province name"),
]


def country_schema() -> StructType:
    return StructType(
        [StructField(name, dtype, True) for name, _, _, dtype, _ in COUNTRY_COLUMNS]
    )


def state_schema() -> StructType:
    return StructType(
        [StructField(name, dtype, True) for name, _, _, dtype, _ in STATE_COLUMNS]
    )

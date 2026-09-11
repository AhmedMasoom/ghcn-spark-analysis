"""Schema definitions for GHCN daily CSV files."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


def daily_schema() -> StructType:
    """Schema used to load daily CSV.gz files.

    DATE and OBSERVATION_TIME are kept as strings:
    - DATE is YYYYMMDD — filtering by year is a cheap substring, and to_date()
      can be applied later when calendar arithmetic is needed.
    - OBSERVATION_TIME is HHMM and frequently blank; StringType avoids parse
      failures on empty fields while still allowing casting when populated.
    """
    return StructType(
        [
            StructField("ID", StringType(), True),
            StructField("DATE", StringType(), True),
            StructField("ELEMENT", StringType(), True),
            StructField("VALUE", DoubleType(), True),
            StructField("MEASUREMENT_FLAG", StringType(), True),
            StructField("QUALITY_FLAG", StringType(), True),
            StructField("SOURCE_FLAG", StringType(), True),
            StructField("OBSERVATION_TIME", StringType(), True),
        ]
    )


# Alias matching the starter notebook's shorter flag names if needed.
def daily_schema_short_flags() -> StructType:
    return StructType(
        [
            StructField("ID", StringType(), True),
            StructField("DATE", StringType(), True),
            StructField("ELEMENT", StringType(), True),
            StructField("VALUE", DoubleType(), True),
            StructField("MEASUREMENT", StringType(), True),
            StructField("QUALITY", StringType(), True),
            StructField("SOURCE", StringType(), True),
            StructField("TIME", StringType(), True),
        ]
    )

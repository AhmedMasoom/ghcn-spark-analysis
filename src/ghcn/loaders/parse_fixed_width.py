"""Fixed-width text parsing helpers for GHCN metadata tables."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DoubleType, IntegerType, StringType


def _substring_expr(start_1idx: int, end_1idx: int):
    """Spark substring for a 1-indexed inclusive character range."""
    length = end_1idx - start_1idx + 1
    return F.substring(F.col("value"), start_1idx, length)


def parse_fixed_width(
    text_df: DataFrame,
    columns: Sequence[tuple],
) -> DataFrame:
    """Parse a spark.read.text DataFrame using GHCN-style column specs.

    Each entry in ``columns`` is
    ``(name, start_1idx, end_1idx, spark_type, description)``.
    Whitespace-only fields become null; numeric fields are cast.
    """
    exprs = []
    for name, start, end, dtype, _desc in columns:
        raw = F.trim(_substring_expr(start, end))
        typed = _cast_field(raw, dtype)
        exprs.append(typed.alias(name))
    return text_df.select(*exprs)


def _cast_field(raw_col, dtype: DataType):
    blank_as_null = F.when(raw_col == "", F.lit(None)).otherwise(raw_col)
    if isinstance(dtype, StringType):
        return blank_as_null.cast(StringType())
    if isinstance(dtype, DoubleType):
        return blank_as_null.cast(DoubleType())
    if isinstance(dtype, IntegerType):
        return blank_as_null.cast(IntegerType())
    return blank_as_null.cast(dtype)

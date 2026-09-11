"""Save enriched / aggregated outputs to the user container."""

from __future__ import annotations

from pyspark.sql import DataFrame


def save_parquet(
    df: DataFrame,
    path: str,
    *,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    coalesce: int | None = None,
) -> str:
    """Write a DataFrame as Parquet (preferred for enriched stations).

    Parquet is columnar, compressed, and preserves types — better than CSV
    for repeated analytical reads. Optional partitioning (e.g. by COUNTRY_CODE)
    keeps later country filters selective.
    """
    writer_df = df.coalesce(coalesce) if coalesce else df
    writer = writer_df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
    return path


def save_csv_gz(
    df: DataFrame,
    path: str,
    *,
    mode: str = "overwrite",
    header: bool = True,
    coalesce: int | None = 1,
) -> str:
    """Write a small aggregated result as gzip-compressed CSV."""
    writer_df = df.coalesce(coalesce) if coalesce else df
    (
        writer_df.write.mode(mode)
        .option("header", str(header).lower())
        .option("compression", "gzip")
        .csv(path)
    )
    return path


def load_parquet(spark, path: str) -> DataFrame:
    return spark.read.parquet(path)

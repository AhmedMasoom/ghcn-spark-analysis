"""Load GHCN daily CSV.gz files into Spark."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ghcn.config.config import MOST_RECENT_DAILY_YEAR
from ghcn.config.paths import Paths, default_paths
from ghcn.schemas.daily_schema import daily_schema


def load_daily(
    spark: SparkSession,
    path: str | None = None,
    paths: Paths | None = None,
) -> DataFrame:
    """Load daily data from a path (single year or whole directory).

    Never cache or collect the result of loading the full daily directory.
    """
    paths = paths or default_paths()
    target = path or paths.daily_dir
    return spark.read.csv(path=target, schema=daily_schema())


def load_daily_year(
    spark: SparkSession,
    year: int = MOST_RECENT_DAILY_YEAR,
    paths: Paths | None = None,
) -> DataFrame:
    paths = paths or default_paths()
    return load_daily(spark, path=paths.daily_year(year), paths=paths)


def with_year_column(daily: DataFrame, date_col: str = "DATE") -> DataFrame:
    """Add an integer YEAR column derived from YYYYMMDD strings."""
    return daily.withColumn("YEAR", F.substring(F.col(date_col), 1, 4).cast("int"))


def filter_quality_ok(daily: DataFrame) -> DataFrame:
    """Keep rows that did not fail a quality-assurance check (blank QFLAG)."""
    return daily.where(
        (F.col("QUALITY_FLAG").isNull()) | (F.trim(F.col("QUALITY_FLAG")) == "")
    )

"""Daily observation analysis for Analysis Q3."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ghcn.loaders.load_daily import with_year_column
from ghcn.utils.constants import CORE_ELEMENTS


def active_stations_in_year(daily: DataFrame, year: int) -> int:
    """Distinct stations with any observation in ``year`` based on daily."""
    d = with_year_column(daily) if "YEAR" not in daily.columns else daily
    return d.where(F.col("YEAR") == year).select("ID").distinct().count()


def core_element_observations(daily: DataFrame) -> DataFrame:
    """Filter to the five core elements and count rows per element."""
    return (
        daily.where(F.col("ELEMENT").isin(list(CORE_ELEMENTS)))
        .groupBy("ELEMENT")
        .agg(F.count(F.lit(1)).alias("N_OBSERVATIONS"))
        .orderBy(F.col("N_OBSERVATIONS").desc())
    )


def tmax_without_tmin(daily: DataFrame) -> tuple[DataFrame, int, int]:
    """Count TMAX observations that have no matching TMIN on the same station-day.

    Method: self-join (or left-anti) of TMAX against TMIN on (ID, DATE).
    This is efficient because we filter to two elements before joining, rather
    than scanning all of daily for every element.
    """
    tmax = (
        daily.where(F.col("ELEMENT") == "TMAX")
        .select("ID", "DATE")
        .withColumnRenamed("ID", "ID_X")
        .withColumnRenamed("DATE", "DATE_X")
    )
    tmin = (
        daily.where(F.col("ELEMENT") == "TMIN")
        .select("ID", "DATE")
        .withColumnRenamed("ID", "ID_N")
        .withColumnRenamed("DATE", "DATE_N")
    )
    missing = tmax.join(
        tmin,
        (tmax.ID_X == tmin.ID_N) & (tmax.DATE_X == tmin.DATE_N),
        how="left_anti",
    ).withColumnRenamed("ID_X", "ID").withColumnRenamed("DATE_X", "DATE")

    n_obs = missing.count()
    n_stations = missing.select("ID").distinct().count()
    return missing, n_obs, n_stations

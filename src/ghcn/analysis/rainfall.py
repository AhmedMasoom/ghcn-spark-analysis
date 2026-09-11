"""Rainfall aggregation for Visualizations Q2."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ghcn.loaders.load_daily import with_year_column


def average_daily_rainfall_by_year_country(
    daily: DataFrame,
    stations: DataFrame,
) -> DataFrame:
    """Average daily PRCP (mm) per year and country.

    VALUE for PRCP is stored in tenths of mm (GHCN README), so we divide by 10.
    Average is over observation rows (station-days with a PRCP report), which
    matches "average daily rainfall" at the resolution of available reports.
    """
    if "COUNTRY_CODE" not in stations.columns:
        stations = stations.withColumn("COUNTRY_CODE", F.substring(F.col("ID"), 1, 2))

    station_country = stations.select("ID", "COUNTRY_CODE").distinct()
    if "COUNTRY_NAME" in stations.columns:
        station_country = stations.select("ID", "COUNTRY_CODE", "COUNTRY_NAME").distinct()

    prcp = (
        with_year_column(daily.where(F.col("ELEMENT") == "PRCP"))
        .withColumn("PRCP_MM", F.col("VALUE") / F.lit(10.0))
        .join(station_country, on="ID", how="inner")
    )

    group_cols = ["YEAR", "COUNTRY_CODE"]
    if "COUNTRY_NAME" in prcp.columns:
        group_cols.append("COUNTRY_NAME")

    return (
        prcp.groupBy(*group_cols)
        .agg(
            F.avg("PRCP_MM").alias("AVG_DAILY_PRCP_MM"),
            F.count(F.lit(1)).alias("N_OBSERVATIONS"),
            F.countDistinct("ID").alias("N_STATIONS"),
        )
        .orderBy("YEAR", "COUNTRY_CODE")
    )


def rainfall_descriptive_stats(rainfall: DataFrame) -> DataFrame:
    return rainfall.select("AVG_DAILY_PRCP_MM").summary(
        "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
    )


def highest_rainfall_year_country(rainfall: DataFrame) -> dict:
    row = rainfall.orderBy(F.col("AVG_DAILY_PRCP_MM").desc()).limit(1).collect()[0]
    return row.asDict()


def rainfall_for_year(rainfall: DataFrame, year: int) -> DataFrame:
    return rainfall.where(F.col("YEAR") == year)

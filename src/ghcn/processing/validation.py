"""Validation helpers — missing-station checks without scanning all of daily."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def distinct_station_ids(df: DataFrame, id_col: str = "ID") -> DataFrame:
    return df.select(id_col).distinct()


def stations_missing_from_daily(
    stations: DataFrame,
    daily_ids: DataFrame,
) -> DataFrame:
    """Stations present in stations but absent from the provided daily ID set.

    Prefer passing distinct IDs extracted from daily (or even from inventory)
    rather than joining the full daily fact table.
    """
    s = stations.select("ID").distinct().withColumnRenamed("ID", "STATION_ID")
    d = daily_ids.select("ID").distinct()
    return (
        s.join(d, s.STATION_ID == d.ID, how="left_anti")
        .withColumnRenamed("STATION_ID", "ID")
    )


def daily_ids_missing_from_stations(
    daily_ids: DataFrame,
    stations: DataFrame,
) -> DataFrame:
    """Station IDs observed in daily that are absent from the stations table."""
    d = daily_ids.select("ID").distinct().withColumnRenamed("ID", "DAILY_ID")
    s = stations.select("ID").distinct()
    return (
        d.join(s, d.DAILY_ID == s.ID, how="left_anti")
        .withColumnRenamed("DAILY_ID", "ID")
    )


def left_join_daily_subset_stations(
    daily_subset: DataFrame,
    stations: DataFrame,
) -> DataFrame:
    """LEFT JOIN a daily subset onto stations (Processing Q4a demonstration)."""
    return daily_subset.join(stations, on="ID", how="left")


def efficient_daily_station_ids(daily: DataFrame) -> DataFrame:
    """Distinct station IDs from daily — far cheaper than joining all rows."""
    return daily.select("ID").distinct()

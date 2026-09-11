"""Join helpers for stations, countries, and states."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_country_code(stations: DataFrame) -> DataFrame:
    """Extract the 2-character FIPS country code from the station ID."""
    return stations.withColumn("COUNTRY_CODE", F.substring(F.col("ID"), 1, 2))


def join_countries(stations: DataFrame, countries: DataFrame) -> DataFrame:
    """LEFT JOIN stations to countries on COUNTRY_CODE = CODE."""
    c = countries.select(
        F.col("CODE").alias("COUNTRY_CODE"),
        F.col("NAME").alias("COUNTRY_NAME"),
    )
    return stations.join(c, on="COUNTRY_CODE", how="left")


def join_states(stations: DataFrame, states: DataFrame) -> DataFrame:
    """LEFT JOIN stations to states on STATE = CODE."""
    s = states.select(
        F.col("CODE").alias("STATE"),
        F.col("NAME").alias("STATE_NAME"),
    )
    return stations.join(s, on="STATE", how="left")


def countries_with_state_codes(stations_with_states: DataFrame) -> DataFrame:
    """Countries that have at least one station with a non-null state code."""
    return (
        stations_with_states.where(
            F.col("STATE").isNotNull() & (F.trim(F.col("STATE")) != "")
        )
        .select("COUNTRY_CODE", "COUNTRY_NAME")
        .distinct()
        .orderBy("COUNTRY_CODE")
    )

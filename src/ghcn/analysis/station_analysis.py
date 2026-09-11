"""Station-level counts for Analysis Q1."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ghcn.utils.constants import NZ_COUNTRY_CODE, US_COUNTRY_CODE


def total_stations(stations: DataFrame) -> int:
    return stations.select("ID").distinct().count()


def network_counts(stations: DataFrame) -> dict[str, int]:
    """Counts for GSN, HCN, CRN and pairwise overlaps."""
    gsn = F.trim(F.coalesce(F.col("GSN_FLAG"), F.lit(""))) == "GSN"
    hcn = F.trim(F.coalesce(F.col("HCN_CRN_FLAG"), F.lit(""))) == "HCN"
    crn = F.trim(F.coalesce(F.col("HCN_CRN_FLAG"), F.lit(""))) == "CRN"

    row = stations.agg(
        F.sum(gsn.cast("int")).alias("GSN"),
        F.sum(hcn.cast("int")).alias("HCN"),
        F.sum(crn.cast("int")).alias("CRN"),
        F.sum((gsn & hcn).cast("int")).alias("GSN_and_HCN"),
        F.sum((gsn & crn).cast("int")).alias("GSN_and_CRN"),
        F.sum((hcn & crn).cast("int")).alias("HCN_and_CRN"),
        F.sum((gsn & hcn & crn).cast("int")).alias("GSN_and_HCN_and_CRN"),
        F.sum(((gsn & hcn) | (gsn & crn) | (hcn & crn)).cast("int")).alias(
            "IN_MORE_THAN_ONE"
        ),
    ).collect()[0]

    return {k: int(row[k] or 0) for k in row.asDict()}


def southern_hemisphere_count(stations: DataFrame) -> int:
    return stations.where(F.col("LATITUDE") < 0).count()


def us_territory_stations(stations: DataFrame) -> DataFrame:
    """Stations in US territories (country name mentions United States, code ≠ US)."""
    name_col = "COUNTRY_NAME" if "COUNTRY_NAME" in stations.columns else "NAME"
    code_col = "COUNTRY_CODE" if "COUNTRY_CODE" in stations.columns else None

    cond = F.lower(F.col(name_col)).contains("united states")
    if code_col:
        cond = cond & (F.col(code_col) != US_COUNTRY_CODE)
    else:
        # Fallback: exclude exact mainland name if only countries table joined differently
        cond = cond & (F.lower(F.col(name_col)) != "united states")
    return stations.where(cond)


def us_territory_count(stations: DataFrame) -> int:
    return us_territory_stations(stations).select("ID").distinct().count()


def stations_per_country(stations: DataFrame, countries: DataFrame) -> DataFrame:
    """Join per-country station counts onto the countries table."""
    if "COUNTRY_CODE" in stations.columns:
        counts = stations.groupBy("COUNTRY_CODE").agg(F.countDistinct("ID").alias("N_STATIONS"))
    else:
        counts = (
            stations.withColumn("COUNTRY_CODE", F.substring(F.col("ID"), 1, 2))
            .groupBy("COUNTRY_CODE")
            .agg(F.countDistinct("ID").alias("N_STATIONS"))
        )
    return countries.join(
        counts,
        countries.CODE == counts.COUNTRY_CODE,
        how="left",
    ).select(
        countries.CODE,
        countries.NAME,
        F.coalesce(counts.N_STATIONS, F.lit(0)).alias("N_STATIONS"),
    )


def new_zealand_count(stations: DataFrame) -> int:
    if "COUNTRY_CODE" in stations.columns:
        return stations.where(F.col("COUNTRY_CODE") == NZ_COUNTRY_CODE).count()
    return stations.where(F.substring(F.col("ID"), 1, 2) == NZ_COUNTRY_CODE).count()


def nz_stations(stations: DataFrame) -> DataFrame:
    if "COUNTRY_CODE" in stations.columns:
        return stations.where(F.col("COUNTRY_CODE") == NZ_COUNTRY_CODE)
    return stations.where(F.substring(F.col("ID"), 1, 2) == NZ_COUNTRY_CODE)

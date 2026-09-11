"""Build the enriched stations table from stations + inventory metrics."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ghcn.config.config import ACTIVE_YEAR
from ghcn.processing.joins import (
    add_country_code,
    countries_with_state_codes,
    join_countries,
    join_states,
)
from ghcn.utils.constants import CORE_ELEMENTS


def inventory_station_metrics(
    inventory: DataFrame,
    active_year: int = ACTIVE_YEAR,
) -> DataFrame:
    """Per-station activity window and element-set summaries from inventory.

    Returns one row per station with:
      FIRST_YEAR, LAST_YEAR, ELEMENTS, N_CORE_ELEMENTS, N_OTHER_ELEMENTS,
      HAS_ALL_CORE, PRECIP_ONLY, ACTIVE_IN_<year>
    """
    core = list(CORE_ELEMENTS)

    agg = inventory.groupBy("ID").agg(
        F.min("FIRSTYEAR").alias("FIRST_YEAR"),
        F.max("LASTYEAR").alias("LAST_YEAR"),
        F.collect_set("ELEMENT").alias("ELEMENTS"),
    )

    # Core / other counts from the collected element set.
    n_core = sum(
        F.when(F.array_contains(F.col("ELEMENTS"), e), 1).otherwise(0) for e in core
    )
    n_elements = F.size(F.col("ELEMENTS"))

    return (
        agg.withColumn("N_CORE_ELEMENTS", n_core)
        .withColumn("N_OTHER_ELEMENTS", n_elements - F.col("N_CORE_ELEMENTS"))
        .withColumn(
            "HAS_ALL_CORE",
            (F.col("N_CORE_ELEMENTS") == len(core)).cast("boolean"),
        )
        .withColumn(
            "PRECIP_ONLY",
            (
                (n_elements == 1)
                & F.array_contains(F.col("ELEMENTS"), F.lit("PRCP"))
            ).cast("boolean"),
        )
        .withColumn(
            f"ACTIVE_IN_{active_year}",
            (
                (F.col("FIRST_YEAR") <= active_year)
                & (F.col("LAST_YEAR") >= active_year)
            ).cast("boolean"),
        )
    )


def build_enriched_stations(
    stations: DataFrame,
    countries: DataFrame,
    states: DataFrame,
    inventory: DataFrame,
    active_year: int = ACTIVE_YEAR,
) -> DataFrame:
    """Processing Q3: join country/state metadata with inventory metrics."""
    s = add_country_code(stations)
    s = join_countries(s, countries)
    s = join_states(s, states)
    metrics = inventory_station_metrics(inventory, active_year=active_year)
    return s.join(metrics, on="ID", how="left")


def count_active_in_year(enriched: DataFrame, year: int = ACTIVE_YEAR) -> int:
    col = f"ACTIVE_IN_{year}"
    return enriched.where(F.col(col) == True).count()  # noqa: E712


def count_all_core(enriched: DataFrame) -> int:
    return enriched.where(F.col("HAS_ALL_CORE") == True).count()  # noqa: E712


def count_precip_only(enriched: DataFrame) -> int:
    return enriched.where(F.col("PRECIP_ONLY") == True).count()  # noqa: E712


def summarise_state_countries(stations_joined: DataFrame) -> list[str]:
    rows = countries_with_state_codes(stations_joined).collect()
    return [f"{r['COUNTRY_CODE']}: {r['COUNTRY_NAME']}" for r in rows]

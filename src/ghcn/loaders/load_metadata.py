"""Load GHCN fixed-width metadata tables into Spark DataFrames."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ghcn.config.paths import Paths, default_paths
from ghcn.loaders.parse_fixed_width import parse_fixed_width
from ghcn.schemas.inventory_schema import INVENTORY_COLUMNS
from ghcn.schemas.metadata_schema import COUNTRY_COLUMNS, STATE_COLUMNS
from ghcn.schemas.station_schema import STATION_COLUMNS


def _read_text(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.text(path)


def load_stations(spark: SparkSession, paths: Paths | None = None) -> DataFrame:
    paths = paths or default_paths()
    return parse_fixed_width(_read_text(spark, paths.stations), STATION_COLUMNS)


def load_countries(spark: SparkSession, paths: Paths | None = None) -> DataFrame:
    paths = paths or default_paths()
    return parse_fixed_width(_read_text(spark, paths.countries), COUNTRY_COLUMNS)


def load_states(spark: SparkSession, paths: Paths | None = None) -> DataFrame:
    paths = paths or default_paths()
    return parse_fixed_width(_read_text(spark, paths.states), STATE_COLUMNS)


def load_inventory(spark: SparkSession, paths: Paths | None = None) -> DataFrame:
    paths = paths or default_paths()
    return parse_fixed_width(_read_text(spark, paths.inventory), INVENTORY_COLUMNS)


def load_all_metadata(spark: SparkSession, paths: Paths | None = None) -> dict[str, DataFrame]:
    paths = paths or default_paths()
    return {
        "stations": load_stations(spark, paths),
        "countries": load_countries(spark, paths),
        "states": load_states(spark, paths),
        "inventory": load_inventory(spark, paths),
    }

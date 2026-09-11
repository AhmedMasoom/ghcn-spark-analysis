"""Spherical geographic distance (Haversine) for Analysis Q2."""

from __future__ import annotations

import math
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from ghcn.utils.constants import EARTH_RADIUS_KM


def haversine_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    radius_km: float = EARTH_RADIUS_KM,
) -> Optional[float]:
    """Great-circle distance between two WGS84 points (kilometres).

    Uses the Haversine formula on a sphere of radius ``radius_km``
    (default mean Earth radius 6371 km). Returns None if any coordinate
    is missing.

    Reference: Sinnott, R. W. (1984). Virtues of the Haversine.
    Sky and Telescope, 68(2), 159.
    """
    if None in (lat1, lon1, lat2, lon2):
        return None
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_km * c


def haversine_udf(radius_km: float = EARTH_RADIUS_KM):
    """Spark UDF wrapping :func:`haversine_km`."""

    def _f(lat1, lon1, lat2, lon2):
        return haversine_km(lat1, lon1, lat2, lon2, radius_km=radius_km)

    return F.udf(_f, DoubleType())


def pairwise_distances(
    stations: DataFrame,
    id_col: str = "ID",
    lat_col: str = "LATITUDE",
    lon_col: str = "LONGITUDE",
    exclude_self: bool = True,
    upper_triangle_only: bool = True,
) -> DataFrame:
    """CROSS JOIN stations with itself and attach Haversine distances.

    For New Zealand (~dozen–hundreds of stations) this is tractable.
    Do NOT apply to the full global stations table.
    """
    dist = haversine_udf()
    a = stations.select(
        F.col(id_col).alias("ID_A"),
        F.col(lat_col).alias("LAT_A"),
        F.col(lon_col).alias("LON_A"),
        F.col("NAME").alias("NAME_A") if "NAME" in stations.columns else F.lit(None).alias("NAME_A"),
    )
    b = stations.select(
        F.col(id_col).alias("ID_B"),
        F.col(lat_col).alias("LAT_B"),
        F.col(lon_col).alias("LON_B"),
        F.col("NAME").alias("NAME_B") if "NAME" in stations.columns else F.lit(None).alias("NAME_B"),
    )
    pairs = a.crossJoin(b)
    if exclude_self:
        pairs = pairs.where(F.col("ID_A") != F.col("ID_B"))
    if upper_triangle_only:
        pairs = pairs.where(F.col("ID_A") < F.col("ID_B"))
    return pairs.withColumn(
        "DISTANCE_KM",
        dist(F.col("LAT_A"), F.col("LON_A"), F.col("LAT_B"), F.col("LON_B")),
    )


def closest_pair(distances: DataFrame) -> dict:
    """Return the geographically closest station pair as a dict."""
    row = distances.orderBy(F.col("DISTANCE_KM").asc()).limit(1).collect()[0]
    return row.asDict()

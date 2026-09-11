"""New Zealand TMIN/TMAX time-series and station map visualizations."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ghcn.loaders.load_daily import with_year_column
from ghcn.utils.constants import NZ_COUNTRY_CODE
from ghcn.visualization.helpers import save_figure, style_axes


def nz_tmin_tmax(
    daily: DataFrame,
    stations: DataFrame,
) -> DataFrame:
    """Filter daily to NZ stations and TMIN/TMAX only."""
    if "COUNTRY_CODE" in stations.columns:
        nz_ids = stations.where(F.col("COUNTRY_CODE") == NZ_COUNTRY_CODE).select("ID", "NAME")
    else:
        nz_ids = (
            stations.where(F.substring(F.col("ID"), 1, 2) == NZ_COUNTRY_CODE)
            .select("ID", "NAME")
        )
    return (
        daily.where(F.col("ELEMENT").isin("TMIN", "TMAX"))
        .join(nz_ids, on="ID", how="inner")
        .withColumn("TEMP_C", F.col("VALUE") / F.lit(10.0))
    )


def monthly_station_series(nz_temps: DataFrame) -> DataFrame:
    """Aggregate to monthly means per station/element for smoother plots."""
    d = with_year_column(nz_temps)
    return (
        d.withColumn("MONTH", F.substring(F.col("DATE"), 5, 2).cast("int"))
        .withColumn(
            "YEAR_MONTH",
            F.to_date(F.concat(F.col("YEAR").cast("string"), F.lit("-"), F.lpad(F.col("MONTH").cast("string"), 2, "0"), F.lit("-01"))),
        )
        .groupBy("ID", "NAME", "ELEMENT", "YEAR_MONTH")
        .agg(F.avg("TEMP_C").alias("TEMP_C"))
        .orderBy("ID", "ELEMENT", "YEAR_MONTH")
    )


def country_monthly_average(nz_temps: DataFrame) -> DataFrame:
    """Country-wide monthly mean TMIN/TMAX across all NZ stations."""
    d = with_year_column(nz_temps)
    return (
        d.withColumn("MONTH", F.substring(F.col("DATE"), 5, 2).cast("int"))
        .withColumn(
            "YEAR_MONTH",
            F.to_date(
                F.concat(
                    F.col("YEAR").cast("string"),
                    F.lit("-"),
                    F.lpad(F.col("MONTH").cast("string"), 2, "0"),
                    F.lit("-01"),
                )
            ),
        )
        .groupBy("ELEMENT", "YEAR_MONTH")
        .agg(F.avg("TEMP_C").alias("TEMP_C"))
        .orderBy("ELEMENT", "YEAR_MONTH")
    )


def plot_station_subplots(
    monthly_pdf: pd.DataFrame,
    output_path: str | Path,
    ncols: int = 3,
) -> Path:
    """One subplot per station with TMIN/TMAX monthly series.

    Gaps (months with no observations) appear as breaks in the line because
    we do not forward-fill missing months.
    """
    stations = sorted(monthly_pdf["ID"].unique())
    n = len(stations)
    ncols = max(1, min(ncols, n))
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(4 * ncols, 2.8 * nrows), sharex=False)
    axes = [axes] if n == 1 else list(axes.ravel())

    for ax, sid in zip(axes, stations):
        sub = monthly_pdf[monthly_pdf["ID"] == sid]
        name = sub["NAME"].iloc[0] if "NAME" in sub.columns else sid
        for element, color in (("TMIN", "#1f77b4"), ("TMAX", "#d62728")):
            el = sub[sub["ELEMENT"] == element].sort_values("YEAR_MONTH")
            ax.plot(el["YEAR_MONTH"], el["TEMP_C"], label=element, color=color, linewidth=1.0)
        style_axes(ax, title=f"{sid}\n{name}", ylabel="°C")
        ax.legend(fontsize=7, loc="best")

    for ax in axes[n:]:
        ax.set_visible(False)

    fig.suptitle("New Zealand monthly mean TMIN / TMAX by station", fontsize=14, y=1.01)
    fig.tight_layout()
    return save_figure(fig, output_path)


def plot_country_average(
    country_pdf: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    fig, ax = plt.subplots(figsize=(14, 5))
    for element, color in (("TMIN", "#1f77b4"), ("TMAX", "#d62728")):
        el = country_pdf[country_pdf["ELEMENT"] == element].sort_values("YEAR_MONTH")
        ax.plot(el["YEAR_MONTH"], el["TEMP_C"], label=element, color=color, linewidth=1.4)
    style_axes(
        ax,
        title="New Zealand — country-average monthly mean TMIN / TMAX",
        xlabel="Date",
        ylabel="Temperature (°C)",
    )
    ax.legend()
    fig.tight_layout()
    return save_figure(fig, output_path)


def plot_nz_station_map(
    nz_stations_pdf: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    fig, ax = plt.subplots(figsize=(7, 9))
    ax.scatter(
        nz_stations_pdf["LONGITUDE"],
        nz_stations_pdf["LATITUDE"],
        s=40,
        c="#c4512a",
        edgecolors="white",
        linewidths=0.5,
        zorder=3,
    )
    for _, r in nz_stations_pdf.iterrows():
        ax.annotate(
            r.get("NAME", r["ID"]),
            (r["LONGITUDE"], r["LATITUDE"]),
            fontsize=6,
            xytext=(4, 4),
            textcoords="offset points",
        )
    style_axes(ax, title="GHCN stations in New Zealand", xlabel="Longitude", ylabel="Latitude")
    ax.set_aspect("equal", adjustable="box")
    fig.tight_layout()
    return save_figure(fig, output_path)


def gap_years(nz_temps: DataFrame) -> list[int]:
    """Years between first and last observation with zero NZ TMIN/TMAX rows."""
    years = [
        int(r.YEAR)
        for r in with_year_column(nz_temps).select("YEAR").distinct().orderBy("YEAR").collect()
    ]
    if not years:
        return []
    full = set(range(years[0], years[-1] + 1))
    return sorted(full - set(years))

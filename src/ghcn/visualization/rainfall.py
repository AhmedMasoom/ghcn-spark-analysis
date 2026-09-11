"""Rainfall summary plots (optional bar charts for the report)."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from ghcn.visualization.helpers import save_figure, style_axes


def plot_top_rainfall_years(
    rainfall_pdf: pd.DataFrame,
    output_path: str | Path,
    top_n: int = 20,
) -> Path:
    top = rainfall_pdf.nlargest(top_n, "AVG_DAILY_PRCP_MM").copy()
    label_col = "COUNTRY_NAME" if "COUNTRY_NAME" in top.columns else "COUNTRY_CODE"
    top["LABEL"] = top[label_col].astype(str) + " (" + top["YEAR"].astype(str) + ")"

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(top["LABEL"][::-1], top["AVG_DAILY_PRCP_MM"][::-1], color="#2a6f97")
    style_axes(
        ax,
        title=f"Top {top_n} country–year average daily rainfall",
        xlabel="Average daily PRCP (mm)",
    )
    fig.tight_layout()
    return save_figure(fig, output_path)

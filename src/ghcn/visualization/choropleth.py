"""Choropleth of average daily rainfall by country (Visualizations Q2b)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ghcn.visualization.helpers import save_figure


# Manual FIPS (GHCN) → ISO-3 overrides where pycountry lookup is unreliable.
FIPS_TO_ISO3_OVERRIDES = {
    "UK": "GBR",  # United Kingdom
    "US": "USA",
    "RS": "RUS",
    "GM": "DEU",
    "JA": "JPN",
    "KS": "KOR",
    "TW": "TWN",
    "VM": "VNM",
    "RI": "IDN",
    "BK": "BIH",
    "HR": "HRV",
    "LO": "SVK",
    "EZ": "CZE",
    "PO": "PRT",
    "SP": "ESP",
    "SW": "SWE",
    "SZ": "CHE",
    "AU": "AUS",
    "NZ": "NZL",
    "CI": "CHL",
    "AR": "ARG",
    "BR": "BRA",
    "CA": "CAN",
    "MX": "MEX",
    "IN": "IND",
    "CH": "CHN",
    "FR": "FRA",
    "IT": "ITA",
    "NL": "NLD",
    "BE": "BEL",
    "PL": "POL",
    "UP": "UKR",
    "BO": "BLR",
    "EN": "EST",
    "LG": "LVA",
    "LH": "LTU",
    "FI": "FIN",
    "NO": "NOR",
    "IC": "ISL",
    "EI": "IRL",
    "DA": "DNK",
    "GR": "GRC",
    "TU": "TUR",
    "IS": "ISR",
    "EG": "EGY",
    "SF": "ZAF",
    "AE": "ARE",
    "SA": "SAU",
    "TH": "THA",
    "MY": "MYS",
    "SN": "SGP",
    "RP": "PHL",
    "PP": "PNG",
    "FJ": "FJI",
    "BQ": "NAV",  # Navassa — often unmatched
}


def fips_to_iso3(code: str) -> str | None:
    """Map a GHCN/FIPS country code to ISO-3166 alpha-3 for map libraries."""
    if not code:
        return None
    code = code.strip().upper()
    if code in FIPS_TO_ISO3_OVERRIDES:
        return FIPS_TO_ISO3_OVERRIDES[code]
    try:
        import pycountry

        # Try historic FIPS via pycountry if available
        for attr in ("alpha_2",):
            hit = pycountry.countries.get(**{attr: code})
            if hit:
                return hit.alpha_3
    except Exception:
        pass
    return None


def prepare_choropleth_frame(rainfall_year_pdf: pd.DataFrame) -> pd.DataFrame:
    df = rainfall_year_pdf.copy()
    df["ISO3"] = df["COUNTRY_CODE"].map(fips_to_iso3)
    return df


def plot_choropleth_plotly(
    rainfall_year_pdf: pd.DataFrame,
    output_html: str | Path,
    output_png: str | Path | None = None,
    year: int = 2025,
    color_col: str = "AVG_DAILY_PRCP_MM",
):
    """Interactive Plotly choropleth using natural-earth ISO-3 geometries.

    Uses a sequential blue colour scale (suitable for rainfall amounts) and
    the equirectangular / natural-earth projection Plotly defaults to for
    country choropleths. Unmatched countries are reported separately.
    """
    import plotly.express as px

    df = prepare_choropleth_frame(rainfall_year_pdf)
    matched = df.dropna(subset=["ISO3"])
    unmatched = df[df["ISO3"].isna()]

    fig = px.choropleth(
        matched,
        locations="ISO3",
        color=color_col,
        hover_name="COUNTRY_NAME" if "COUNTRY_NAME" in matched.columns else "COUNTRY_CODE",
        hover_data={
            "COUNTRY_CODE": True,
            color_col: ":.2f",
            "N_OBSERVATIONS": True,
            "N_STATIONS": True,
            "ISO3": False,
        },
        color_continuous_scale="Blues",
        projection="natural earth",
        title=f"Average daily rainfall by country ({year})",
        labels={color_col: "Avg daily PRCP (mm)"},
    )
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))

    output_html = Path(output_html)
    output_html.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output_html))

    if output_png is not None:
        try:
            fig.write_image(str(output_png), scale=2)
        except Exception:
            # kaleido may be unavailable on the cluster — HTML is enough.
            pass

    return fig, matched, unmatched


def plot_daily_size_bars(
    year_sizes: list[tuple[int, int]],
    output_path: str | Path,
):
    """Bar chart of compressed daily file sizes by year (Processing viz)."""
    import matplotlib.pyplot as plt

    years = [y for y, _ in year_sizes]
    sizes_mb = [s / (1024 ** 2) for _, s in year_sizes]
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.bar(years, sizes_mb, width=1.0, color="#3d5a80")
    ax.set_title("Compressed size of each year in daily")
    ax.set_xlabel("Year")
    ax.set_ylabel("Size (MB)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    return save_figure(fig, output_path)

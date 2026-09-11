# GHCN-Daily Climate Analysis with Apache Spark

[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.3.5-e25a1c)](https://spark.apache.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](LICENSE)

Scalable analysis of the **Global Historical Climatology Network - Daily (GHCN-Daily)** dataset
using Apache Spark on an Azure/Kubernetes cluster. The pipeline reads ~13 GB of compressed daily
climate summaries (**~3.19 billion rows**) from Azure Blob Storage over `wasbs://`, enriches the
station metadata, answers a set of station- and observation-level questions, and produces
time-series and geospatial visualisations - collecting only small aggregated results to the driver.

> Coursework for **DATA420-26S2 (C) - Scalable Data Science** (University of Canterbury),
> refactored into a reusable, tested Python package with a single authoritative analysis notebook.

---

## Highlights

| Question | Result |
|---|---|
| Total stations | **132,501** (GSN 991 - HCN 1,218 - CRN 234; 15 in >1 network) |
| `daily` volume | **3,190,506,382 rows**, 13.15 GiB compressed (~111 GB / 7.87x uncompressed), 99.67% of all data |
| Years in `daily` | **265** (1750-2026; 1751-1762 missing) |
| Stations with all 5 core elements / precip-only | **20,505 / 16,189** |
| Stations never in `daily` | **63** (and 0 daily IDs missing from the metadata) |
| Closest NZ stations | **Paraparaumu & Wellington Aero, 50.5 km** (Haversine) |
| Most-observed element | **PRCP, 1,095,438,345 obs** across all years |
| TMAX with no same-day TMIN | **10,727,375** obs from 28,762 stations |
| Highest country-year rainfall | **Equatorial Guinea 2000, 436.1 mm/day - from one observation** (sparse-sample outlier) |

Full discussion and figures: **[`reports/final_report.md`](reports/final_report.md)**.

---

## Project status

The `ghcn` package is a **clean-room refactor** of the analysis: its logic reproduces the
results reported here, and its pure functions are unit-tested (see `tests/`). It has **not yet
been executed end-to-end on the cluster** in this packaged form, so a verification run on the
UC MADS Spark cluster is the recommended next step. The authoritative numbers and figures come
from the original cluster run, preserved under [`docs/executed_notebooks/`](docs/executed_notebooks)
and written up in [`reports/final_report.md`](reports/final_report.md). Some field names in the
package differ slightly from those exports (a deliberate tidy-up).

## What this project demonstrates

- **Distributed-join strategy** - replacing a 3.19-billion-row shuffle join with a
  `broadcast` + LEFT SEMI + LEFT ANTI pattern (verified in the physical plan).
- **Schema discipline** - parsing fixed-width metadata by character range, and catching the
  silent `DateType` NULL trap when reading `YYYYMMDD`.
- **Efficient storage** - the enriched table saved as Parquet partitioned by country for
  predicate/partition pruning, with an `ELEMENTS` array column CSV could not hold.
- **Honest data analysis** - distinguishing *no observation* from *zero*, and tracing the
  "wettest country on Earth" outlier back to a single reading.
- **Software engineering** - a typed, documented `ghcn` package; unit tests; linting; a
  reproducible notebook; secrets kept out of source control.

---

## Repository structure

```
ghcn-spark-analysis/
├── src/ghcn/                    # installable analysis package
│   ├── config/                  # paths & Spark/cluster configuration
│   ├── loaders/                 # fixed-width + CSV(.gz) readers
│   ├── schemas/                 # column ranges & Spark schemas
│   ├── processing/              # enrichment, joins, storage, validation
│   ├── analysis/                # stations, distance, observations, rainfall
│   ├── visualization/           # NZ time series, choropleth, helpers
│   └── utils/                   # Spark session, IO helpers, constants
├── notebooks/
│   └── ghcn_analysis.ipynb      # single authoritative walkthrough (real cluster outputs)
├── reports/
│   ├── final_report.md          # written report (3,000-5,000 words) + figures/
│   └── final_report.docx
├── docs/
│   ├── assignment/              # brief + grading rubric
│   ├── executed_notebooks/      # cluster-rendered HTML (evidence, tokens redacted)
│   └── data_dictionary.md
├── tests/                       # pytest unit tests (Spark-free where possible)
├── config/config.example.yaml   # non-secret configuration template
├── pyproject.toml · requirements.txt · Makefile · .env.example
```

---

## Getting started

> **Reproducibility note.** The full results require the UC MADS Spark cluster and the read-only
> `campus-data` container; they cannot be regenerated on a laptop. The committed notebook and
> `docs/executed_notebooks/` preserve the real, cluster-executed outputs. The steps below set up
> the package for local development, linting, and testing.

```bash
# 1. Install (editable, with geo + dev extras)
pip install -e ".[geo,dev]"      # or: make install

# 2. Provide the output-container SAS token (never committed)
cp .env.example .env             # then paste your AZURE_USER_SAS_TOKEN

# 3. Lint and test
make lint
make test

# 4. Open the walkthrough
make notebook                    # notebooks/ghcn_analysis.ipynb
```

On the cluster, the notebook builds a Spark session via
`ghcn.utils.spark_utils.build_spark_session()` (2-4 executors, per the assignment caps) and reads
the SAS token from the `AZURE_USER_SAS_TOKEN` environment variable.

---

## Data

| Dataset | Format | Description |
|---|---|---|
| `daily` | gzip CSV, one file/year | One row per station/day/element; ~3.19 B rows - never `cache`/`collect` in full |
| `stations` | fixed-width text | Coordinates, elevation, country/state, name, GSN/HCN/CRN flags, WMO ID |
| `countries` / `states` | fixed-width text | Two-character code to name |
| `inventory` | fixed-width text | Elements recorded per station + first/last year |

See [`docs/data_dictionary.md`](docs/data_dictionary.md) for field ranges and the enriched-table schema.

---

## Acknowledgements

- Data: NOAA National Centers for Environmental Information, GHCN-Daily.
- Course: DATA420-26S2 (C), University of Canterbury.
- Generative AI was used to assist with refactoring, documentation, and review; all results
  were produced and verified on the course Spark cluster. See the report for the full
  acknowledgement.

## License

[MIT](LICENSE) (c) 2026 Ahmed Masoom Butt.

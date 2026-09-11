# Data dictionary

Character ranges are **1-indexed, inclusive** (as in the GHCN-Daily README); Spark `substring`
uses a start position and a length. Blank fixed-width fields are treated as null.

## `daily` (gzip CSV, one file per year)

| Field | Type | Notes |
|---|---|---|
| ID | string | 11-char station code |
| DATE | date | Source `YYYYMMDD`; read as string then `to_date(..., "yyyyMMdd")` |
| ELEMENT | string | Element code (PRCP, TMAX, TMIN, SNOW, SNWD, ...) |
| VALUE | double | Tenths of mm (PRCP) or tenths of degrees C (TMAX/TMIN); SNOW/SNWD in mm |
| MEASUREMENT_FLAG / QUALITY_FLAG / SOURCE_FLAG | string | Data flags |
| OBSERVATION_TIME | string | `HHMM` clock reading (kept as text to preserve leading zeros) |

## `stations` (fixed-width)

| Field | Range | Type |
|---|---|---|
| ID | 1-11 | string |
| LATITUDE | 13-20 | double |
| LONGITUDE | 22-30 | double |
| ELEVATION | 32-37 | double (-999.9 = missing) |
| STATE | 39-40 | string |
| NAME | 42-71 | string |
| GSN_FLAG | 73-75 | string |
| HCN_CRN_FLAG | 77-79 | string |
| WMO_ID | 81-85 | string |

## `countries` / `states` (fixed-width)

| Field | Range | Type |
|---|---|---|
| CODE | 1-2 | string |
| NAME | 4-.. | string |

## `inventory` (fixed-width)

| Field | Range | Type |
|---|---|---|
| ID | 1-11 | string |
| LATITUDE | 13-20 | double |
| LONGITUDE | 22-30 | double |
| ELEMENT | 32-35 | string |
| FIRSTYEAR | 37-40 | int |
| LASTYEAR | 42-45 | int |

## Enriched stations table (derived, 18 columns)

`ID, LATITUDE, LONGITUDE, ELEVATION, STATE, NAME, GSN_FLAG, HCN_CRN_FLAG, WMO_ID,
COUNTRY_CODE, COUNTRY_NAME, STATE_NAME, ELEMENTS (array), FIRST_YEAR, LAST_YEAR,
ACTIVE_2026 (bool), CORE_ELEMENT_COUNT, OTHER_ELEMENT_COUNT`.

Saved as Parquet partitioned by `COUNTRY_CODE` (repartitioned to 3).

"""Shared constants for GHCN element codes and related helpers."""

from __future__ import annotations

# Five core elements (GHCN Daily README Section III).
CORE_ELEMENTS = ("PRCP", "SNOW", "SNWD", "TMAX", "TMIN")

# Units after converting VALUE to physical units (VALUE is stored in tenths for
# temperatures and precipitation; SNOW/SNWD are already in mm).
ELEMENT_UNITS = {
    "PRCP": "mm",          # stored as tenths of mm → divide by 10
    "SNOW": "mm",
    "SNWD": "mm",
    "TMAX": "°C",          # stored as tenths of °C → divide by 10
    "TMIN": "°C",
}

# Mean Earth radius used by the Haversine distance (km).
EARTH_RADIUS_KM = 6371.0

# New Zealand FIPS country code in GHCN station IDs.
NZ_COUNTRY_CODE = "NZ"

# United States mainland FIPS code — territories are identified via country name.
US_COUNTRY_CODE = "US"

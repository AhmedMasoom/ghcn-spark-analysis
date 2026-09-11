"""Constants are import-safe (no Spark) and internally consistent."""
from ghcn.utils.constants import CORE_ELEMENTS, EARTH_RADIUS_KM, ELEMENT_UNITS


def test_five_core_elements():
    assert set(CORE_ELEMENTS) == {"PRCP", "SNOW", "SNWD", "TMAX", "TMIN"}
    assert len(CORE_ELEMENTS) == 5


def test_element_units_cover_core_elements():
    assert set(ELEMENT_UNITS) == set(CORE_ELEMENTS)


def test_earth_radius_is_mean_sphere():
    assert EARTH_RADIUS_KM == 6371.0

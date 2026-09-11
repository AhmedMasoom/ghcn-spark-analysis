"""Haversine distance regression tests.

The New Zealand closest-pair result (Paraparaumu AWS NZ000093417 and
Wellington Aero AWS NZM00093439, ~50.5 km) is the headline of Analysis Q2,
so we pin it here. Skips cleanly if pyspark is not installed locally.
"""
import pytest

pytest.importorskip("pyspark")  # distance module imports pyspark at top level

from ghcn.analysis.distance import haversine_km  # noqa: E402


def test_zero_distance_for_identical_points():
    assert haversine_km(-41.3, 174.8, -41.3, 174.8) == pytest.approx(0.0, abs=1e-9)


def test_none_for_missing_coordinate():
    assert haversine_km(None, 174.8, -41.3, 174.8) is None


def test_nz_closest_pair_matches_report():
    # NZ000093417 (Paraparaumu) and NZM00093439 (Wellington Aero)
    d = haversine_km(-40.9, 174.983, -41.333, 174.8)
    assert d == pytest.approx(50.5, abs=0.5)


def test_symmetry():
    a = haversine_km(-36.85, 174.76, -43.53, 172.64)   # Auckland ↔ Christchurch
    b = haversine_km(-43.53, 172.64, -36.85, 174.76)
    assert a == pytest.approx(b, rel=1e-9)
    assert a == pytest.approx(768, abs=15)              # ~760-780 km

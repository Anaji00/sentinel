"""
tests/test_regions.py

Comprehensive unit test suite for shared/utils/regions.py.
Validates:
  - Strategic chokepoint spatial classification (Strait of Hormuz, Malacca, Suez, Taiwan, Bab-el-Mandeb, Black Sea)
  - Sensitivity region flags and score multiplier scaling
  - All 16 ITU-R M.1371-5 AIS navigational status codes
  - All AIS vessel/cargo type decoding ranges
  - CWD path independence for loading regions.geojson
  - MultiPolygon coordinate flattening
"""

import os
import pytest
import numpy as np
from shared.utils.regions import (
    classify_region,
    is_sensitive_region,
    get_region_sensitivity_multiplier,
    decode_nav_status,
    decode_vessel_type,
    _flatten_coords,
    _init_spatial_index,
    NAVIGATIONAL_STATUS
)

# ── 1. STRATEGIC REGION SPATIAL CLASSIFICATION ──────────────────────────────

@pytest.mark.parametrize("lat, lon, expected_keyword", [
    (26.5, 56.5, "Hormuz"),
    (1.4, 103.0, "Malacca"),
    (12.6, 43.3, "Mandeb"),
    (30.0, 32.5, "Suez"),
    (24.0, 119.5, "Taiwan"),
    (43.5, 34.2, "Black Sea"),
    (0.0, 0.0, "Gulf of Guinea"),
    (85.0, 0.0, None),
])
def test_classify_region_strategic_points(lat, lon, expected_keyword):
    region = classify_region(lat, lon)
    if expected_keyword is None:
        assert region is None or region == "Unknown"
    else:
        assert region is not None
        assert expected_keyword.lower() in region.lower()


# ── 2. SENSITIVITY REGION & MULTIPLIERS ──────────────────────────────────────

@pytest.mark.parametrize("region_name, expected_sensitive, min_multiplier", [
    ("Strait of Hormuz", True, 1.4),
    ("Taiwan Strait", True, 1.3),
    ("Bab-el-Mandeb", True, 1.4),
    ("Persian Gulf", True, 1.2),
    ("Black Sea", True, 1.2),
    ("North Sea", False, 1.0),
    (None, False, 1.0),
])
def test_region_sensitivity(region_name, expected_sensitive, min_multiplier):
    assert is_sensitive_region(region_name) == expected_sensitive
    mult = get_region_sensitivity_multiplier(region_name)
    assert mult >= min_multiplier


# ── 3. ITU AIS NAVIGATIONAL STATUS DECODING (CODES 0-15) ──────────────────────

# Spaced, not CamelCase. These strings are read: they reach vessel headlines
# and the reasoning prompt, and a live headline said "Tanker 'BANDA'
# restrictedmanoeuverability in Turkish Straits" -- a word nobody writes and no
# model has seen. The detector no longer reads them at all; it matches the AIS
# code, because prose-matching a display label is what hid status 2.
@pytest.mark.parametrize("status_code, expected_label", [
    (0, "Under Way Using Engine"),
    (1, "Anchored"),
    (2, "Not Under Command"),
    (3, "Restricted Manoeuverability"),
    (4, "Constrained By Draught"),
    (5, "Moored"),
    (6, "Aground"),
    (7, "Engaged In Fishing"),
    (8, "Under Way Sailing"),
    (9, "Reserved HSC"),
    (10, "Reserved WIG"),
    (11, "Towing Astern"),
    (12, "Pushing Ahead Towing Alongside"),
    (13, "Reserved Future"),
    (14, "AIS-SART Active"),
    (15, "Undefined"),
    (99, "Unknown(99)"),
])
def test_decode_nav_status(status_code, expected_label):
    assert decode_nav_status(status_code) == expected_label


# ── 4. AIS VESSEL TYPE CATEGORY DECODING ──────────────────────────────────────

@pytest.mark.parametrize("type_code, expected_category", [
    (80, "Tanker"), (85, "Tanker"), (89, "Tanker"),
    (70, "CargoVessel"), (74, "CargoVessel"), (79, "CargoVessel"),
    (60, "Passenger"), (65, "Passenger"),
    (30, "Fishing"), (35, "Fishing"),
    (36, "Military"), (37, "Military"),
    (1, "WIG"), (4, "WIG"),
    (50, "SpecialCraft"), (55, "SpecialCraft"),
    (999, "Unknown(999)"),
])
def test_decode_vessel_type(type_code, expected_category):
    assert decode_vessel_type(type_code) == expected_category


# ── 5. MULTIPOLYGON COORDINATE FLATTENING ─────────────────────────────────────

def test_flatten_coords_helper():
    nested_coords = [
        [[[10.0, 20.0], [11.0, 21.0]]],
        [[[30.0, 40.0], [31.0, 41.0]]]
    ]
    flattened = list(_flatten_coords(nested_coords))
    assert len(flattened) == 4
    assert flattened[0] == [10.0, 20.0]
    assert flattened[-1] == [31.0, 41.0]


# ── 6. CWD PATH INDEPENDENCE ──────────────────────────────────────────────────

def test_cwd_path_independence(tmp_path):
    orig_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        _init_spatial_index()
        # Should resolve regions.geojson from package relative location without throwing
        reg = classify_region(26.5, 56.5)
        assert reg is not None
    finally:
        os.chdir(orig_cwd)

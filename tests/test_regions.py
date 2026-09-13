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
    # 43.5/34.2 sits inside Ukraine's claimed waters south of Crimea, and
    # classify_region now returns the smallest containing polygon rather than
    # whichever the spatial index yielded first -- so it answers "Ukrainian
    # Waters", which is strictly more informative and carries a higher
    # sensitivity multiplier (1.4) than the sea around it (1.3).
    (43.5, 34.2, "Ukrainian Waters"),
    # And a point in the open Black Sea, outside any territorial polygon, to
    # keep the parent region covered by this table as well.
    (42.5, 33.0, "Black Sea"),
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


def test_classify_region_returns_the_smallest_containing_polygon():
    """The chokepoint, not whichever polygon the spatial index yielded first.

    95.3% of live vessel positions fall inside more than one named region, so
    "first containing" was effectively arbitrary: the Bab-el-Mandeb answered
    'Gulf of Aden', the Panama Canal answered 'Colombian Territorial', and the
    Taiwan Strait answered 'Taiwan Territorial'. That is not only a wrong
    label -- is_sensitive_region and get_region_sensitivity_multiplier key off
    it, so 7,073 Taiwan Strait transits in three days lost the sensitivity flag.
    """
    assert classify_region(12.58, 43.33) == "Bab-el-Mandeb"
    assert classify_region(9.08, -79.68) == "Panama Canal"
    assert classify_region(24.50, 119.50) == "Taiwan Strait"
    assert classify_region(26.57, 56.25) == "Strait of Hormuz"


def test_resolving_more_specifically_never_lowers_sensitivity():
    """The specificity rule must be monotone in sensitivity.

    If a chokepoint is sensitive and the territorial water inside it is not,
    answering more precisely *loses* the flag and the fix becomes a regression
    for exactly the waters it improves. Every small polygon that sits inside a
    sensitive parent is therefore in HIGH_SENSITIVITY too.
    """
    pairs = [
        ("Taiwan Strait", "Taiwan Territorial"),
        ("Black Sea", "Crimean Waters"),
        ("Black Sea", "Ukrainian Waters"),
        ("Strait of Malacca", "Singapore Approach"),
        ("Taiwan ADIZ", "Taiwan Territorial"),
    ]
    for parent, child in pairs:
        if is_sensitive_region(parent):
            assert is_sensitive_region(child), (
                f"{child} sits inside sensitive {parent} and would lose the flag"
            )

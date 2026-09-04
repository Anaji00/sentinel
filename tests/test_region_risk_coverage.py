"""Every region the platform can name must be rated or deliberately neutral.

Two multiplier tables were written independently and never reconciled:
get_region_sensitivity_multiplier in shared/utils/regions.py, and
STS_TRANSFER_ZONES in shared/utils/streaming_detectors.py. Measured against the
69 regions classify_region can actually return:

  18 regions the platform rates sensitive were open ocean to the STS table
  10 regions appear in both tables, and all 10 carried different numbers
   3 STS keys name places classify_region can never return

The two highest-traffic regions in the data were among the 18. Strait of Malacca
and Taiwan Strait -- 30,611 and 40,891 events in a week -- returned 1.0, so a
vessel going dark in the busiest waterway on earth scored as if it had gone dark
in open ocean.

The list below is the point of this file. A region is allowed to be neutral, but
only by being named here, so that adding a region to the map is a decision about
its risk rather than a silent default.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.regions import get_region_sensitivity_multiplier  # noqa: E402
from shared.utils.streaming_detectors import (  # noqa: E402
    STS_TRANSFER_ZONES, sts_zone_risk_multiplier,
)

# Open water and transit approaches with no standing conflict, sanctions or
# chokepoint concentration. Neutral on purpose.
DELIBERATELY_NEUTRAL = {
    "Baltic Sea", "Belarus Airspace", "Bering Sea", "Caribbean Sea",
    "Caspian Sea", "Colombian Territorial", "Georgian Waters",
    "Gulf of Alaska", "Gulf of Mexico", "Gulf of Sidra",
    "Gulf of St. Lawrence", "Houston Ship Channel", "Labrador Sea",
    "Libyan Airspace", "Libyan Territorial", "Mediterranean Sea",
    "Mozambique Channel", "Nigerian Territorial", "North Sea",
    "Norwegian Sea", "Panama Canal Approach", "Rotterdam Approach",
    "Sea of Okhotsk", "Shanghai Approach", "Sudanese Airspace",
    "Sudanese Territorial", "Venezuelan Airspace", "Venezuelan Territorial",
    "Yellow Sea",
}


def _canonical_regions() -> list:
    """The names classify_region can return, from the map it actually loads."""
    import shared.utils.regions as regions
    names = sorted(set(getattr(regions, "_polygon_names", []) or []))
    if not names:
        names = sorted({n for n, *_ in getattr(regions, "_fallback_boxes", [])})
    return names


def test_the_region_map_actually_loaded():
    """Without this the two tests below pass by iterating over nothing."""
    regions = _canonical_regions()
    assert len(regions) > 50, f"only {len(regions)} regions loaded"


def test_every_region_is_rated_or_named_as_neutral():
    regions = _canonical_regions()
    unaccounted = [
        r for r in regions
        if get_region_sensitivity_multiplier(r) == 1.0 and r not in DELIBERATELY_NEUTRAL
    ]
    assert not unaccounted, (
        "these regions score as open ocean without being declared neutral:\n  "
        + "\n  ".join(unaccounted)
    )


def test_no_region_is_less_sensitive_to_a_dark_vessel_than_to_anything_else():
    """The STS multiplier amplifies for transfer risk; it must never de-rate.

    This is the defect itself: Malacca and Taiwan Strait were sensitive
    everywhere in the platform except in the one scorer that judges a vessel
    going dark.
    """
    for region in _canonical_regions():
        general = get_region_sensitivity_multiplier(region)
        sts = sts_zone_risk_multiplier(region)
        assert sts >= general, f"{region}: STS {sts} is below general {general}"


def test_the_busiest_dark_regions_are_not_open_ocean():
    """Named explicitly because they carried the cost."""
    for region in ("Strait of Malacca", "Taiwan Strait", "Turkish Straits"):
        assert sts_zone_risk_multiplier(region) > 1.0, region


def test_genuine_transfer_hubs_keep_their_higher_weight():
    """The fallback takes a maximum, so STS-specific ratings still win."""
    assert sts_zone_risk_multiplier("Strait of Hormuz") == 3.0
    assert sts_zone_risk_multiplier("North Korean Waters") == 3.0
    assert STS_TRANSFER_ZONES["Strait of Hormuz"] == 3.0


def test_open_ocean_stays_neutral():
    assert sts_zone_risk_multiplier("Bering Sea") == 1.0
    assert sts_zone_risk_multiplier(None) == 1.0
    assert sts_zone_risk_multiplier("a place that does not exist") == 1.0


def test_the_yemeni_pair_no_longer_disagrees():
    """Airspace was 1.5 and territorial water 1.0, though the shipping is the
    risk this platform actually watches."""
    assert get_region_sensitivity_multiplier("Yemeni Territorial") == \
           get_region_sensitivity_multiplier("Yemeni Airspace")


def test_a_watched_chokepoint_is_not_neutral():
    """Gulf of Guinea is one of the nine zones the AIS collector names."""
    assert get_region_sensitivity_multiplier("Gulf of Guinea") > 1.0

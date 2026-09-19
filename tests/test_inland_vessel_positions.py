"""
tests/test_inland_vessel_positions.py

A transponder reporting from the desert tripped nothing.

`impossible_transit` needs two reports and measures the speed between them. It
catches a transponder that jumps and passes one that sits still in the wrong
place, because a first report -- or a stationary one -- is never compared
against anything. Measured over seven days:

    Sudanese Airspace   64 events, 2 vessels   17.45N  35.33E
    Iran Airspace        1 event,  1 vessel    27.32N  57.82E

TITAN (MMSI 304496000) at 19.13N 35.33E is roughly 200km from the Red Sea
coast. The Iranian position is on land north of the Strait of Hormuz.

Sixty-five is a small number and the wrong reason to care. A hull reporting an
inland position beside Hormuz and beside the Red Sea is the signature of AIS
manipulation, which is one of the specific things this platform exists to see.

Note on the environment: these tests do not depend on the geometry, because the
test environment has no shapely and the bounding-box fallback gives a different
answer -- see `test_this_check_is_only_as_good_as_the_classifier` below, which
is the point of finding 558 stated as a live consequence.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import shared.utils.maritime_behaviour as mb


def _with_region(monkeypatch, region):
    """Pin the classifier, so these assert the rule and not the polygons."""
    import shared.utils.regions as regions
    monkeypatch.setattr(regions, "classify_region", lambda lat, lon: region)


def test_an_airspace_region_is_a_vessel_reporting_inland(monkeypatch):
    _with_region(monkeypatch, "Sudanese Airspace")
    out = mb.implausible_position(19.13, 35.33)
    assert out is not None
    assert out["reported_region"] == "Sudanese Airspace"
    assert out["reason"] == "vessel_position_inland"
    assert out["at"] == {"lat": 19.13, "lon": 35.33}


def test_a_maritime_region_is_not_a_finding(monkeypatch):
    for region in ("Strait of Hormuz", "Red Sea", "Bab-el-Mandeb", "Suez Canal",
                   "Singapore Approach", "Taiwan Territorial"):
        _with_region(monkeypatch, region)
        assert mb.implausible_position(26.57, 56.25) is None, region


def test_open_ocean_is_not_a_finding(monkeypatch):
    """Most of the sea is in no named region.

    Flagging `None` would report the Atlantic as fraud, which is the failure
    mode this check has to avoid before it is worth anything.
    """
    _with_region(monkeypatch, None)
    assert mb.implausible_position(30.0, -40.0) is None


def test_nonsense_coordinates_are_refused():
    assert mb.implausible_position(None, None) is None
    assert mb.implausible_position("x", "y") is None
    assert mb.implausible_position(91.0, 0.0) is None
    assert mb.implausible_position(0.0, 181.0) is None


def test_the_check_needs_no_previous_report():
    """The whole reason it exists: `impossible_transit` requires two."""
    import inspect
    sig = inspect.signature(mb.implausible_position)
    assert list(sig.parameters) == ["lat", "lon"]
    transit = inspect.signature(mb.impossible_transit)
    assert "previous" in transit.parameters


def test_this_check_is_only_as_good_as_the_classifier():
    """Finding 558, stated as a consequence rather than a disagreement rate.

    Production has shapely and classifies TITAN's position as "Sudanese
    Airspace". Without shapely the bounding-box fallback classifies the same
    point as "Sudanese Territorial" -- a MARITIME region. The fallback does not
    merely mislabel the position; it turns this detection into a
    non-detection, and a spoofed transponder 200km inland reads as a vessel
    lawfully at sea.

    That is why shapely is a declared dependency rather than an optional
    import, and why this file pins the classifier instead of trusting it.
    """
    from shared.utils.regions import HAS_SHAPELY, classify_region

    label = classify_region(19.13, 35.33)
    if HAS_SHAPELY:
        assert label == "Sudanese Airspace"
        assert mb.implausible_position(19.13, 35.33) is not None
    else:
        # Documented, not asserted as acceptable: this is the degraded answer.
        assert label == "Sudanese Territorial"
        assert mb.implausible_position(19.13, 35.33) is None


def test_the_enricher_runs_the_check_without_a_previous_position():
    src = (ROOT / "services" / "enrichment" / "enrichers" / "maritime.py").read_text(
        encoding="utf-8"
    )
    assert "implausible_position(lat, lon)" in src
    assert "_inland_event(" in src
    # Outside the `if prev` guard that gates the transit check.
    #
    # Asserted on indentation, because that is the structural claim: the inland
    # check must run at loop level for every position, not nested inside the
    # branch that requires a previous report. Nesting it there would reproduce
    # exactly the gap it exists to close.
    lines = src.splitlines()
    guard = next(i for i, l in enumerate(lines) if "if prev and lat is not None" in l)
    call = next(i for i, l in enumerate(lines) if "inland = implausible_position" in l)
    assert call > guard
    guard_indent = len(lines[guard]) - len(lines[guard].lstrip())
    owner = next(
        i for i in range(call, guard, -1)
        if lines[i].lstrip().startswith("if lat is not None")
    )
    owner_indent = len(lines[owner]) - len(lines[owner].lstrip())
    assert owner_indent == guard_indent, (
        "the inland check must sit at the same level as the transit guard, "
        "not inside it"
    )

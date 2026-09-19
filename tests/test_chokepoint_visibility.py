"""
tests/test_chokepoint_visibility.py

A strait nothing can see must not read as a strait with nothing in it.

Three separate mechanisms were hiding the chokepoints this platform exists to
watch, and each one looked fine on its own:

  the query     /events/maritime returns the newest N rows. Measured live, the
                newest 250 span 184 seconds and carry seven regions. Singapore
                Approach and Taiwan Territorial alone produce about 4,400 vessel
                events an hour, so the Strait of Hormuz -- four in a day -- is
                not reachable at any limit the endpoint can afford to scan.

  the feed      Sentinel-1 radar density is the only current observation of
                Bab-el-Mandeb this deployment has. Those events carry no
                `vessel_data`, so the one maritime feed the map fetches cannot
                return them, and nothing else asked.

  the map       a vessel with no position was drawn at the default 25N 55E,
                which is the Persian Gulf just inside the approaches to Hormuz.
                34 of 250, 13.6%, were stacked on that point.

The third is the one worth stating plainly: the map was inventing vessels a few
miles from the strait the operator could not get real data about.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MAP = ROOT / "frontend" / "src" / "components" / "GlobalMap.tsx"
CHOKEPOINTS = ROOT / "services" / "api_gateway" / "routes" / "chokepoints.py"
MAIN = ROOT / "services" / "api_gateway" / "routes" / "main.py"


def _map() -> str:
    return MAP.read_text(encoding="utf-8")


def _map_code_only() -> str:
    """The map with its comments stripped.

    The module explains these defaults in prose directly above the lines that
    no longer use them, and a test that fails on its own explanation is a trap
    this audit has already sprung three times.
    """
    out = []
    for line in _map().splitlines():
        stripped = line.strip()
        if stripped.startswith("//") or stripped.startswith("*") or stripped.startswith("/*"):
            continue
        out.append(line)
    return chr(10).join(out)


# -- the map must not invent a position ---------------------------------------


def test_no_default_coordinates_remain():
    """25N 55E is the Persian Gulf; 38.8 -77.0 is Washington DC."""
    code = _map_code_only()
    assert "25.0, 55.0" not in code, (
        "the vessel layer must not fall back to a fixed point in the Gulf"
    )
    assert "38.8, -77.0" not in code, (
        "the aviation layer must not fall back to a fixed point over DC"
    )


def test_an_unplaceable_region_drops_the_marker():
    code = _map()
    assert "function resolveRegionFallback(regionName: string): [number, number] | null" in code
    assert code.count("if (!centroid) return;") == 2, (
        "both layers must drop what they cannot place, rather than placing it"
    )


def test_a_centroid_position_is_marked_approximate():
    code = _map()
    assert "approximatePosition" in code
    assert "POSITION IS THE REGION CENTROID" in code, (
        "a vessel drawn at its region's centroid must say so"
    )


def test_zero_is_a_coordinate():
    """Vessels in the Gulf of Guinea report a latitude of exactly 0."""
    code = _map_code_only()
    assert "n !== 0 && Math.abs(n) <= maxBound" not in code, (
        "rejecting 0 discards the equator and the prime meridian, and then the "
        "fallback moved those vessels to the Persian Gulf"
    )
    assert code.count("if (lat === 0 && lon === 0)") == 2, (
        "only the (0, 0) pair is the no-fix marker, and both layers check it"
    )


def test_the_map_does_not_invent_telemetry():
    """12.4 knots and 'Underway Using Engine' were defaults, not observations."""
    code = _map_code_only()
    assert "12.4" not in code, "a vessel that reported no speed was shown making 12.4 knots"
    assert "'Underway Using Engine'" not in code
    assert "'International Shipping Lane'" not in code


def test_the_map_asks_for_regional_coverage():
    code = _map()
    assert "region_spread=" in code, (
        "without it the maritime page is 184 seconds of the busiest regions"
    )


# -- and the chokepoint endpoint must distinguish quiet from unobserved -------


def test_the_endpoint_separates_silent_from_unobserved():
    code = CHOKEPOINTS.read_text(encoding="utf-8")
    assert "ais_silent" in code
    assert "unobserved_by_any_instrument" in code, (
        "a strait SAR can see is not dark because AIS cannot see it"
    )


def test_both_instruments_are_queried():
    code = CHOKEPOINTS.read_text(encoding="utf-8")
    assert "copernicus_sentinel1" in code, "SAR is the only current view of Bab-el-Mandeb"
    assert "vessel_data IS NOT NULL" in code


def test_the_watched_list_covers_the_straits_the_platform_claims():
    from services.api_gateway.routes.chokepoints import WATCHED

    for strait in ("Strait of Hormuz", "Bab-el-Mandeb", "Strait of Malacca", "Suez Canal"):
        assert strait in WATCHED, f"{strait} is named throughout this codebase"


def test_the_router_is_registered():
    """A route nothing mounts answers 404, which is the defect it was written for."""
    code = MAIN.read_text(encoding="utf-8")
    assert re.search(r"^\s*chokepoints,\s*$", code, re.M), "imported"
    assert "app.include_router(chokepoints.router)" in code, "mounted"

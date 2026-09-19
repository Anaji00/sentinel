"""
tests/test_ais_coverage_and_radar_grid.py

What the platform can see in a strait, and what it was throwing away.

Two measurements drove the changes this file pins, both taken against the live
AISStream account on a second connection (the account allows three):

  coverage    One box from the Red Sea to India's west coast, 5-32N / 32-75E,
              no message-type filter, four minutes: 264 positioned messages
              from 148 vessels, every one of them in two 2-degree cells at
              30-32N / 32-36E -- Suez and Port Said. Hormuz, Bab-el-Mandeb, the
              Red Sea, the Gulf of Oman, the Arabian Sea and the Indian west
              coast returned nothing at all. A control box over the Taiwan
              Strait in the same session returned 261 messages, so the probe
              worked. Terrestrial AIS cannot cover those straits, and no
              configuration change makes it.

  filter      That same control box, unfiltered, carried 261 messages of which
              the collector's FilterMessageTypes admitted 101. The other 160 --
              61.3% -- were Class B: the transponder class on fishing boats,
              tugs, tenders and pilot craft, which is most of what moves in a
              strait.

The first is a fact about the world and is recorded in the chokepoint endpoint.
The second was ours, and is fixed here.
"""

import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

COLLECTOR = ROOT / "services" / "collector-ais" / "main.py"
MARITIME = ROOT / "services" / "enrichment" / "enrichers" / "maritime.py"
SAR = ROOT / "services" / "collector-sar" / "main.py"


def _sar_module():
    """The SAR collector, imported for its pure helpers."""
    os.environ.setdefault("SAR_GRID_STEPS", "8")
    spec = importlib.util.spec_from_file_location("sar_main_for_test", SAR)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


# -- the 61% ------------------------------------------------------------------


def test_class_b_position_reports_are_subscribed():
    """160 of 261 messages in the control window were Class B."""
    code = COLLECTOR.read_text(encoding="utf-8")
    for message_type in (
        "StandardClassBPositionReport",
        "ExtendedClassBPositionReport",
        "StaticDataReport",
    ):
        assert message_type in code, (
            f"{message_type} is served by AISStream and was being discarded "
            "at the subscription"
        )


def test_the_enricher_routes_what_the_collector_now_asks_for():
    """A type subscribed and not routed is counted as unrouted and dropped."""
    code = MARITIME.read_text(encoding="utf-8")
    assert "_POSITION_MESSAGE_TYPES" in code
    assert "_STATIC_MESSAGE_TYPES" in code
    for message_type in ("StandardClassBPositionReport", "ExtendedClassBPositionReport"):
        assert message_type in code, f"{message_type} reaches the enricher unrouted"


def test_the_position_body_is_looked_up_not_chained():
    """AISStream keys the body by message type name."""
    code = MARITIME.read_text(encoding="utf-8")
    assert "_POSITION_BODY_KEYS" in code
    assert 'pos = msg.get("PositionReport") or {}' not in code, (
        "reading only the Class A body leaves every Class B report position-less"
    )


# -- the radar grid -----------------------------------------------------------


def test_the_grid_covers_the_box_exactly():
    m = _sar_module()
    bbox = {"west": 54.0, "south": 24.0, "east": 58.0, "north": 27.5}
    cells = m._grid_cells(bbox, 4)
    assert len(cells) == 16
    lats = [lat for lat, _lon, _g in cells]
    lons = [lon for _lat, lon, _g in cells]
    # Centres, so they sit half a cell inside each edge.
    assert min(lats) > bbox["south"] and max(lats) < bbox["north"]
    assert min(lons) > bbox["west"] and max(lons) < bbox["east"]


def test_every_cell_is_a_closed_polygon():
    m = _sar_module()
    bbox = {"west": 43.0, "south": 11.5, "east": 45.5, "north": 13.5}
    for _lat, _lon, geom in m._grid_cells(bbox, 3):
        ring = geom["coordinates"][0]
        assert ring[0] == ring[-1], "openEO rejects an unclosed ring"
        assert len(ring) == 5


def test_counts_survive_every_shape_a_backend_returns():
    """A wrong guess here reads an empty grid rather than failing."""
    m = _sar_module()
    assert m._as_count_list([1, 2, 3]) == [1, 2, 3]
    assert m._as_count_list([[4], [5], [6]]) == [4, 5, 6]
    assert m._as_count_list({"1": 7, "0": 8}) == [8, 7]
    assert m._as_count_list(None) == []


def test_the_grid_cannot_take_down_the_scalar_reading():
    """Bab-el-Mandeb has depended on the whole-box reading alone.

    The grid is asked for first and the box totals are its column sums, so the
    cost is the two openEO executions it always was. A backend that refuses a
    FeatureCollection must still get the scalar reading, by the path that
    produced it before the grid existed.
    """
    code = SAR.read_text(encoding="utf-8")
    body = code.split("async def sweep(")[1]
    assert "_measure_grid(connection, bbox)" in body
    assert "Radar grid failed for %s (scalar reading kept)" in body
    assert "reading = await _measure(connection, name, bbox)" in body, (
        "the pre-grid scalar path must remain as the fallback"
    )


def test_the_grid_is_on_and_its_cost_is_measured():
    """Cost does not scale with geometry count; the earlier verdict was wrong.

    Bab-el-Mandeb, one openEO execution each: 1 polygon 176.5s, 9 cells 113.2s,
    36 cells 109.7s. Validated on Hormuz at about seven times the area: gridded
    sum+count 1046.6s against the ungridded pair's 846.0s -- 24% more, on a
    sweep that runs once a day, for 35 of 36 cells carrying a return and a 25x
    spread between the densest cell and the box mean.
    """
    code = SAR.read_text(encoding="utf-8")
    assert 'os.getenv("SAR_GRID_STEPS", "6")' in code
    assert "_GridDisabled" in code, "0 must still skip the query, not request 0 cells"
    assert 'os.getenv("SAR_GRID_TIMEOUT_SEC", "2700")' in code, (
        "the budget must exceed the measured 1046.6s with room for a loaded backend"
    )


def test_the_grid_call_is_bounded():
    """An exception fallback does not catch a call that never answers.

    This collector sweeps once a day, so a hung openEO request does not retry --
    it means no radar reading at all until someone restarts the service, on the
    one source that sees Bab-el-Mandeb.
    """
    code = SAR.read_text(encoding="utf-8")
    assert "SAR_GRID_TIMEOUT_SEC" in code
    assert "asyncio.wait_for(" in code
    assert "except asyncio.TimeoutError:" in code, (
        "the timeout must fall back to the scalar path, not propagate"
    )


def test_the_grid_does_not_double_the_cube_evaluations():
    """Four executions per chokepoint stopped the sweep finishing.

    openEO re-evaluates the cube per `.execute()`. Asking for the box and then
    for the cells meant four passes where there had been two, and the first
    sweep after deploy had not produced a single chokepoint in fifteen minutes
    against a five-minute-per-strait baseline.
    """
    code = SAR.read_text(encoding="utf-8")
    body = code.split("async def sweep(")[1].split("async def ")[0]
    # The box totals come from the cells, not from a second pair of queries.
    assert 'target_pixels=sum(c["target_pixels"] for c in grid)' in body
    assert 'water_pixels=sum(c["water_pixels"] for c in grid)' in body


def test_the_grid_is_published_where_the_api_reads_it():
    """Built and unwired is the defect this audit keeps finding.

    Both sides must reach the same key through the same import. Asserting the
    literal in both files would pass while pinning the duplication this
    repository already lost a watchlist lookup to.
    """
    sar = SAR.read_text(encoding="utf-8")
    api = (ROOT / "services" / "api_gateway" / "routes" / "chokepoints.py").read_text(
        encoding="utf-8"
    )
    shared = (ROOT / "shared" / "utils" / "chokepoints.py").read_text(encoding="utf-8")

    assert 'GRID_KEY = "sentinel:chokepoint:grid:{chokepoint}"' in shared, (
        "the key is defined once, beside the baseline key"
    )
    for side, code in (("collector", sar), ("api", api)):
        assert "grid_key" in code, f"{side} must use the shared key helper"
        assert "sentinel:chokepoint:grid" not in code, (
            f"{side} spells the key out instead of importing it"
        )
    assert "radar_grid" in api


def test_a_radar_cell_is_never_called_a_vessel():
    """No MMSI, no name, and two hulls close together read as one."""
    api = (ROOT / "services" / "api_gateway" / "routes" / "chokepoints.py").read_text(
        encoding="utf-8"
    )
    assert "vessel_count" not in api
    map_code = (ROOT / "frontend" / "src" / "components" / "GlobalMap.tsx").read_text(
        encoding="utf-8"
    )
    assert "RADAR RETURN (NO TRANSPONDER)" in map_code
    assert "not a vessel" in map_code

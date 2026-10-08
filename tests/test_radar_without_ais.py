"""Radar returns checked against AIS, and radar over oil terminals.

A radar return in a cell where AIS -- which receives well in the check areas --
saw no hull over the same days is a vessel not transmitting. Two things would
make that claim false and are guarded: an AIS index younger than the radar
window, and a cell that returns on every pass because it is a structure.
"""

import asyncio
import importlib.util
import json
import pathlib
import sys
from datetime import date, datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.chokepoints import (  # noqa: E402
    AIS_CELL_INDEX_SINCE_KEY, AIS_CELL_KEY, SAR_AIS_CHECK_AREAS, SAR_TERMINALS,
    ais_cell, ais_cells_in_box, in_ais_check_area, likely_fixed_structure,
)
from shared.utils.maritime_behaviour import AIS_COVERAGE_BOXES  # noqa: E402


def test_check_areas_lie_inside_ais_coverage():
    """A check area outside the subscription would flag every return."""
    for name, b in SAR_AIS_CHECK_AREAS.items():
        inside = any(
            lo[0] <= b["south"] and b["north"] <= hi[0] and lo[1] <= b["west"] and b["east"] <= hi[1]
            for lo, hi in AIS_COVERAGE_BOXES
        )
        assert inside, name


def test_terminal_boxes_are_small():
    """Each area is one openEO job a day against the credit allowance."""
    for name, b in {**SAR_TERMINALS, **SAR_AIS_CHECK_AREAS}.items():
        assert (b["east"] - b["west"]) * (b["north"] - b["south"]) <= 0.25, name


def test_cells_in_box_cover_the_box():
    cells = ais_cells_in_box(1.20, 103.80, 1.30, 103.90)
    for lat, lon in ((1.20, 103.80), (1.25, 103.85), (1.2999, 103.8999)):
        assert ais_cell(lat, lon) in cells
    # Rounding at an edge may add a row; erring wide only adds AIS evidence.
    assert len(cells) <= 16


def test_check_area_membership():
    assert in_ais_check_area(1.27, 103.88)
    assert not in_ais_check_area(26.5, 56.3)
    assert not in_ais_check_area(None, None)


def test_a_cell_present_on_most_passes_is_a_structure():
    assert likely_fixed_structure([1, 1, 1, 1, 0])
    assert not likely_fixed_structure([1, 0, 1, 0])
    assert not likely_fixed_structure([1, 1, 1])  # too few passes to say


class _Raw:
    def __init__(self):
        self.kv = {}
        self.sets = {}

    async def get(self, k):
        return self.kv.get(k)

    async def set(self, k, v, ex=None, nx=False):
        if nx and k in self.kv:
            return False
        self.kv[k] = v
        return True

    async def sunion(self, keys):
        out = set()
        for k in keys:
            out |= self.sets.get(k, set())
        return out


class _Redis:
    def __init__(self):
        self.raw = _Raw()


def _sar():
    src = ROOT / "services" / "collector-sar" / "main.py"
    sys.path.insert(0, str(src.parent))
    spec = importlib.util.spec_from_file_location("collector_sar_main", src)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except SystemExit:
        pass
    return mod


BOX = SAR_AIS_CHECK_AREAS["Singapore Strait"]


def _grid(lat, lon, targets):
    return [{"latitude": lat, "longitude": lon, "target_pixels": targets, "water_pixels": 100,
             "target_density": targets / 100}]


def test_warming_up_until_the_index_covers_the_window():
    m = _sar()
    r = _Redis()
    r.raw.kv[AIS_CELL_INDEX_SINCE_KEY] = str(datetime.now(timezone.utc).timestamp())
    out = asyncio.run(m._cross_check_ais(r, "Singapore Strait", BOX, _grid(1.2, 103.9, 5), "2026-10-04"))
    assert out == {"status": "warming_up"}


def _seeded():
    r = _Redis()
    r.raw.kv[AIS_CELL_INDEX_SINCE_KEY] = "0"
    return r


def test_a_return_with_no_ais_is_unidentified():
    m = _sar()
    r = _seeded()
    grid = _grid(1.2, 103.9, 5)
    out = asyncio.run(m._cross_check_ais(r, "Singapore Strait", BOX, grid, "2026-10-04"))
    assert out["status"] == "checked" and out["unidentified_contacts"] == 1
    assert grid[0]["no_ais"] is True and grid[0]["ais_vessels"] == 0


def test_a_return_ais_saw_is_identified():
    m = _sar()
    r = _seeded()
    i, j = ais_cell(1.2, 103.9)
    r.raw.sets[AIS_CELL_KEY.format(day=date.today().isoformat(), i=i, j=j)] = {"563000001"}
    grid = _grid(1.2, 103.9, 5)
    out = asyncio.run(m._cross_check_ais(r, "Singapore Strait", BOX, grid, "2026-10-04"))
    assert out["unidentified_contacts"] == 0
    assert grid[0]["ais_vessels"] == 1 and "no_ais" not in grid[0]


def test_a_cell_that_always_returns_is_a_structure_not_a_contact():
    m = _sar()
    r = _seeded()
    for n, day in enumerate(["2026-09-01", "2026-09-07", "2026-09-13", "2026-09-19", "2026-09-25"]):
        asyncio.run(m._cross_check_ais(r, "Singapore Strait", BOX, _grid(1.2, 103.9, 5), day))
    grid = _grid(1.2, 103.9, 5)
    out = asyncio.run(m._cross_check_ais(r, "Singapore Strait", BOX, grid, "2026-10-01"))
    assert out["fixed_structures"] == 1 and out["unidentified_contacts"] == 0
    assert grid[0]["fixed_structure"] is True


def test_rereading_one_acquisition_does_not_build_its_own_history():
    """A daily sweep re-reads the same 8-day window; one pass is one entry."""
    m = _sar()
    r = _seeded()
    for _ in range(6):
        asyncio.run(m._cross_check_ais(r, "Singapore Strait", BOX, _grid(1.2, 103.9, 5), "2026-10-04"))
    hist = json.loads(next(v for k, v in r.raw.kv.items() if "cell_history" in k))
    assert list(hist["cells"].values())[0] == [1]


def test_the_sweep_covers_terminals_and_check_areas():
    m = _sar()
    for name in list(SAR_TERMINALS) + list(SAR_AIS_CHECK_AREAS):
        assert name in m.SAR_AREAS

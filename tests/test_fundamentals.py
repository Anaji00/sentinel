"""SEC XBRL frames parse to point-in-time fundamentals rows."""

import importlib.util
import pathlib
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("fundamentals_under_test", ROOT / "services/collector-filings/fundamentals.py")
fd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fd)


def test_frames_cover_quarters_and_years_and_mark_recent_ones() -> None:
    frames = fd.frames_wanted(date(2026, 10, 8))
    names = {(c, f) for _t, c, _u, f, _r in frames}
    assert ("Assets", "CY2020Q1I") in names and ("Assets", "CY2026Q3I") in names
    assert ("Assets", "CY2026Q4I") not in names                      # not ended
    assert ("NetIncomeLoss", "CY2025") in names and ("NetIncomeLoss", "CY2026") not in names
    recent = {f for _t, c, _u, f, r in frames if r and c == "Assets"}
    assert recent == {"CY2025Q4I", "CY2026Q1I", "CY2026Q2I", "CY2026Q3I"}


def test_rows_map_cik_to_ticker_and_skip_bad_records() -> None:
    payload = {"data": [
        {"cik": 1750, "end": "2025-11-30", "val": 3242500000, "accn": "a"},
        {"cik": 2098, "start": "2025-07-01", "end": "2025-09-30", "val": 49063000},
        {"cik": "x", "end": "2025-11-30", "val": 1},
    ]}
    rows = fd.rows_from(payload, "Assets", "CY2025Q4I", {"1750": "AIR"})
    assert len(rows) == 2
    assert rows[0][:4] == ("1750", "AIR", "Assets", "CY2025Q4I") and rows[0][5] == date(2025, 11, 30)
    assert rows[1][1] is None and rows[1][4] == date(2025, 7, 1)

"""The split-date backfill covers its range without gaps or overlaps (scripts/backfill_split_dates.py)."""

import importlib.util
import pathlib
from datetime import date, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("backfill_split_dates", ROOT / "scripts" / "backfill_split_dates.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def test_windows_tile_the_range_exactly() -> None:
    since, until = date(2016, 1, 1), date(2026, 11, 11)
    ws = mod.windows(since, until)
    assert ws[0][0] == since and ws[-1][1] == until
    for (a0, a1), (b0, b1) in zip(ws, ws[1:]):
        assert b0 == a1 + timedelta(days=1)
    assert all((e - s).days < mod.WINDOW_DAYS for s, e in ws)
    assert mod.windows(date(2026, 11, 11), date(2026, 11, 11)) == [(date(2026, 11, 11), date(2026, 11, 11))]
    assert mod.windows(date(2026, 11, 12), date(2026, 11, 11)) == []

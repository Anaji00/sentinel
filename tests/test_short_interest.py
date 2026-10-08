"""FINRA short interest parses to point-in-time rows with an unrounded days-to-cover."""

import importlib.util
import pathlib
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "short_interest_under_test", ROOT / "services/collector-filings/short_interest.py")
si = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(si)


def test_rows_keep_the_universe_and_recompute_days_to_cover() -> None:
    recs = [
        {"symbolCode": "nvda", "settlementDate": "2026-09-15", "currentShortPositionQuantity": 11993211,
         "previousShortPositionQuantity": 11453634, "averageDailyVolumeQuantity": 13770013,
         "daysToCoverQuantity": 1, "changePercent": 4.71},
        {"symbolCode": "ZZZZ", "settlementDate": "2026-09-15", "currentShortPositionQuantity": 5},
        {"symbolCode": "AMC", "settlementDate": "2026-09-15", "currentShortPositionQuantity": 900,
         "averageDailyVolumeQuantity": 0},
        {"symbolCode": "AAPL", "settlementDate": "bad", "currentShortPositionQuantity": 1},
    ]
    rows = si.rows_from(recs, {"NVDA", "AMC", "AAPL"})
    assert [r[0] for r in rows] == ["NVDA", "AMC"]
    t, d, short, prev, adv, dtc, chg = rows[0]
    assert d == date(2026, 9, 15) and short == 11993211 and dtc == round(11993211 / 13770013, 4)
    assert rows[1][5] is None            # no volume, no days-to-cover


def test_finra_never_receives_the_sec_contact_address() -> None:
    assert "@" not in si.FINRA_HEADERS["User-Agent"]

"""Finnhub's free earnings data, kept: the calendar's consensus snapshotted
daily (revisions come from our own copies) and reported surprises."""

import importlib.util
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "earnings_estimates", ROOT / "services" / "collector-tradfi" / "earnings_estimates.py")
ee = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ee)


def test_calendar_rows_carry_the_days_consensus() -> None:
    items = [
        {"symbol": "mu", "date": "2026-12-17", "year": 2027, "quarter": 1, "hour": "amc",
         "epsEstimate": 3.21, "revenueEstimate": 12.5e9, "epsActual": None, "revenueActual": None},
        {"symbol": "BAD", "date": "not-a-date"},
        {"symbol": "", "date": "2026-12-01"},
    ]
    rows = ee.calendar_rows(date(2026, 10, 9), items)
    assert rows == [(date(2026, 10, 9), "MU", date(2026, 12, 17), 2027, 1, "amc", 3.21, 12.5e9, None, None)]


def test_surprise_rows_keep_actual_estimate_and_percent() -> None:
    items = [{"period": "2026-06-30", "actual": 1.1, "estimate": 1.0, "surprisePercent": 10.0},
             {"period": None, "actual": 1.0}]
    assert ee.surprise_rows("AAPL", items) == [("AAPL", date(2026, 6, 30), 1.1, 1.0, 10.0)]

"""Finnhub share counts fill only where the SEC frames have none, from the day they are known."""

import importlib.util
import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import factor_engine as fe  # noqa: E402

spec = importlib.util.spec_from_file_location("vendor_shares_t", ROOT / "services" / "collector-filings" / "vendor_shares.py")
vs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vs)


def test_vendor_counts_fill_only_missing_tickers_and_are_dated_one_lag_back() -> None:
    sec = [{"ticker": "XOM", "concept": "EntityCommonStockSharesOutstanding", "period_end": date(2026, 6, 30), "value": 4.2e9},
           {"ticker": "GOOGL", "concept": "Assets", "period_end": date(2026, 6, 30), "value": 4e11}]
    vendor = [{"ticker": "GOOGL", "as_of": date(2026, 10, 16), "shares": 1.21e10},
              {"ticker": "XOM", "as_of": date(2026, 10, 16), "shares": 9.9e9}]
    rows = fe.vendor_share_rows(sec, vendor)
    assert rows == [{"ticker": "GOOGL", "concept": "EntityCommonStockSharesOutstanding",
                     "period_end": date(2026, 7, 18), "value": 1.21e10}]


def test_finnhub_reports_millions() -> None:
    assert vs.shares_from_profile({"shareOutstanding": 12100.5}) == 12100.5e6
    assert vs.shares_from_profile({}) is None and vs.shares_from_profile({"shareOutstanding": 0}) is None

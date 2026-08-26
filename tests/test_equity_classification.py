r"""
tests/test_equity_classification.py

The equity filter decides what the platform is allowed to look at.

It sits on the tradfi collector's watchlist read, so anything it rejects is
never polled, never enriched, and never reaches an agent. Two defects, both
measured against the classifier rather than read off its docstring:

  1. BRK.B and BF.B were rejected as "non-alphabetic structural punctuation".
     The rule `[\.\/\-\=\+\~\d]` fires on the dot in a share class, so two
     of the largest US listings could not enter the watchlist. Adding them to
     PRIMARY_EQUITY_EXCEPTIONS would not have helped: that check runs *after*
     the punctuation rule.

  2. SPY, QQQ, GLD and ARKK were classified "clean primary US common equity".
     ALL_DERIVATIVE_ETFS lists leveraged and inverse products only, so the most
     heavily traded funds on the market matched no rule and fell through the
     bottom -- into the watchlist, for agents to reason about as companies.

The second defect also made the two validators contradict each other: the async
path returns False for anything Finnhub types as ETF/ETP, so the same ticker
answered True or False depending on which function the caller reached for.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.equities import (  # noqa: E402
    fast_classify_equity,
    is_valid_primary_equity,
    is_valid_primary_equity_async,
)


@pytest.mark.parametrize("ticker", [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM",
    "XOM", "UNH", "V", "MA", "LLY", "AVGO", "WMT", "COST", "PG", "JNJ",
])
def test_ordinary_large_caps_are_tradable(ticker):
    assert is_valid_primary_equity(ticker) is True


@pytest.mark.parametrize("ticker", ["BRK.B", "BF.B", "BRK.A", "HEI.A"])
def test_class_shares_are_common_equity(ticker):
    """Berkshire and Brown-Forman are companies, dot notwithstanding."""
    assert is_valid_primary_equity(ticker) is True
    assert fast_classify_equity(ticker)["asset_class"] == "PRIMARY_COMMON_EQUITY"


def test_class_share_rule_runs_before_the_punctuation_rule():
    """Ordering is the whole defect: the punctuation rule would reject first."""
    assert "Class share" in fast_classify_equity("BRK.B")["reason"]


@pytest.mark.parametrize("ticker", ["SPY", "QQQ", "IWM", "GLD", "TLT", "XLF", "ARKK"])
def test_index_and_sector_funds_are_not_companies(ticker):
    assert is_valid_primary_equity(ticker) is False
    assert fast_classify_equity(ticker)["asset_class"] == "INDEX_SECTOR_ETF"


@pytest.mark.parametrize("ticker", ["TQQQ", "SQQQ", "NVDL", "TSLL", "UVXY", "SOXL"])
def test_leveraged_products_stay_excluded(ticker):
    assert is_valid_primary_equity(ticker) is False


def test_real_punctuation_derivatives_are_still_rejected():
    """The class-share rule must not open the door to warrants and rights."""
    for bad in ("AAPL-W", "F/PB", "BRK.WS", "T.PRA", "XYZ=1"):
        assert is_valid_primary_equity(bad) is False, bad


@pytest.mark.anyio
async def test_the_two_validators_agree():
    """Same ticker, same answer, whichever function the caller reaches for."""
    class _Raw:
        async def get(self, key):
            return "ETF" if key.endswith("SPY") else None

    class _Redis:
        raw = _Raw()

    for ticker in ("SPY", "QQQ", "BRK.B", "AAPL", "TQQQ"):
        sync = is_valid_primary_equity(ticker)
        asyn = await is_valid_primary_equity_async(ticker, _Redis())
        assert sync == asyn, f"{ticker}: sync={sync} async={asyn}"


def test_a_junk_ticker_is_rejected_without_raising():
    for junk in ("", "   ", None, 42, "TOOLONGTICKER"):
        assert is_valid_primary_equity(junk) is False

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


# -- what Alpaca's equity endpoint will parse ----------------------------------

def test_alpaca_snapshot_filter_drops_only_non_equity_symbology():
    """One unparseable symbol 400s the whole snapshot request.

    CORE_MACRO_SYMBOLS deliberately carries CL=F, GC=F and ZB=F so the
    discovery engine has both legs of a macro relationship. The same list feeds
    Alpaca's equity snapshot endpoint, which rejects the entire request when any
    symbol in it is unparseable -- so one futures ticker cost the other
    forty-nine symbols their extended-hours snapshots, once a minute.

    Verified against the live endpoint: TLT, HYG, SPY, QQQ and AAPL return 200;
    TNX, VIX and DXY return 200 with an empty body; only the `=F` futures 400.
    The filter is therefore on symbology, not on membership of the macro list --
    excluding the list itself would drop TLT and HYG, which Alpaca prices.
    """
    import re
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1]
           / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")
    pattern = re.search(r'_ALPACA_EQUITY_SYMBOL = re\.compile\(r"([^"]+)"\)', src)
    assert pattern, "_ALPACA_EQUITY_SYMBOL must stay a module-level compiled pattern"
    rx = re.compile(pattern.group(1))

    for keep in ("AAPL", "SPY", "QQQ", "TLT", "HYG", "TNX", "VIX", "DXY", "BRK.B"):
        assert rx.match(keep), f"{keep} is priced by Alpaca and must be requested"

    for drop in ("CL=F", "GC=F", "ZB=F", "BTCUSDT"):
        assert not rx.match(drop), f"{drop} would 400 the whole batch"


def test_the_snapshot_request_uses_the_filtered_list():
    """A filter the request does not read is the defect it was written for."""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1]
           / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")
    url_line = next(
        ln for ln in src.splitlines() if "stocks/snapshots?symbols=" in ln
    )
    assert "snapshot_symbols" in url_line, (
        "the snapshot URL must be built from the filtered list, not from the "
        "raw subscription universe"
    )


def test_the_websocket_subscription_is_equities_only():
    """Six of fifty slots were held by symbols the feed cannot serve.

    Measured over eight hours of a regular session: CL=F, GC=F, ZB=F, DXY, TNX
    and VIX produced zero bars, while AAPL produced 229 and NVDA 255 -- and TLT
    and HYG, the two ETFs in the same CORE_MACRO_SYMBOLS list, produced theirs.
    The list is not the problem; the symbology is.

    Finnhub's cap is what makes it cost something: those six were 12% of the
    streaming budget, on the feed whose limit is the reason
    select_subscription_symbols exists.
    """
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1]
           / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")
    body = src.split("desired_subs = ")[1].split("to_add = ")[0]
    assert "_ALPACA_EQUITY_SYMBOL.match" in body, (
        "the websocket subscription must be filtered to equity symbology, the "
        "same way the snapshot request is"
    )

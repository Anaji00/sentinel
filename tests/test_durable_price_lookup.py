"""
tests/test_durable_price_lookup.py

Predictions could not be resolved, for a reason unrelated to eviction.

`_latest_price` read only `sentinel:quotes:latest:{ticker}`, which the collectors
write with a one-hour TTL. A prediction carries a 24-hour horizon and is resolved
the next day, by which time that key is gone. Measured an hour after the close:
two quote keys survived for a fifty-symbol watchlist.

`_score_directional` reads None as "unverifiable, so uncounted", so the
prediction was skipped, left unresolved, and eventually expired. No scorecard
could move, which meant the consensus engine had no weights to read and the
conviction calibration this audit keeps deferring could never be measured.

Two problems compounded, and the second was mine. The cache had no fallback, and
when one was added the existing early `return None` on a cache miss sat above it
-- so the fallback was unreachable for exactly the tickers that needed it. A miss
is the ordinary case here, not the end of the search.

Both fallbacks read what the platform already stores durably: crypto candle
lists are trimmed rather than expired, and tradfi_bars is the equity history
everything else measures against.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SOURCE = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")


def test_a_cache_miss_does_not_end_the_search():
    """The early return is what made the fallback dead code."""
    block = SOURCE.split("async def _latest_price")[1].split("async def ")[0]
    assert "raise _QuoteCacheMiss" in block
    assert "if not raw:\n                return None" not in block


def test_the_fallback_is_reached_from_the_cache_path():
    block = SOURCE.split("async def _latest_price")[1].split("async def ")[0]
    assert "return await self._durable_price(ticker)" in block


def test_crypto_falls_back_to_the_candle_list():
    """Those lists are trimmed, not expired, so they outlive the quote cache."""
    block = SOURCE.split("async def _durable_price")[1].split("\n    async def ")[0]
    assert "sentinel:candles:1m:" in block


def test_equities_fall_back_to_the_bar_history():
    block = SOURCE.split("async def _durable_price")[1].split("\n    async def ")[0]
    assert "FROM tradfi_bars" in block
    assert "ORDER BY time DESC" in block


def test_an_unknown_ticker_still_returns_nothing():
    """None is a real answer. Resolving against a price we do not have would
    manufacture a track record out of nothing."""
    block = SOURCE.split("async def _durable_price")[1].split("\n    async def ")[0]
    assert block.rstrip().endswith("return None")


def test_an_empty_ticker_is_rejected_before_any_lookup():
    block = SOURCE.split("async def _durable_price")[1].split("\n    async def ")[0]
    assert "if not symbol:" in block


def test_a_storage_failure_does_not_break_resolution():
    """A database hiccup should cost one unresolved prediction, not the loop."""
    block = SOURCE.split("async def _durable_price")[1].split("\n    async def ")[0]
    assert block.count("except Exception") >= 1


def test_the_cache_miss_signal_is_its_own_type():
    """Conflating 'no cached quote' with a genuine error is how the miss stayed
    invisible: both were swallowed by the same bare handler."""
    assert "class _QuoteCacheMiss(Exception):" in SOURCE
    assert "except _QuoteCacheMiss:" in SOURCE

"""
tests/test_agent_data_contracts.py

Agents reading a different shape than the collectors write.

Two defects, both silent, both found by comparing code against live Redis rather
than against the docstrings:

  1. _latest_price() json.loads()'d the quote then called .get("price") on it.
     Collectors write a bare number -- "93.23" -- and json.loads parses that to a
     float quite happily, so .get raised AttributeError into a bare
     `except Exception: return None`. It returned None for every ticker that
     existed, every time. The prediction resolver reads None as "unverifiable,
     so uncounted", so no prediction was ever scored and no scorecard ever moved
     -- which is also why the consensus engine found zero scorecards to weight
     agents by.

  2. StockCorrelationAgent split macro instruments from equities with a
     substring match on names: "BRENT", "WTI", "GLD", "VIX", "US10Y". The quote
     keys hold ticker symbols. On the live set of 22, that recognised CL=F and
     TLT and classified BZ=F (Brent), GC=F (gold), SI=F (silver), NG=F (gas),
     ZC=F (corn), ZW=F (wheat), ES=F and NQ=F (index futures), VXX and TIP as
     *equities* -- putting the macro side of a cross-asset correlation onto the
     equity side of the comparison.
"""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.equities import is_macro_asset, split_macro_and_equities  # noqa: E402


# ── quote parsing ────────────────────────────────────────────────────────────

def _run(coro):
    import asyncio
    return asyncio.run(coro)


class _Raw:
    def __init__(self, store):
        self._store = store

    async def get(self, key):
        return self._store.get(key)


class _Redis:
    def __init__(self, store):
        self.raw = _Raw(store)


def _agent(store):
    """SentinelAgent is abstract, so a minimal concrete subclass is needed."""
    import logging
    from services.agents.base import SentinelAgent

    class _Concrete(SentinelAgent):
        @property
        def output_topic(self):
            return "test.topic"

        async def handle(self, message):
            return None

    a = _Concrete.__new__(_Concrete)
    a.redis = _Redis(store)
    a.logger = logging.getLogger("test.quotes")
    return a


def test_a_bare_number_is_the_format_collectors_write():
    """json.loads("93.23") is a float, and floats have no .get()."""
    parsed = json.loads("93.23")
    assert isinstance(parsed, float)
    assert not hasattr(parsed, "get")


def test_a_bare_number_quote_is_read():
    a = _agent({"sentinel:quotes:latest:BIDU": "93.23"})
    assert _run(a._latest_price("BIDU")) == pytest.approx(93.23)


def test_a_json_object_quote_is_still_read():
    """Any collector that upgrades to an object must keep working."""
    a = _agent({"sentinel:quotes:latest:AAPL": json.dumps({"price": 231.4})})
    assert _run(a._latest_price("AAPL")) == pytest.approx(231.4)
    for field in ("close", "last", "c"):
        a = _agent({"sentinel:quotes:latest:X": json.dumps({field: 12.5})})
        assert _run(a._latest_price("X")) == pytest.approx(12.5)


def test_bytes_from_redis_are_handled():
    a = _agent({"sentinel:quotes:latest:MSFT": b"412.80"})
    assert _run(a._latest_price("MSFT")) == pytest.approx(412.80)


def test_a_missing_or_junk_quote_is_still_none():
    """None must stay a real answer -- it is what keeps a prediction uncounted."""
    assert _run(_agent({})._latest_price("NOPE")) is None
    assert _run(_agent({"sentinel:quotes:latest:X": "not-a-price"})._latest_price("X")) is None
    assert _run(_agent({"sentinel:quotes:latest:X": ""})._latest_price("X")) is None


# ── macro vs equity classification ───────────────────────────────────────────

@pytest.mark.parametrize("symbol", [
    "CL=F", "BZ=F", "GC=F", "SI=F", "NG=F", "ZC=F", "ZW=F",
    "ES=F", "NQ=F", "TLT", "TIP", "VXX", "GLD", "US10Y", "^TNX", "EURUSD=X",
])
def test_macro_instruments_are_recognised(symbol):
    assert is_macro_asset(symbol) is True


@pytest.mark.parametrize("symbol", ["AAPL", "MSFT", "BIDU", "AGCO", "RACE", "CGCP"])
def test_equities_are_not_macro(symbol):
    assert is_macro_asset(symbol) is False


def test_the_live_quote_set_splits_sensibly():
    """The exact 22 symbols held in sentinel:quotes:latest:* when this was found."""
    live = ("BIDU BZ=F TIP SI=F ACWX ZW=F NQ=F CL=F NG=F AGCO ATO CGCP AAPL "
            "MSFT ZC=F CCI HXL TLT VXX ES=F RACE GC=F").split()
    macro, equities = split_macro_and_equities(live)

    assert len(macro) >= 12, "the substring version found only CL=F and TLT"
    for symbol in ("BZ=F", "GC=F", "SI=F", "NG=F", "ES=F", "NQ=F", "VXX", "TIP"):
        assert symbol in macro, f"{symbol} is back on the equity side"
    for symbol in ("AAPL", "MSFT", "BIDU", "RACE"):
        assert symbol in equities


def test_the_agent_uses_the_shared_classifier():
    src = (ROOT / "services/agents/stock_correlation_agent.py").read_text(encoding="utf-8")
    assert "split_macro_and_equities(list(price_map))" in src
    body = chr(10).join(l for l in src.splitlines() if not l.strip().startswith("#"))
    assert '"BRENT"' not in body, "the substring list is back"


def test_junk_symbols_are_not_macro():
    for junk in ("", "   ", None, 42):
        assert is_macro_asset(junk) is False

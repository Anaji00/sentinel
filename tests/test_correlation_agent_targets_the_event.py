"""
tests/test_correlation_agent_targets_the_event.py

The agent holding the most inference slots spent them on 19 instruments.

Measured over 20-21 September: stock_correlation_agent completed 282
inferences, more than any other agent, and assessed GOOGL 78 times and six
crypto pairs 136 times. A message naming no quoted equity fell back to
`equities[0]` -- the first, focus-boosted entry of the quote scan -- and the
headline was still pasted into the prompt as the "Recent Market Event", so an
al-Jazeera headline won a slot to produce another GOOGL-versus-wheat
assessment it had nothing to do with. Crypto reached the equity side because
the macro/equity split puts everything that is not macro there.

The quant engine had the same shape one field over: its crypto gate accepted
any coin `asset_class` recognises, so a headline about Trump became a TRUMP
coin analysis three times, on an asset nothing collects.
"""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.stock_correlation_agent import StockCorrelationAgent, _message_equity
from shared.utils.equities import asset_class, is_collected_crypto


def _agent(quoted: bool) -> StockCorrelationAgent:
    agent = StockCorrelationAgent.__new__(StockCorrelationAgent)
    agent.redis = MagicMock()
    agent.redis.raw.exists = AsyncMock(return_value=1 if quoted else 0)
    agent.capacity_or_defer = AsyncMock(return_value=False)
    return agent


def test_a_plain_ticker_is_read():
    assert _message_equity({"ticker": "AAPL"}) == "AAPL"
    assert _message_equity({"primary_entity": {"id": "nvda"}}) == "NVDA"
    assert _message_equity({"financial_data": {"ticker": "XOM"}}) == "XOM"


def test_a_decorated_ticker_is_cleaned():
    assert _message_equity({"ticker": "CPB ($21.53)"}) == "CPB"


def test_a_name_is_not_truncated_into_a_symbol():
    """`_clean_symbol` keeps the first word of a model's output. Applied to a
    message, "Bank of America" would become BANK."""
    assert _message_equity({"primary_entity": {"name": "Bank of America"}}) is None


def test_crypto_never_reaches_the_equity_side():
    for sym in ("AVAXUSDT", "LINKUSDT", "BTCUSDT", "ETH", "SOL", "TRUMP"):
        assert _message_equity({"ticker": sym}) is None, sym


def test_macro_instruments_are_not_the_equity_either():
    for sym in ("CL=F", "TLT", "VIX", "US10Y", "GLD"):
        assert _message_equity({"ticker": sym}) is None, sym


def test_a_message_with_no_ticker_names_nothing():
    assert _message_equity({"headline": "Strikes reported near the border"}) is None
    assert _message_equity({"ticker": ""}) is None
    assert _message_equity({"ticker": None}) is None


def test_no_equity_is_refused_before_admission():
    """Refused before the buffer, so it cannot win a slot later either."""
    agent = _agent(quoted=True)
    res = asyncio.run(agent.handle_message({"source": "aljazeera", "headline": "x"}))
    assert res is None
    agent.capacity_or_defer.assert_not_awaited()


def test_an_unquoted_equity_is_refused_before_admission():
    agent = _agent(quoted=False)
    res = asyncio.run(agent.handle_message({"ticker": "AAPL"}))
    assert res is None
    agent.capacity_or_defer.assert_not_awaited()
    agent.redis.raw.exists.assert_awaited_once_with("sentinel:quotes:latest:AAPL")


def test_a_quoted_equity_proceeds_to_admission():
    agent = _agent(quoted=True)
    asyncio.run(agent.handle_message({"ticker": "AAPL"}))
    agent.capacity_or_defer.assert_awaited_once()


def test_the_fallback_is_gone():
    src = (ROOT / "services" / "agents" / "stock_correlation_agent.py").read_text(encoding="utf-8")
    code = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
    assert "else equities[0]" not in code
    assert "target_equity = ticker" in code


def test_collected_crypto_is_btc_and_eth_only():
    for sym in ("BTC", "BTCUSDT", "ETH", "ETH-USDT-SWAP"):
        assert is_collected_crypto(sym), sym
    for sym in ("SOL", "AVAXUSDT", "TRUMP", "LINKUSDT", "AAPL", "", None):
        assert not is_collected_crypto(sym), sym


def test_recognising_a_coin_is_unchanged():
    """The wide set still classifies: narrowing it would turn SOL into an
    equity through the plain-ticker rule."""
    assert asset_class("SOL") == "crypto"
    assert asset_class("TRUMP") == "crypto"


def test_quant_gate_uses_the_collected_set():
    src = (ROOT / "services" / "agents" / "quant_trading_engine.py").read_text(encoding="utf-8")
    assert "or is_collected_crypto(ticker)" in src
    assert "or is_major_crypto(ticker)" not in src

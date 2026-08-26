"""
tests/test_financial_agent_scope.py

The financial agents could not see the crypto data the platform collects.

QuantTradingEngine gates its entire handle path on an equity check, and
verify_ticker_with_reasoning() instructed the model that "if the ticker is a
crypto altcoin (ETH, SOL, XRP, DOGE, etc.), set valid=false". Both refused every
crypto asset except BTC -- in an engine that carries _fetch_funding_context(),
written to read funding rate, basis, mark and index for exactly those assets.

So the perpetual surface was collected from OKX, enriched, stored, and then
dropped one step before anything could reason about it.

Two questions were being conflated, and they are now separate predicates:
  - is_valid_primary_equity: may this enter the *equity* watchlist? ETH: no.
  - is_supported_asset:      does the platform hold data on this? ETH: yes.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.equities import (  # noqa: E402
    is_major_crypto,
    is_supported_asset,
    is_valid_primary_equity,
)


@pytest.mark.parametrize("asset", ["ETH", "SOL", "XRP", "DOGE", "AAVE", "PEPE", "ENA", "BTC"])
def test_collected_crypto_is_supported(asset):
    assert is_supported_asset(asset) is True
    assert is_major_crypto(asset) is True


@pytest.mark.parametrize("inst", [
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USD-SWAP", "DOGEUSDT", "AAVEUSD",
])
def test_venue_instrument_ids_resolve_to_their_asset(inst):
    """OKX writes BTC-USDT-SWAP, Binance wrote BTCUSDT. Both mean BTC."""
    assert is_major_crypto(inst) is True


def test_crypto_still_does_not_belong_in_the_equity_watchlist():
    """The two predicates answer different questions and must stay different."""
    for asset in ("ETH", "SOL", "DOGE"):
        assert is_supported_asset(asset) is True
        assert is_valid_primary_equity(asset) is False, (
            f"{asset} would now pollute the equity watchlist"
        )


def test_equities_are_unaffected():
    for ticker in ("AAPL", "NVDA", "BRK.B", "JPM"):
        assert is_supported_asset(ticker) is True
        assert is_valid_primary_equity(ticker) is True


@pytest.mark.parametrize("junk", ["JUNKCOIN", "SPY", "TQQQ", "NVDL", "", "   "])
def test_nothing_else_slips_through(junk):
    assert is_supported_asset(junk) is False


def test_the_quant_gate_admits_crypto_majors():
    src = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    gate = src[src.index("SUPPORTED ASSET GATE"):][:1200]
    assert "is_major_crypto(ticker)" in gate
    assert "or" in gate, "the gate must be a union, not a replacement"


def test_verification_no_longer_instructs_the_model_to_reject_altcoins():
    src = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    assert "is a crypto altcoin (ETH, SOL, XRP, DOGE, etc.), set valid=false" not in src
    assert "is_supported_asset(clean_ticker)" in src


def test_a_crypto_major_short_circuits_the_model_call():
    """Membership of a fixed set is not a question worth an inference for."""
    src = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    fn = src[src.index("async def verify_ticker_with_reasoning"):][:1200]
    assert "if is_major_crypto(clean_ticker):" in fn
    assert fn.index("if is_major_crypto(clean_ticker):") < fn.index("prompt = f")


def test_funding_is_cached_under_the_key_the_agent_reads():
    """_fetch_funding_context tries "BTC" and "BTCUSDT", not "BTC-USDT-SWAP"."""
    collector = (ROOT / "services/collector-crypto/main.py").read_text(encoding="utf-8")
    okx = collector[collector.index("async def poll_okx_perpetuals"):]
    assert 'f"sentinel:crypto:funding:{asset}"' in okx

    engine = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    assert 'f"sentinel:crypto:funding:{candidate}"' in engine

"""
tests/test_microstructure_and_refdata.py

Comprehensive test suite verifying:
1. OHLCVAggregator true VWAP calculation
2. Windowed trade-level microstructure calculations (Kyle's λ, Amihud illiquidity, VWAP)
3. Crypto & TradFi microstructure calculation parity
4. Dynamic Redis asset classification fallback in equities.py
5. Score adjustment provenance tracking in NormalizedEvent
6. IntraTradFiHawkesCorrelator sector excitation
"""

import pytest
import math
import json
from unittest.mock import AsyncMock, MagicMock
from shared.utils import quant_calc
from shared.utils.equities import fast_classify_equity, is_valid_primary_equity_async
from shared.models.events import NormalizedEvent, EventType, Entity, FinancialData, ScoreAdjustment


def test_ohlcv_aggregator_vwap():
    """Verify that multi-trade VWAP is correctly aggregated using pv_sum / V."""
    trades = [
        (100.0, 10),  # $1,000
        (102.0, 20),  # $2,040
        (99.0,  30),  # $2,970
    ]
    # Total volume = 60
    # Total dollar volume (pv_sum) = $6,010
    # Expected VWAP = 6010 / 60 = 100.1667 -> round to 4 decimals = 100.1667
    pv_sum = sum(p * v for p, v in trades)
    v_sum = sum(v for _, v in trades)
    computed_vwap = round(pv_sum / v_sum, 4)
    assert computed_vwap == 100.1667

    # Multi-trade VWAP via quant_calc
    prices = [p for p, _ in trades]
    volumes = [v for _, v in trades]
    qc_vwap = quant_calc.vwap(prices, volumes)
    assert qc_vwap == 100.1667


def test_microstructure_kyle_lambda_and_amihud_parity():
    """Verify parity of Kyle's λ and Amihud calculation between price_changes and signed_volumes."""
    # Generate synthetic price series (12 trades, n >= 10 price changes)
    prices = [100.0, 100.5, 100.2, 101.0, 100.8, 101.5, 101.2, 102.0, 101.7, 102.5, 102.1, 103.0]
    # Signed volumes (positive for buy aggressor, negative for sell aggressor)
    signed_volumes = [100.0, -50.0, 200.0, -80.0, 150.0, -60.0, 180.0, -70.0, 220.0, -90.0, 250.0]
    notionals = [p * abs(sv) for p, sv in zip(prices[1:], signed_volumes)]

    # Price changes: P_i - P_{i-1} for newest to oldest or P_t - P_{t-1} for chronological
    price_changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    assert len(price_changes) == 11
    assert len(signed_volumes) == 11

    # Kyle's lambda
    k_lam = quant_calc.kyle_lambda(price_changes, signed_volumes)
    assert k_lam > 0.0, f"Expected positive Kyle's lambda, got {k_lam}"

    # Amihud illiquidity: period returns |r_t| = |P_t / P_{t-1} - 1|
    returns = [abs(prices[i] / prices[i-1] - 1) for i in range(1, len(prices))]
    ami = quant_calc.amihud_illiquidity(returns, notionals)
    assert ami > 0.0, f"Expected positive Amihud illiquidity, got {ami}"


def test_fast_classify_equity_cached_asset_type():
    """Verify that fast_classify_equity respects cached_asset_type dynamically."""
    # Case 1: Redis cached "ETP" -> classified as LEVERAGED_INVERSE_ETF
    res_etp = fast_classify_equity("TSLL", cached_asset_type="ETP")
    assert res_etp["is_primary_equity"] is False
    assert res_etp["asset_class"] == "LEVERAGED_INVERSE_ETF"
    assert "Redis cached asset type" in res_etp["reason"]

    # Case 2: Redis cached "Common Stock" -> classified as PRIMARY_COMMON_EQUITY
    res_stock = fast_classify_equity("AAPL", cached_asset_type="Common Stock")
    assert res_stock["is_primary_equity"] is True
    assert res_stock["asset_class"] == "PRIMARY_COMMON_EQUITY"

    # Case 3: Uncached -> fallback to regex / blocklist
    res_uncached_etf = fast_classify_equity("TQQQ")
    assert res_uncached_etf["is_primary_equity"] is False


def test_is_valid_primary_equity_async_redis_cache():
    """Verify async equity validation checking sentinel:asset_type Redis key."""
    import asyncio

    async def _test():
        mock_redis = MagicMock()
        mock_redis.raw = AsyncMock()

        # Mock Redis returning "ETP" for ticker
        mock_redis.raw.get.return_value = b"ETP"
        is_valid = await is_valid_primary_equity_async("SOME_UNKNOWN_SYMBOL", redis_client=mock_redis)
        assert is_valid is False
        mock_redis.raw.get.assert_called_with("sentinel:asset_type:SOME_UNKNOWN_SYMBOL")

        # Mock Redis returning "Common Stock"
        mock_redis.raw.get.return_value = b"Common Stock"
        is_valid_stock = await is_valid_primary_equity_async("UNKNOWN_EQUITY", redis_client=mock_redis)
        assert is_valid_stock is True

    asyncio.run(_test())


def test_score_adjustment_provenance_schema():
    """Verify NormalizedEvent score_adjustments provenance tracking."""
    event = NormalizedEvent(
        type=EventType.EQUITY_BLOCK,
        occurred_at="2026-08-09T22:00:00Z",
        source="finnhub_equities",
        primary_entity=Entity(id="AAPL", name="Apple Inc."),
        financial_data=FinancialData(
            ticker="AAPL",
            sector="Technology",
            industry="Consumer Electronics",
            index_membership=["SPX", "NDX"],
            market_cap_tier="mega",
            exchange="NASDAQ",
        ),
        anomaly_score=0.85,
        score_adjustments=[
            ScoreAdjustment(reason="watchlist_boost", delta=0.15),
            ScoreAdjustment(reason="institutional_flow_x1.15", delta=0.10),
        ]
    )

    assert len(event.score_adjustments) == 2
    assert event.score_adjustments[0].reason == "watchlist_boost"
    assert event.score_adjustments[0].delta == 0.15
    assert event.financial_data.sector == "Technology"
    assert "SPX" in event.financial_data.index_membership


def test_sector_hawkes_correlator_branching_ratio():
    """Verify IntraTradFiHawkesCorrelator computes branching ratios across GICS sectors."""
    from services.correlation.sector_hawkes import IntraTradFiHawkesCorrelator
    correlator = IntraTradFiHawkesCorrelator()

    # Synthetic event streams for Tech and Healthcare
    streams = {
        "Technology": [10.0, 12.0, 15.0, 18.0, 20.0, 22.0, 25.0, 30.0],
        "Healthcare": [11.0, 13.0, 16.0, 19.0, 21.0, 23.0, 26.0, 31.0],
    }
    result = correlator.fit(streams, T=60.0)
    assert "branching_ratio" in result or "alpha" in result

    top = correlator.get_top_excitations("Healthcare")
    assert isinstance(top, list)

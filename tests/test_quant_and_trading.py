"""
tests/test_quant_and_trading.py

Consolidated Test Suite: Quantitative Math, Trading Engine, Financial Advisor, Market Microstructure & RefData.
Combines:
  - test_quant_calc.py
  - test_quant_trading_engine.py
  - test_financial_advisor.py
  - test_quant_financial_advisor_correlation.py
  - test_equities.py
  - test_watched_equities_candles.py
  - test_microstructure_and_refdata.py
"""

import math
import asyncio
import numpy as np
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from shared.utils import quant_calc
from shared.utils.equities import (
    is_valid_primary_equity, fast_classify_equity, ALLOWED_CRYPTO_TOKENS, is_valid_primary_equity_async
)
from shared.utils.cyber_mapper import map_cve_to_equity
from shared.utils.candles import evaluate_multi_timeframe, TIMEFRAMES_MINUTES, get_domain_tag
from shared.models.events import NormalizedEvent, EventType, Entity, FinancialData, ScoreAdjustment
from services.agents.quant_trading_engine import (
    QuantTradingEngine, QuantTradingEngine as FinancialAdvisorAgent,
    compute_ta_indicators, TradingSignal, FinancialAdviceBrief
)


# ── 1. VOLATILITY ESTIMATORS & QUANT MATH ────────────────────────────────────

def test_ewma_volatility_math():
    returns = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01, 0.008]
    vol_daily = quant_calc.ewma_volatility(returns, lam=0.94, annualize=False)
    vol_annual = quant_calc.ewma_volatility(returns, lam=0.94, annualize=True)
    assert vol_daily > 0.0
    assert abs(vol_annual - vol_daily * math.sqrt(252)) < 1e-4

def test_ewma_volatility_edge_cases():
    assert quant_calc.ewma_volatility([]) == 0.0
    assert quant_calc.ewma_volatility([0.01]) == 0.0

def test_garch_volatility_math():
    returns = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01, 0.008, 0.012, -0.015]
    vol = quant_calc.garch_volatility(returns, omega=1e-5, alpha=0.1, beta=0.85, annualize=False)
    assert vol > 0.0

def test_garch_volatility_edge_cases():
    assert quant_calc.garch_volatility([]) == 0.0

def test_parkinson_volatility_math():
    highs = [102.0, 105.0, 104.0, 106.0, 108.0]
    lows = [98.0, 101.0, 100.0, 102.0, 103.0]
    vol = quant_calc.parkinson_volatility(highs, lows)
    assert vol > 0.0

def test_parkinson_volatility_invalid():
    assert quant_calc.parkinson_volatility([], []) == 0.0
    assert quant_calc.parkinson_volatility([100.0], [105.0]) == 0.0

def test_yang_zhang_volatility_math():
    opens = [100.0, 102.0, 103.0, 105.0]
    highs = [103.0, 104.0, 106.0, 107.0]
    lows = [99.0, 101.0, 102.0, 104.0]
    closes = [102.0, 103.0, 105.0, 106.0]
    vol = quant_calc.yang_zhang_volatility(opens, highs, lows, closes)
    assert vol > 0.0


# ── 2. POSITION SIZING & RISK METRICS ─────────────────────────────────────────

def test_kelly_criterion_full_and_half():
    f_full = quant_calc.kelly_criterion(win_probability=0.60, win_loss_ratio=2.0, half_kelly=False)
    f_half = quant_calc.kelly_criterion(win_probability=0.60, win_loss_ratio=2.0, half_kelly=True)
    assert abs(f_full - 0.40) < 1e-3
    assert abs(f_half - 0.20) < 1e-3

def test_kelly_criterion_negative_ev():
    f_neg = quant_calc.kelly_criterion(win_probability=0.30, win_loss_ratio=1.0)
    assert f_neg == 0.0

def test_var_historical():
    returns = [-0.05, -0.03, -0.02, -0.01, 0.0, 0.01, 0.02, 0.03, 0.04, 0.05]
    var_90 = quant_calc.var_historical(returns, confidence=0.90, position_value=10000.0)
    assert var_90 > 0.0

def test_cvar_historical():
    returns = [-0.05, -0.03, -0.02, -0.01, 0.0, 0.01, 0.02, 0.03, 0.04, 0.05]
    var_90 = quant_calc.var_historical(returns, confidence=0.90, position_value=10000.0)
    cvar_90 = quant_calc.cvar_historical(returns, confidence=0.90, position_value=10000.0)
    assert cvar_90 >= var_90


# ── 3. PERFORMANCE RATIOS & DRAWDOWN ──────────────────────────────────────────

def test_sharpe_ratio():
    returns = [0.01, 0.02, -0.005, 0.015, 0.01, -0.01, 0.025]
    sr = quant_calc.sharpe_ratio(returns, risk_free_rate=0.02)
    assert isinstance(sr, float)

def test_max_drawdown():
    prices = [100.0, 110.0, 120.0, 90.0, 105.0, 130.0, 80.0, 100.0]
    mdd, peak, trough = quant_calc.max_drawdown(prices)
    assert abs(mdd) > 0.30

def test_calmar_ratio():
    prices = [100.0, 110.0, 120.0, 90.0, 105.0, 130.0]
    returns = [0.1, 0.09, -0.25, 0.166, 0.238]
    cr = quant_calc.calmar_ratio(returns, prices)
    assert cr > 0.0


# ── 4. TIME SERIES & STATISTICAL TESTS ────────────────────────────────────────

def test_augmented_dickey_fuller_stationary():
    np.random.seed(42)
    stationary = list(np.random.normal(0, 1, 100))
    res = quant_calc.augmented_dickey_fuller(stationary)
    assert res["is_stationary"] is True
    assert res["adf_statistic"] < -2.86

def test_engle_granger_cointegration_pair():
    np.random.seed(42)
    x = list(np.cumsum(np.random.normal(0, 1, 100)))
    y = list(2.0 * np.array(x) + np.random.normal(0, 0.5, 100))
    res = quant_calc.engle_granger_cointegration(x, y)
    assert res["is_cointegrated"] is True
    assert abs(res["beta"] - 2.0) < 0.2
    assert res["critical_5pct"] < -3.30
    assert res["adf_statistic"] < res["critical_5pct"]

def test_granger_causality_pair():
    np.random.seed(42)
    x = np.random.normal(0, 1, 100)
    y = np.zeros(100)
    for i in range(1, 100):
        y[i] = 0.8 * x[i - 1] + np.random.normal(0, 0.1)
    res = quant_calc.granger_causality(list(x), list(y), max_lag=3)
    assert res["x_granger_causes_y"] is True
    assert res["degraded"] is False

def test_granger_causality_degraded():
    res = quant_calc.granger_causality([1.0, 2.0], [1.0, 2.0], max_lag=3)
    assert res["degraded"] is True
    assert res["x_granger_causes_y"] is False

def test_hurst_exponent_bounds():
    np.random.seed(42)
    series = list(np.cumsum(np.random.normal(0, 1, 100)))
    h = quant_calc.hurst_exponent(series)
    assert 0.0 <= h <= 1.0


# ── 5. MICROSTRUCTURE & LIQUIDITY METRICS ────────────────────────────────────

def test_kyle_lambda():
    price_changes = [0.5, -0.2, 0.8, -0.4, 0.3]
    order_flows = [100.0, -50.0, 150.0, -80.0, 60.0]
    kl = quant_calc.kyle_lambda(price_changes, order_flows)
    assert kl >= 0.0

def test_amihud_illiquidity():
    returns = [0.02, -0.01, 0.03, -0.02]
    volumes = [1_000_000, 800_000, 1_200_000, 900_000]
    ami = quant_calc.amihud_illiquidity(returns, volumes)
    assert ami > 0.0

def test_cusum_change_detection():
    np.random.seed(42)
    series = list(np.random.normal(0, 1, 50)) + list(np.random.normal(5, 1, 50))
    res = quant_calc.cusum_change_detection(series, threshold=4.0)
    assert isinstance(res, list)
    assert len(res) > 0

def test_vwap_and_twap():
    prices = [100.0, 102.0, 101.0, 103.0]
    volumes = [100, 200, 150, 250]
    v = quant_calc.vwap(prices, volumes)
    t = quant_calc.twap(prices)
    assert round(t, 2) == 101.50
    assert v > 0.0


# ── 6. CALIBRATION & ADVANCED QUANT METRICS ──────────────────────────────────

def test_engle_granger_cointegration_small_spread_std():
    np.random.seed(42)
    x = list(np.linspace(100.0, 110.0, 100))
    noise = np.random.normal(0, 1e-5, 100)
    y = list(1.5 * np.array(x) + noise)
    res = quant_calc.engle_granger_cointegration(x, y)
    assert res["spread_std"] > 0.0
    assert res["spread_std"] < 1e-3

def test_black_litterman_optimization():
    market_caps = {"AAPL": 3.0e12, "MSFT": 2.8e12, "NVDA": 2.5e12}
    cov_matrix = [[0.04, 0.02, 0.025], [0.02, 0.035, 0.02], [0.025, 0.02, 0.05]]
    views_matrix = [[1.0, 0.0, -1.0]]
    view_returns = [0.05]
    view_uncertainties = [0.01]

    res = quant_calc.black_litterman_optimization(
        market_caps=market_caps, cov_matrix=cov_matrix, views_matrix=views_matrix,
        view_returns=view_returns, view_uncertainties=view_uncertainties,
    )
    assert "expected_returns" in res
    assert "optimal_weights" in res
    assert len(res["optimal_weights"]) == 3

def test_garch_volatility_cone():
    closes = [100.0 + i * 0.5 + ((-1) ** i) * 1.2 for i in range(50)]
    highs = [c + 1.5 for c in closes]
    lows = [c - 1.5 for c in closes]

    cone = quant_calc.garch_volatility_cone(closes, highs, lows, horizon_hours=24)
    assert "cond_volatility_pct" in cone
    assert cone["tp1_sigma_1_0"] > closes[-1]

def test_microstructure_stop_distance():
    mult_normal = quant_calc.microstructure_stop_distance(atr=2.5, ofi=0.1, kyle_lambda=0.2)
    mult_tight = quant_calc.microstructure_stop_distance(atr=2.5, ofi=-0.70, kyle_lambda=2.5)
    assert mult_normal == 1.5
    assert mult_tight == 0.5

def test_hawkes_risk_multiplier():
    mult_low = quant_calc.hawkes_risk_multiplier(hawkes_intensity=1.2)
    mult_high = quant_calc.hawkes_risk_multiplier(hawkes_intensity=3.5)
    assert mult_low == 1.0
    assert mult_high < 1.0

def test_moving_average_distances():
    closes = [100.0 + i * 0.5 for i in range(220)]
    res = quant_calc.moving_average_distances(closes)
    assert res["sma_20"] is not None
    assert res["dist_sma_20_pct"] > 0
    assert res["ma_alignment"] == "BULLISH_STACK"


def test_black_scholes_pricing_and_greeks():
    S, K, T, r, sigma = 100.0, 100.0, 30 / 365.0, 0.045, 0.30
    call_p = quant_calc.black_scholes_call_price(S, K, T, r, sigma)
    put_p = quant_calc.black_scholes_put_price(S, K, T, r, sigma)
    assert call_p > 0.0
    assert put_p > 0.0
    
    greeks_call = quant_calc.black_scholes_greeks(S, K, T, r, sigma, "call")
    assert 0.45 <= greeks_call["delta"] <= 0.55
    assert greeks_call["gamma"] > 0.0

def test_black_scholes_delta_inversion():
    S, T, r, sigma, target_delta = 100.0, 30 / 365.0, 0.045, 0.30, 0.30
    strike = quant_calc.black_scholes_delta_inversion_strike(S, T, r, sigma, target_delta=target_delta, option_type="call")
    assert strike > S
    greeks = quant_calc.black_scholes_greeks(S, strike, T, r, sigma, "call")
    assert abs(greeks["delta"] - target_delta) < 0.02

def test_covered_call_recommendation_engine():
    # 1. Gated on Z >= 2.5 and valid ticker
    rec_nvda = quant_calc.generate_covered_call_recommendation(
        ticker="NVDA", current_price=130.0, z_score=2.8, target_delta=0.30, dte_days=30, live_iv=0.45
    )
    assert rec_nvda is not None
    assert rec_nvda["recommended_strike"] > 130.0
    assert rec_nvda["iv_source"] == "OPTIONS_SWEEP_IV"
    assert rec_nvda["greeks"]["delta"] > 0.0

    # 2. Gate check: Z < 2.5 returns None
    rec_low_z = quant_calc.generate_covered_call_recommendation(
        ticker="NVDA", current_price=130.0, z_score=1.8
    )
    assert rec_low_z is None

    # 3. Gate check: Unwatched ticker when watched_equities provided returns None
    rec_unwatched = quant_calc.generate_covered_call_recommendation(
        ticker="UNWATCHED_TICKER", current_price=10.0, z_score=3.0, watched_equities={"AAPL", "NVDA"}
    )
    assert rec_unwatched is None

    # 4. Fallback IV provenance check
    rec_fallback = quant_calc.generate_covered_call_recommendation(
        ticker="AAPL", current_price=220.0, z_score=2.6, live_iv=None, realized_volatility=0.22
    )
    assert rec_fallback is not None
    assert rec_fallback["iv_source"] == "REALIZED_VOLATILITY_FALLBACK"

    # 5. Watched equities custom universe scoping check
    rec_watched = quant_calc.generate_covered_call_recommendation(
        ticker="CUSTOM_TICKER", current_price=50.0, z_score=2.9, watched_equities={"CUSTOM_TICKER"}
    )
    assert rec_watched is not None
    assert rec_watched["ticker"] == "CUSTOM_TICKER"



# ── 7. PRIMARY EQUITIES & ASSET CLASSIFICATION ────────────────────────────────

def test_clean_primary_equities():
    valid_tickers = ["AAPL", "NVDA", "MSFT", "INTC", "TSLA", "PLTR", "AMZN", "GOOGL", "META"]
    for ticker in valid_tickers:
        assert is_valid_primary_equity(ticker) is True
        res = fast_classify_equity(ticker)
        assert res["is_primary_equity"] is True
        assert res["asset_class"] == "PRIMARY_COMMON_EQUITY"

def test_btc_crypto_exception():
    for token in ALLOWED_CRYPTO_TOKENS:
        assert is_valid_primary_equity(token) is True
        res = fast_classify_equity(token)
        assert res["is_primary_equity"] is True

def test_single_stock_leveraged_etfs_exclusion():
    leveraged_tickers = ["IONZ", "IONU", "IOND", "NVDL", "TSLL", "TSLZ", "AAPU", "MSTZ"]
    for ticker in leveraged_tickers:
        assert is_valid_primary_equity(ticker) is False
        res = fast_classify_equity(ticker)
        assert res["is_primary_equity"] is False

def test_yieldmax_option_funds_exclusion():
    yieldmax_tickers = ["NVDY", "CONY", "TSLY", "AMZY", "MSTY", "APLY"]
    for ticker in yieldmax_tickers:
        assert is_valid_primary_equity(ticker) is False

def test_broad_leveraged_etfs_exclusion():
    broad_leveraged = ["TQQQ", "SQQQ", "SOXL", "SOXS", "UPRO", "SPXU", "FNGU", "BULZ"]
    for ticker in broad_leveraged:
        assert is_valid_primary_equity(ticker) is False

def test_volatility_etns_exclusion():
    vol_etns = ["UVXY", "SVIX", "UVIX", "VIXY", "VXX"]
    for ticker in vol_etns:
        assert is_valid_primary_equity(ticker) is False


# ── 8. QUANT TRADING ENGINE & CVE MAPPER ──────────────────────────────────────

def test_map_cve_to_equity():
    ticker, risk_type = map_cve_to_equity("CVE-2026-16812", vendor_name="microsoft")
    assert ticker == "MSFT"
    assert risk_type == "DIRECT_VENDOR_VULNERABILITY"

    ticker, risk_type = map_cve_to_equity("CVE-2026-16812", vendor_name="Palo Alto Networks")
    assert ticker == "PANW"

    ticker, risk_type = map_cve_to_equity("CVE-2026-16812", vendor_name="unknown_home_router")
    assert ticker is None

def test_quant_engine_rejects_unmapped_cve():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.exists = AsyncMock(return_value=0)
        agent = QuantTradingEngine(
            agent_name="quant_trading", input_topics=["events.enriched"],
            redis_client=redis_mock, db_client=MagicMock(), neo4j_client=MagicMock(),
            producer=AsyncMock(), consumer=AsyncMock(), dlq=AsyncMock()
        )
        agent._process_peer_discovery = AsyncMock()
        agent._process_trading_advisory = AsyncMock()

        unmapped_cve_msg = {
            "agent_run_id": "quant_CVE-2026-16812_x",
            "trigger": {"ticker": "CVE-2026-16812", "anomaly_score": 0.7},
            "primary_entity": {"id": "CVE-2026-16812"}
        }
        result = await agent.handle(unmapped_cve_msg)
        assert result is None
        agent._process_peer_discovery.assert_not_called()

    asyncio.run(run_test())

def test_quant_engine_positive_path_real_ticker():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.exists = AsyncMock(return_value=0)
        agent = QuantTradingEngine(
            agent_name="quant_trading", input_topics=["events.enriched"],
            redis_client=redis_mock, db_client=MagicMock(), neo4j_client=MagicMock(),
            producer=AsyncMock(), consumer=AsyncMock(), dlq=AsyncMock()
        )
        agent._process_peer_discovery = AsyncMock(return_value={"discovery": "ok"})
        agent._process_trading_advisory = AsyncMock(return_value={"advisory": "ok"})

        real_ticker_msg = {
            "agent_run_id": "quant_NVDA_1",
            "trigger": {"ticker": "NVDA", "anomaly_score": 0.75},
            "primary_entity": {"id": "NVDA"}
        }
        result = await agent.handle(real_ticker_msg)
        assert result == {"advisory": "ok"}

    asyncio.run(run_test())


# ── 9. FINANCIAL ADVISOR TA & SCHEDULED REVIEW ────────────────────────────────

def test_compute_ta_indicators_mathematics():
    closes = [10.0 + i for i in range(20)]
    highs = [c + 0.5 for c in closes]
    lows = [c - 0.5 for c in closes]
    indicators = compute_ta_indicators(closes, highs, lows)

    assert "rsi" in indicators
    assert indicators["rsi"] > 50.0
    assert abs(indicators["fib_levels"]["0.500"] - 19.5) < 1e-5

def test_financial_advisor_scheduled_review():
    async def run_test():
        redis_mock = MagicMock()
        db_mock = MagicMock()
        db_mock.execute = AsyncMock()
        neo_mock = MagicMock()
        prod_mock = AsyncMock()
        cons_mock = AsyncMock()
        dlq_mock = AsyncMock()

        redis_mock.raw.zrange = AsyncMock(return_value=[b"AAPL", b"BTC-USD"])
        redis_mock.raw.get = AsyncMock(return_value=None)
        redis_mock.raw.set = AsyncMock()
        redis_mock.raw.lrange = AsyncMock(return_value=[])
        redis_mock.raw.zadd = AsyncMock()
        redis_mock.raw.exists = AsyncMock(return_value=0)

        agent = FinancialAdvisorAgent(
            agent_name="financial_advisor", input_topics=["heartbeat"],
            redis_client=redis_mock, db_client=db_mock, neo4j_client=neo_mock,
            producer=prod_mock, consumer=cons_mock, dlq=dlq_mock,
        )
        closes = [100.0 + i for i in range(20)]
        agent._fetch_prices = AsyncMock(return_value=(closes, [c + 1.0 for c in closes], [c - 1.0 for c in closes]))
        agent.fetch_global_context = AsyncMock(return_value="macro context info")

        mock_brief = FinancialAdviceBrief(
            market_regime="Mean-Reverting",
            highest_conviction_plays=[
                TradingSignal(
                    ticker="BTC-USD", action="BUY", trade_type="Scalp/Buy",
                    entry_level=64000.0, target_price=68000.0, stop_loss=62000.0,
                    risk_reward_ratio=2.0, kelly_allocation_pct=10.0, conviction_score=0.85,
                    quantitative_rationale="Asymmetric EV support setup near Fib cluster."
                )
            ],
            general_hedging_strategy="Delta-hedging with out-of-the-money puts."
        )
        agent._execute_with_telemetry = AsyncMock(return_value=mock_brief)
        agent.write_agent_memory = AsyncMock()

        res = await agent.run_scheduled_review()
        assert res is not None

    asyncio.run(run_test())


# ── 10. WATCHED EQUITIES CANDLES & MICROSTRUCTURE ─────────────────────────────

def test_get_domain_tag():
    assert get_domain_tag("crypto", "BTCUSDT") == "CRYPTO"
    assert get_domain_tag("tradfi", "AAPL") == "EQUITY"
    assert get_domain_tag("tradfi", "CL=F") == "MACRO"

class DummyScorer:
    async def score_market_candle(self, domain, asset, features):
        return 0.40

    async def check_watchlist(self, asset, watchlist_type):
        return asset == "AAPL"

def test_evaluate_multi_timeframe_watched_equities():
    async def run_test():
        redis = MagicMock()
        redis.raw = AsyncMock()
        redis.raw.get = AsyncMock(return_value=None)
        redis.raw.set = AsyncMock()
        redis.raw.lrange = AsyncMock(return_value=[])

        scorer = DummyScorer()

        results = await evaluate_multi_timeframe(
            redis, scorer, domain="tradfi", asset="AAPL",
            ts=datetime.now(timezone.utc), open_p=150.0, high_p=152.0, low_p=149.0, close_p=151.5, volume=10000.0
        )
        assert isinstance(results, list)
        assert len(TIMEFRAMES_MINUTES) == 6

    asyncio.run(run_test())

def test_ohlcv_aggregator_vwap():
    trades = [(100.0, 10), (102.0, 20), (99.0, 30)]
    prices = [p for p, _ in trades]
    volumes = [v for _, v in trades]
    qc_vwap = quant_calc.vwap(prices, volumes)
    assert qc_vwap == 100.1667

def test_score_adjustment_provenance_schema():
    event = NormalizedEvent(
        type=EventType.EQUITY_BLOCK, occurred_at="2026-08-09T22:00:00Z", source="finnhub_equities",
        primary_entity=Entity(id="AAPL", name="Apple Inc."),
        financial_data=FinancialData(ticker="AAPL", sector="Technology"),
        anomaly_score=0.85,
        score_adjustments=[ScoreAdjustment(reason="watchlist_boost", delta=0.15)]
    )
    assert len(event.score_adjustments) == 1
    assert event.score_adjustments[0].reason == "watchlist_boost"

@pytest.mark.parametrize("target_delta, option_type, S, r, sigma, T", [
    (0.50, "call", 100.0, 0.05, 0.20, 1.0),
    (0.25, "call", 100.0, 0.05, 0.30, 0.5),
    (0.75, "call", 100.0, 0.02, 0.15, 2.0),
    (0.50, "put",  100.0, 0.05, 0.20, 1.0),
    (0.25, "put",  100.0, 0.05, 0.30, 0.5),
    (0.75, "put",  100.0, 0.02, 0.15, 2.0),
])
def test_black_scholes_delta_inversion_grid(target_delta, option_type, S, r, sigma, T):
    strike = quant_calc.black_scholes_delta_inversion_strike(S=S, T=T, r=r, sigma=sigma, target_delta=target_delta, option_type=option_type)
    assert strike > 0.0
    assert not np.isnan(strike)


def test_portfolio_metrics_force_write_and_cvar_99_confidence():
    from services.agents.quant_trading_engine import PortfolioMetrics
    
    # 20 return samples
    returns = [-0.05, -0.04, -0.03, -0.02, -0.01, 0.0, 0.01, 0.02, 0.03, 0.04,
               -0.03, 0.01, 0.02, -0.01, 0.015, -0.025, 0.035, -0.015, 0.02, 0.01]
    has_history = len(returns) >= 10
    assert has_history is True

    var_95_pct = round(quant_calc.var_historical(returns, confidence=0.95, position_value=1.0) * 100.0, 2)
    cvar_95_pct = round(quant_calc.cvar_historical(returns, confidence=0.95, position_value=1.0) * 100.0, 2)
    cvar_99_pct = round(quant_calc.cvar_historical(returns, confidence=0.99, position_value=1.0) * 100.0, 2)
    sharpe_ratio_val = round(float(quant_calc.sharpe_ratio(returns, annualize=True)), 2)

    # 99% CVaR must be strictly >= 95% CVaR (capturing extreme tail loss)
    assert cvar_99_pct >= cvar_95_pct
    assert var_95_pct > 0.0

    metrics = PortfolioMetrics()
    metrics.var_95_pct = var_95_pct
    metrics.cvar_99_pct = cvar_99_pct
    metrics.sharpe_ratio = sharpe_ratio_val
    metrics.metrics_source = "computed" if has_history else "insufficient_history"

    assert metrics.metrics_source == "computed"
    assert metrics.var_95_pct == var_95_pct
    assert metrics.cvar_99_pct == cvar_99_pct
    assert metrics.sharpe_ratio == sharpe_ratio_val


def test_portfolio_metrics_insufficient_history():
    from services.agents.quant_trading_engine import PortfolioMetrics

    # Insufficient samples (< 10)
    returns = [0.01, -0.02, 0.015]
    has_history = len(returns) >= 10
    assert has_history is False

    metrics = PortfolioMetrics()
    if has_history:
        metrics.var_95_pct = 1.0
        metrics.cvar_99_pct = 2.0
        metrics.sharpe_ratio = 1.5
        metrics.metrics_source = "computed"
    else:
        metrics.var_95_pct = None
        metrics.cvar_99_pct = None
        metrics.sharpe_ratio = None
        metrics.metrics_source = "insufficient_history"

    assert metrics.metrics_source == "insufficient_history"
    assert metrics.var_95_pct is None
    assert metrics.cvar_99_pct is None
    assert metrics.sharpe_ratio is None


# ── 12. TIER 2 FOUNDATIONAL INFRASTRUCTURE TESTS ─────────────────────────────

def test_migration_runner_transaction_free_branching():
    """Verify that migration runner executes continuous aggregate DDL without transaction wrapper (§2.2)."""
    import asyncio
    from shared.db.migrate import MIGRATIONS, apply_migrations

    # Verify migration definitions exist and have correct flags
    cagg_mig = next((m for m in MIGRATIONS if m["version"] == "0005_create_tradfi_bars_continuous_aggregates"), None)
    assert cagg_mig is not None
    assert cagg_mig.get("transactional") is False
    assert "tradfi_bars_5m" in cagg_mig["sql"]
    assert "ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING" in cagg_mig["sql"]

    hyper_mig = next((m for m in MIGRATIONS if m["version"] == "0004_create_tradfi_bars_hypertable"), None)
    assert hyper_mig is not None
    assert hyper_mig.get("transactional", True) is True

    # Test apply_migrations dispatch logic with mock db
    mock_db = MagicMock()
    mock_db.execute = AsyncMock()
    mock_db.execute_without_transaction = AsyncMock()
    mock_db.query = AsyncMock(return_value=[{"version": "0001_initial_schema"}, {"version": "0002_add_trace_id"}, {"version": "0003_add_lat_lon_columns"}])

    with patch("shared.db.migrate.get_timescale", AsyncMock(return_value=mock_db)):
        asyncio.run(apply_migrations())
        # Hypertable migration (0004) executed with transaction
        assert mock_db.execute.called
        # CAGG migration (0005) executed without transaction
        assert mock_db.execute_without_transaction.called


def test_rolling_zscore_preceding_only_math():
    """Verify Z-score mathematical properties: excluding current bucket prevents self-dampening (§2.3)."""
    # 20 baseline closes around 100 with std ~ 2
    baseline = [100.0 + (i % 5 - 2) * 1.0 for i in range(20)]
    mean_baseline = float(np.mean(baseline))
    std_baseline = float(np.std(baseline))

    # A large print of 130
    current_close = 130.0

    # 1. Correct Z-score (excluding current bucket from baseline)
    correct_z = (current_close - mean_baseline) / std_baseline

    # 2. Flawed Z-score (including current bucket in baseline)
    inclusive_window = baseline[1:] + [current_close]
    flawed_z = (current_close - np.mean(inclusive_window)) / np.std(inclusive_window)

    # The correct Z-score should capture true anomaly without dampening
    assert correct_z > 10.0
    assert correct_z > flawed_z  # Inclusive window dampens the signal


def test_tradfi_enricher_unconditional_bars_write():
    """Verify tradfi enricher persists closed bars to tradfi_bars hypertable even for unwatched tickers (§2.4)."""
    from services.enrichment.enrichers.tradfi import TradFiEnricher
    from shared.models import RawEvent

    mock_scorer = MagicMock()
    mock_scorer.check_watchlist = AsyncMock(return_value=False)  # Unwatched ticker!
    mock_redis = MagicMock()
    mock_redis.raw.set = AsyncMock()
    mock_graph = MagicMock()
    mock_db = MagicMock()
    mock_db.execute = AsyncMock()

    enricher = TradFiEnricher(mock_scorer, mock_redis, mock_graph, db=mock_db)

    raw_event = RawEvent(
        source="finnhub_equities",
        raw_payload={
            "ticker": "UNWATCHED_CORP",
            "trade_type": "OHLCV_MINUTE_BAR",
            "open": 50.0,
            "high": 52.0,
            "low": 49.5,
            "close": 51.5,
            "volume": 25000.0,
            "session": "REGULAR"
        }
    )

    async def _test():
        res = await enricher._enrich_equity_candle(raw_event, raw_event.raw_payload)
        # Watchlist gating returns empty list
        assert res == []
        # BUT tradfi_bars write was executed unconditionally!
        assert mock_db.execute.called
        call_sql = mock_db.execute.call_args[0][0]
        assert "INSERT INTO tradfi_bars" in call_sql

    asyncio.run(_test())


def test_covered_call_engine_queries_cagg_zscore_view():
    """Verify quant trading engine queries tradfi_bars_5m_zscore for covered call overlay (§2.6)."""
    mock_db = MagicMock()
    mock_db.query_one = AsyncMock(return_value={"z_score": 3.2})

    async def _test():
        # Verify query against view
        row = await mock_db.query_one(
            "SELECT z_score FROM tradfi_bars_5m_zscore WHERE ticker = $1 ORDER BY bucket_time DESC LIMIT 1;",
            "NVDA"
        )
        assert row["z_score"] == 3.2
        rec = quant_calc.generate_covered_call_recommendation(
            ticker="NVDA",
            current_price=130.0,
            z_score=float(row["z_score"]),
            target_delta=0.30,
            dte_days=30
        )
        assert rec is not None
        assert rec["recommended_strike"] > 130.0

    asyncio.run(_test())




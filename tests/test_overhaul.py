"""
tests/test_overhaul.py

Unit tests for the Sentinel 5-Phase Overhaul:
1. Centralized Quantitative Calculations (quant_calc.py)
2. Anomaly Breakdown & Market Microstructure Data Models (events.py)
3. Agent Bulletins & Consensus Engine (base.py, consensus_engine.py)
4. System Metrics Collector (metrics.py)
"""

import pytest
import math
import numpy as np
from shared.utils import quant_calc
from shared.models import (
    NormalizedEvent, EventType, Entity, EntityType,
    AnomalyBreakdown, MarketMicrostructure, CrossDomainSignal
)
from services.agents.base import AgentBulletin, AgentScorecard
from services.agents.consensus_engine import ConsensusEngine, ConsensusReport
from shared.utils.metrics import MetricsCollector, time_block


# ── 1. QUANT CALC TESTS ───────────────────────────────────────────────────────

def test_ewma_volatility():
    returns = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01, 0.008]
    vol = quant_calc.ewma_volatility(returns, lam=0.94)
    assert isinstance(vol, float)
    assert vol > 0.0
    assert vol < 0.10


def test_garch_volatility():
    returns = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01, 0.008, 0.012, -0.015]
    vol = quant_calc.garch_volatility(returns, annualize=True)
    assert isinstance(vol, float)
    assert vol > 0.0


def test_parkinson_volatility():
    highs = [102.0, 105.0, 104.0, 106.0, 108.0]
    lows = [98.0, 101.0, 100.0, 102.0, 103.0]
    vol = quant_calc.parkinson_volatility(highs, lows)
    assert isinstance(vol, float)
    assert vol > 0.0


def test_yang_zhang_volatility():
    opens = [100.0, 102.0, 103.0, 105.0]
    highs = [103.0, 105.0, 106.0, 108.0]
    lows = [99.0, 101.0, 102.0, 104.0]
    closes = [102.0, 104.0, 105.0, 107.0]
    vol = quant_calc.yang_zhang_volatility(opens, highs, lows, closes)
    assert isinstance(vol, float)
    assert vol >= 0.0


def test_kelly_criterion():
    # Win rate 60%, win/loss ratio 2.0 -> Kelly f* = 0.6 - 0.4/2 = 0.4. Half-Kelly = 0.2
    f_half = quant_calc.kelly_criterion(0.60, 2.0, half_kelly=True)
    assert f_half == 0.20

    f_full = quant_calc.kelly_criterion(0.60, 2.0, half_kelly=False)
    assert f_full == 0.40


def test_var_and_cvar_historical():
    np.random.seed(42)
    returns = list(np.random.normal(0.001, 0.02, 500))
    var = quant_calc.var_historical(returns, confidence=0.95, position_value=100_000)
    cvar = quant_calc.cvar_historical(returns, confidence=0.95, position_value=100_000)
    assert var > 0
    assert cvar >= var  # CVaR is expected loss beyond VaR, so CVaR >= VaR


def test_augmented_dickey_fuller():
    np.random.seed(42)
    # Stationary series (white noise)
    stationary_series = list(np.random.normal(0, 1, 100))
    res = quant_calc.augmented_dickey_fuller(stationary_series)
    assert "adf_statistic" in res
    assert res["is_stationary"] is True

    # Non-stationary series (random walk)
    random_walk = list(np.cumsum(np.random.normal(0, 1, 100)))
    res_rw = quant_calc.augmented_dickey_fuller(random_walk)
    assert res_rw["is_stationary"] is False


def test_engle_granger_cointegration():
    np.random.seed(42)
    x = np.cumsum(np.random.normal(0, 1, 100))
    # Cointegrated pair: y = 2*x + noise
    y = 2.0 * x + np.random.normal(0, 0.5, 100)

    res = quant_calc.engle_granger_cointegration(list(x), list(y))
    assert res["is_cointegrated"] is True
    assert abs(res["beta"] - 2.0) < 0.2
    assert res["half_life"] < 50.0


def test_granger_causality():
    np.random.seed(42)
    x = np.random.normal(0, 1, 100)
    # y depends on lagged x
    y = np.zeros(100)
    for i in range(1, 100):
        y[i] = 0.8 * x[i - 1] + np.random.normal(0, 0.1)

    res = quant_calc.granger_causality(list(x), list(y), max_lag=3)
    assert res["x_granger_causes_y"] is True
    assert res["optimal_lag"] == 1


def test_hurst_exponent():
    # Mean-reverting series -> H < 0.5
    np.random.seed(42)
    sine_wave = list(np.sin(np.linspace(0, 20 * np.pi, 200)))
    h = quant_calc.hurst_exponent(sine_wave)
    assert h < 0.5


def test_kalman_filter_1d():
    obs = [100.0, 102.0, 101.5, 103.0, 102.8, 105.0]
    filtered = quant_calc.kalman_filter_1d(obs)
    assert len(filtered) == len(obs)
    assert filtered[0] == 100.0


def test_cusum_change_detection():
    # Step change from mean 0 to mean 10 at index 50
    np.random.seed(42)
    series = list(np.random.normal(0, 1, 50)) + list(np.random.normal(10, 1, 50))
    changes = quant_calc.cusum_change_detection(series, threshold=4.0)
    assert len(changes) > 0
    assert any(c["direction"] == "up" for c in changes)


def test_market_microstructure_math():
    ofi = quant_calc.order_flow_imbalance(7000.0, 3000.0)
    assert ofi == 0.40

    returns = [0.01, -0.02, 0.03, -0.01, 0.02]
    dvol = [1e6, 1.2e6, 8e5, 1.1e6, 9e5]
    amihud = quant_illiquidity = quant_calc.amihud_illiquidity(returns, dvol)
    assert amihud > 0


# ── 2. DATA MODEL TESTS ──────────────────────────────────────────────────────

def test_overhaul_event_models():
    breakdown = AnomalyBreakdown(
        composite_score=0.85,
        spatial_score=0.90,
        temporal_score=0.75,
        volume_z_score=2.5,
        volatility_z_score=1.8,
        domain="tradfi",
        is_significant=True,
    )
    assert breakdown.composite_score == 0.85

    micro = MarketMicrostructure(
        order_flow_imbalance=0.45,
        vwap=150.25,
        twap=150.10,
        parkinson_volatility=0.018,
    )
    assert micro.order_flow_imbalance == 0.45

    signal = CrossDomainSignal(
        event_id="evt_123",
        event_type="vessel_dark",
        domain="maritime",
        entity_id="IMO1234567",
        headline="Sanctioned tanker dark in Strait of Hormuz",
        anomaly_score=0.92,
    )
    assert signal.domain == "maritime"

    entity = Entity(id="NVDA", type=EntityType.INSTRUMENT, name="NVDA")
    event = NormalizedEvent(
        type=EventType.EQUITY_BLOCK,
        occurred_at=pytest.importorskip("datetime").datetime.now(pytest.importorskip("datetime").timezone.utc),
        source="finnhub_equities",
        primary_entity=entity,
        anomaly_score=0.85,
        anomaly_breakdown=breakdown,
        market_microstructure=micro,
        cross_domain_signals=[signal],
    )
    assert event.anomaly_breakdown.composite_score == 0.85
    assert event.market_microstructure.vwap == 150.25
    assert len(event.cross_domain_signals) == 1


# ── 3. BULLETIN & CONSENSUS TESTS ────────────────────────────────────────────

def test_agent_bulletin_model():
    bulletin = AgentBulletin(
        agent_name="financial_advisor",
        bulletin_type="signal",
        ticker="NVDA",
        conviction=0.85,
        expected_direction="up",
        summary="BUY NVDA target 160",
    )
    assert bulletin.agent_name == "financial_advisor"
    assert bulletin.expected_direction == "up"


def test_consolidated_engine_imports():
    from services.agents.macro_intelligence_engine import MacroIntelligenceEngine
    from services.agents.quant_trading_engine import QuantTradingEngine
    from services.agents.knowledge_graph_engine import KnowledgeGraphEngine

    assert MacroIntelligenceEngine.__name__ == "MacroIntelligenceEngine"
    assert QuantTradingEngine.__name__ == "QuantTradingEngine"
    assert KnowledgeGraphEngine.__name__ == "KnowledgeGraphEngine"


@pytest.mark.asyncio
async def test_consensus_engine_logic():
    class DummyRedis:
        async def scan(self, cursor=0, match="", count=100):
            return 0, []
        async def mget(self, keys):
            return []
        async def set(self, key, value, ex=None):
            pass

    engine = ConsensusEngine(DummyRedis())
    report = await engine.analyze()
    assert isinstance(report, ConsensusReport)
    assert report.total_active_bulletins == 0


# ── 4. METRICS TESTS ──────────────────────────────────────────────────────────

def test_metrics_collector():
    MetricsCollector.increment("test_counter", 5)
    MetricsCollector.set_gauge("test_gauge", 42.5)

    with time_block("test_latency"):
        _ = 1 + 1

    summary = MetricsCollector.get_summary()
    assert "test_counter" in summary["counters"]
    assert summary["counters"]["test_counter"] == 5
    assert summary["gauges"]["test_gauge"] == 42.5

    prom = MetricsCollector.to_prometheus_format()
    assert "sentinel_test_counter 5" in prom

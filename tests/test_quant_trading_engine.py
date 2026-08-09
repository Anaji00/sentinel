"""
tests/test_quant_trading_engine.py

Unit tests for QuantTradingEngine domain gate and cyber mapper.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from services.agents.quant_trading_engine import QuantTradingEngine
from shared.utils.cyber_mapper import map_cve_to_equity


def test_map_cve_to_equity():
    """Verify map_cve_to_equity maps known vendors to tickers and unmapped to None."""
    # Mapped vendor
    ticker, risk_type = map_cve_to_equity("CVE-2026-16812", vendor_name="microsoft")
    assert ticker == "MSFT"
    assert risk_type == "DIRECT_VENDOR_VULNERABILITY"

    # Case insensitive mapped vendor
    ticker, risk_type = map_cve_to_equity("CVE-2026-16812", vendor_name="Palo Alto Networks")
    assert ticker == "PANW"
    assert risk_type == "DIRECT_VENDOR_VULNERABILITY"

    # Unmapped vendor
    ticker, risk_type = map_cve_to_equity("CVE-2026-16812", vendor_name="unknown_home_router")
    assert ticker is None
    assert risk_type == "INFRASTRUCTURE_CYBER_RISK"

    # Description search
    ticker, risk_type = map_cve_to_equity("CVE-2026-99999", description="Vulnerability in Cisco ASA firewall")
    assert ticker == "CSCO"
    assert risk_type == "DIRECT_VENDOR_VULNERABILITY"


def test_quant_engine_rejects_unmapped_cve():
    """
    Construct a message with an unmapped CVE ticker and assert handle()
    returns None without invoking _process_peer_discovery or _process_trading_advisory.
    """
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.exists = AsyncMock(return_value=0)
        db_mock = MagicMock()
        neo_mock = MagicMock()
        prod_mock = AsyncMock()
        cons_mock = AsyncMock()
        dlq_mock = AsyncMock()

        agent = QuantTradingEngine(
            agent_name="quant_trading",
            input_topics=["events.enriched"],
            redis_client=redis_mock,
            db_client=db_mock,
            neo4j_client=neo_mock,
            producer=prod_mock,
            consumer=cons_mock,
            dlq=dlq_mock,
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
        agent._process_trading_advisory.assert_not_called()

    asyncio.run(run_test())


def test_quant_engine_positive_path_real_ticker():
    """
    Confirm a real ticker ("NVDA", score 0.75) passes the gate and proceeds to sub-engines.
    """
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.exists = AsyncMock(return_value=0)
        db_mock = MagicMock()
        neo_mock = MagicMock()
        prod_mock = AsyncMock()
        cons_mock = AsyncMock()
        dlq_mock = AsyncMock()

        agent = QuantTradingEngine(
            agent_name="quant_trading",
            input_topics=["events.enriched"],
            redis_client=redis_mock,
            db_client=db_mock,
            neo4j_client=neo_mock,
            producer=prod_mock,
            consumer=cons_mock,
            dlq=dlq_mock,
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
        agent._process_peer_discovery.assert_called_once_with(real_ticker_msg, "NVDA", 0.75)
        agent._process_trading_advisory.assert_called_once_with(real_ticker_msg, "NVDA", 0.75)

    asyncio.run(run_test())


def test_quant_engine_cve_mapped_vendor_proceeds_under_mapped_ticker():
    """
    Confirm a CVE- ticker with a mapped vendor (e.g. microsoft) reaches
    _process_peer_discovery/_process_trading_advisory under MSFT, not the raw CVE string.
    """
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.exists = AsyncMock(return_value=0)
        db_mock = MagicMock()
        neo_mock = MagicMock()
        prod_mock = AsyncMock()
        cons_mock = AsyncMock()
        dlq_mock = AsyncMock()

        agent = QuantTradingEngine(
            agent_name="quant_trading",
            input_topics=["events.enriched"],
            redis_client=redis_mock,
            db_client=db_mock,
            neo4j_client=neo_mock,
            producer=prod_mock,
            consumer=cons_mock,
            dlq=dlq_mock,
        )

        agent._process_peer_discovery = AsyncMock(return_value={"discovery": "ok"})
        agent._process_trading_advisory = AsyncMock(return_value={"advisory": "ok"})

        mapped_cve_msg = {
            "agent_run_id": "quant_CVE-2026-16812_msft",
            "trigger": {"ticker": "CVE-2026-16812", "vendor": "microsoft", "anomaly_score": 0.75},
            "primary_entity": {"id": "CVE-2026-16812"}
        }

        result = await agent.handle(mapped_cve_msg)

        assert result == {"advisory": "ok"}
        # Assert called under mapped ticker 'MSFT', not 'CVE-2026-16812'
        agent._process_peer_discovery.assert_called_once_with(mapped_cve_msg, "MSFT", 0.75)
        agent._process_trading_advisory.assert_called_once_with(mapped_cve_msg, "MSFT", 0.75)

    asyncio.run(run_test())


def test_deterministic_trade_signal_construction():
    """Verify _process_trading_advisory enforces server-side deterministic trade construction."""
    from services.agents.quant_trading_engine import TradingSignal, FinancialAdviceBrief

    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.get = AsyncMock(return_value=None)
        redis_mock.raw.set = AsyncMock()
        db_mock = MagicMock()
        neo_mock = MagicMock()
        prod_mock = AsyncMock()
        cons_mock = AsyncMock()
        dlq_mock = AsyncMock()

        agent = QuantTradingEngine(
            agent_name="quant_trading",
            input_topics=["events.enriched"],
            redis_client=redis_mock,
            db_client=db_mock,
            neo4j_client=neo_mock,
            producer=prod_mock,
            consumer=cons_mock,
            dlq=dlq_mock,
        )

        agent._fetch_prices = AsyncMock(return_value=([100.0] * 10, [102.0] * 10, [98.0] * 10))
        agent._compute_ta = MagicMock(return_value={
            "rsi": 50.0, "ema_12": 100.0, "ema_26": 100.0,
            "atr": 2.0, "fib_levels": {}
        })
        agent.get_scorecard = AsyncMock(return_value=MagicMock(predictions_made=10, predictions_correct=6))
        agent.get_cross_agent_context = AsyncMock(return_value="")
        agent.record_prediction = AsyncMock()
        agent.publish_bulletin = AsyncMock()

        # LLM returns hallucinated entry/target/stop/rr/kelly values
        hallucinated_play = TradingSignal(
            ticker="AAPL",
            action="BUY",
            trade_type="Long/Buy",
            entry_level=999.0,
            target_price=5000.0,
            stop_loss=1.0,
            risk_reward_ratio=47.0,
            kelly_allocation_pct=95.0,
            conviction_score=0.85,  # High conviction -> RR should be 3.0
            quantitative_rationale="Strong momentum"
        )
        mock_brief = FinancialAdviceBrief(
            market_regime="Bullish",
            highest_conviction_plays=[hallucinated_play],
            general_hedging_strategy="None"
        )
        agent._execute_with_telemetry = AsyncMock(return_value=mock_brief)

        res = await agent._process_trading_advisory({"ticker": "AAPL"}, "AAPL", 0.8)
        assert res is not None

        play = res["brief"]["highest_conviction_plays"][0]
        # Current price = 100.0, ATR = 2.0 -> stop_distance = 3.0
        assert play["entry_level"] == 100.0
        assert play["stop_loss"] == 97.0  # 100 - 3.0
        assert play["risk_reward_ratio"] == 3.0  # High conviction tier (>= 0.8)
        assert play["target_price"] == 109.0  # 100 + (3.0 * 3.0)
        # Kelly allocation must be clamped to server calculated max (half-kelly)
        assert play["kelly_allocation_pct"] <= 50.0

    asyncio.run(run_test())


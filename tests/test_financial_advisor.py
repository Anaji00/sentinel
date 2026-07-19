import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pydantic import BaseModel

from services.agents.financial_advisor import (
    FinancialAdvisorAgent, compute_ta_indicators,
    TradingSignal, FinancialAdviceBrief
)
from shared.kafka import Topics

def test_compute_ta_indicators_mathematics():
    """Verify that programmatically computed RSI, EMA, ATR, and Fib levels are correct."""
    # Build a rising price series
    closes = [10.0 + i for i in range(20)]  # 10.0 to 29.0
    highs = [c + 0.5 for c in closes]
    lows = [c - 0.5 for c in closes]
    
    indicators = compute_ta_indicators(closes, highs, lows)
    
    assert "rsi" in indicators
    assert "ema_12" in indicators
    assert "ema_26" in indicators
    assert "atr" in indicators
    assert "fib_levels" in indicators
    
    # Rising series should have RSI > 50
    assert indicators["rsi"] > 50.0
    
    # Test Fib levels: 0.0 is min low (9.5), 1.0 is max high (29.5)
    fibs = indicators["fib_levels"]
    assert fibs["0.0"] == 9.5
    assert fibs["1.0"] == 29.5
    # Fib 0.5 should be exactly midway (19.5)
    assert abs(fibs["0.500"] - 19.5) < 1e-5


def test_financial_advisor_scheduled_review():
    """Verify that FinancialAdvisorAgent queries watched tickers and generates structured briefs."""
    async def run_test():
        redis_mock = MagicMock()
        db_mock = MagicMock()
        neo_mock = MagicMock()
        prod_mock = AsyncMock()
        cons_mock = AsyncMock()
        dlq_mock = AsyncMock()

        # Mock Redis watchlist zrange
        redis_mock.raw.zrange = AsyncMock(return_value=[b"AAPL", b"BTC-USD"])

        # Instantiate agent
        agent = FinancialAdvisorAgent(
            agent_name="financial_advisor",
            input_topics=["heartbeat"],
            redis_client=redis_mock,
            db_client=db_mock,
            neo4j_client=neo_mock,
            producer=prod_mock,
            consumer=cons_mock,
            dlq=dlq_mock,
        )

        # Mock price fetch to return valid price series (minimum 10 bars)
        closes = [100.0 + i for i in range(20)]
        highs = [c + 1.0 for c in closes]
        lows = [c - 1.0 for c in closes]
        agent._fetch_prices = AsyncMock(return_value=(closes, highs, lows))
        agent.fetch_global_context = AsyncMock(return_value="macro context info")

        # Mock LLM and telemetry
        mock_brief = FinancialAdviceBrief(
            market_regime="Mean-Reverting",
            highest_conviction_plays=[
                TradingSignal(
                    ticker="BTC-USD",
                    action="BUY",
                    entry_level=64000.0,
                    target_price=68000.0,
                    stop_loss=62000.0,
                    risk_reward_ratio=2.0,
                    kelly_allocation_pct=10.0,
                    conviction_score=0.85,
                    technical_indicators={"rsi": 35.0, "ema_12": 63900.0, "ema_26": 64200.0, "atr": 1200.0, "close": 64100.0},
                    fib_levels={"0.618": 63800.0},
                    quantitative_rationale="Asymmetric EV support setup near the 61.8% Fibonacci cluster.",
                )
            ],
            general_hedging_strategy="Delta-hedging with out-of-the-money puts."
        )
        
        agent._execute_with_telemetry = AsyncMock(return_value=mock_brief)
        agent.write_agent_memory = AsyncMock()
        
        # We manually trigger run_scheduled_review logic once by patching asyncio.sleep to break the loop
        with patch("asyncio.sleep", AsyncMock(side_effect=[None, Exception("StopLoop")])):
            with pytest.raises(Exception, match="StopLoop"):
                await agent.run_scheduled_review()

        # Verify executing telemetry and publishing plays worked
        agent._execute_with_telemetry.assert_called_once()
        agent.write_agent_memory.assert_called_once()
        prod_mock.send.assert_called_once()
        
        # Verify published topic matches FINANCIAL_ADVICE
        args, kwargs = prod_mock.send.call_args
        assert args[0] == Topics.FINANCIAL_ADVICE
        assert "brief" in args[1]

    asyncio.run(run_test())


def test_financial_advisor_live_trigger():
    """Verify that FinancialAdvisorAgent extracts tickers from a live event and runs the handle execution path."""
    async def run_test():
        redis_mock = MagicMock()
        db_mock = MagicMock()
        neo_mock = MagicMock()
        prod_mock = AsyncMock()
        cons_mock = AsyncMock()
        dlq_mock = AsyncMock()

        # Mock Redis watchlist zrange and pipeline
        redis_mock.raw.zrange = AsyncMock(return_value=[b"AAPL"])
        
        pipe_mock = AsyncMock()
        pipe_mock.zadd = MagicMock()
        pipe_mock.zremrangebyrank = MagicMock()
        pipe_mock.execute = AsyncMock(return_value=[])
        
        pipeline_mock = MagicMock()
        pipeline_mock.__aenter__ = AsyncMock(return_value=pipe_mock)
        pipeline_mock.__aexit__ = AsyncMock(return_value=None)
        redis_mock.raw.pipeline = MagicMock(return_value=pipeline_mock)

        # Instantiate agent
        agent = FinancialAdvisorAgent(
            agent_name="financial_advisor",
            input_topics=["heartbeat"],
            redis_client=redis_mock,
            db_client=db_mock,
            neo4j_client=neo_mock,
            producer=prod_mock,
            consumer=cons_mock,
            dlq=dlq_mock,
        )

        # Mock price fetch to return valid price series (minimum 10 bars)
        closes = [100.0 + i for i in range(20)]
        highs = [c + 1.0 for c in closes]
        lows = [c - 1.0 for c in closes]
        agent._fetch_prices = AsyncMock(return_value=(closes, highs, lows))
        agent.fetch_global_context = AsyncMock(return_value="macro context info")

        # Mock LLM and telemetry
        mock_brief = FinancialAdviceBrief(
            market_regime="Trending Up",
            highest_conviction_plays=[
                TradingSignal(
                    ticker="AAPL",
                    action="BUY",
                    entry_level=180.0,
                    target_price=200.0,
                    stop_loss=170.0,
                    risk_reward_ratio=2.0,
                    kelly_allocation_pct=15.0,
                    conviction_score=0.90,
                    technical_indicators={"rsi": 45.0, "ema_12": 178.0, "ema_26": 175.0, "atr": 5.0, "close": 180.0},
                    fib_levels={"0.618": 177.0},
                    quantitative_rationale="Bullish breakout setup near local Fib support.",
                )
            ],
            general_hedging_strategy="None required."
        )
        
        agent._execute_with_telemetry = AsyncMock(return_value=mock_brief)
        agent.write_agent_memory = AsyncMock()

        # Build an incoming live quant discovery payload
        quant_discovery_event = {
            "agent": "quant_researcher",
            "trigger": {
                "ticker": "AAPL",
                "event_type": "equity_block",
                "notional_usd": 150000.0,
                "anomaly_score": 0.88,
            },
            "discovery": {
                "peer_tickers": [{"ticker": "MSFT", "relationship_type": "allied_with"}]
            }
        }

        # Trigger handle method
        await agent.handle(quant_discovery_event)

        # Verify executing telemetry and publishing plays worked
        agent._execute_with_telemetry.assert_called_once()
        agent.write_agent_memory.assert_called_once()
        prod_mock.send.assert_called_once()
        
        # Verify published topic matches FINANCIAL_ADVICE
        args, kwargs = prod_mock.send.call_args
        assert args[0] == Topics.FINANCIAL_ADVICE
        assert "brief" in args[1]

    asyncio.run(run_test())

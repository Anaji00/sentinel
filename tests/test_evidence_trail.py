"""
tests/test_evidence_trail.py

Unit tests for Task B5: Per-alert evidence trail.
"""

import asyncio
from services.agents.consensus_engine import ConsensusEngine, SubjectiveOpinion
from services.agents.base import AgentBulletin, AgentScorecard


class DummyRedisStore:
    def __init__(self, bulletins):
        self.bulletins = bulletins
        self.raw = self

    async def scan(self, cursor=0, match="", count=100):
        keys = list(self.bulletins.keys())
        return 0, keys

    async def mget(self, keys):
        return [self.bulletins.get(k) for k in keys]

    async def get(self, key):
        return None

    async def set(self, key, val, ex=None):
        pass


async def _async_test_evidence_trail_on_consensus_signal():
    b1 = AgentBulletin(
        bulletin_id="b1",
        bulletin_type="signal",
        agent_name="quant_trading_engine",
        ticker="AAPL",
        expected_direction="bullish",
        conviction=0.85,
        thesis="Strong momentum breakout",
    )
    b2 = AgentBulletin(
        bulletin_id="b2",
        bulletin_type="signal",
        agent_name="macro_intelligence_engine",
        ticker="AAPL",
        expected_direction="bullish",
        conviction=0.75,
        thesis="Favorable rate environment",
    )

    redis = DummyRedisStore({
        "sentinel:bulletins:quant:AAPL": b1.model_dump_json(),
        "sentinel:bulletins:macro:AAPL": b2.model_dump_json(),
    })

    engine = ConsensusEngine(redis)
    report = await engine.analyze()

    assert len(report.consensus_signals) == 1
    signal = report.consensus_signals[0]
    assert signal.ticker == "AAPL"
    assert signal.direction == "bullish"

    # Verify evidence trail is populated with each contributing agent and its score/weight
    assert hasattr(signal, "evidence_trail")
    assert len(signal.evidence_trail) == 2

    agents = {e.agent_name: e for e in signal.evidence_trail}
    assert "quant_trading_engine" in agents
    assert "macro_intelligence_engine" in agents

    assert agents["quant_trading_engine"].conviction == 0.85
    assert agents["quant_trading_engine"].direction == "bullish"
    assert agents["quant_trading_engine"].weight > 0.0

    assert agents["macro_intelligence_engine"].conviction == 0.75
    assert agents["macro_intelligence_engine"].direction == "bullish"
    assert agents["macro_intelligence_engine"].weight > 0.0


def test_evidence_trail_on_consensus_signal():
    asyncio.run(_async_test_evidence_trail_on_consensus_signal())

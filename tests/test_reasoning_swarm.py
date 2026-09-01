"""
tests/test_reasoning_swarm.py

Consolidated Test Suite: Consensus Engine, Subjective Logic, Adversarial Wargamer, Edge Validator, Macro Intelligence Engine, Scorecard & Evidence Trail.
Combines:
  - test_subjective_logic_consensus.py
  - test_adversarial_wargamer.py
  - test_edge_validator.py
  - test_macro_intelligence_engine.py
  - test_exposure_confidence_gate.py
  - test_source_scorecard.py
  - test_evidence_trail.py
"""

import os
import json
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from services.agents.consensus_engine import (
    ConsensusEngine, ConsensusReport, SubjectiveOpinion, ACHReport, CompetingHypothesis, ACH_UNCERTAINTY_THRESHOLD
)
from services.agents.adversarial_wargamer import (
    AdversarialWargamerAgent,
    WargameSimulationOutput,
    SimulationBoard,
    SimulationMove,
)
from services.agents.edge_validator import validate_edges
from services.agents.macro_intelligence_engine import MacroIntelligenceEngine, TICKER_METADATA_REGISTRY
from services.agents.base import AgentBulletin, AgentScorecard
from shared.utils.source_scorecard import get_source_scorecard, update_source_scorecard
from services.enrichment.enrichers.news import NewsEnricher
from shared.models import RawEvent


# ── 1. SUBJECTIVE LOGIC & OPINION FUSION ──────────────────────────────────────

def test_subjective_opinion_math():
    op = SubjectiveOpinion(belief=0.7, disbelief=0.1, uncertainty=0.2, base_rate=0.5)
    assert abs(op.projected_probability - 0.8) < 0.01
    assert abs(op.belief + op.disbelief + op.uncertainty - 1.0) < 0.01

def test_subjective_opinion_from_bulletin():
    bulletin = AgentBulletin(
        agent_name="quant", bulletin_type="signal", ticker="AAPL", conviction=0.85, expected_direction="bullish"
    )
    op = SubjectiveOpinion.from_bulletin(bulletin, weight=1.5)
    assert op.belief > op.disbelief

def test_cumulative_fusion_agreeing_strengthens():
    op1 = SubjectiveOpinion(belief=0.5, disbelief=0.1, uncertainty=0.4)
    op2 = SubjectiveOpinion(belief=0.6, disbelief=0.1, uncertainty=0.3)
    fused = SubjectiveOpinion.cumulative_fuse([op1, op2])
    assert fused.belief > 0.5
    assert fused.uncertainty < 0.4

def test_ach_report_structure():
    ach = ACHReport(
        ticker="AAPL", fused_opinion=SubjectiveOpinion(belief=0.3, disbelief=0.3, uncertainty=0.4),
        competing_hypotheses=[
            CompetingHypothesis(hypothesis="Bullish", supporting_agents=["quant"], refuting_agents=["macro"])
        ],
        conflict_level=0.6, fused_uncertainty=0.4, reason="High uncertainty"
    )
    assert len(ach.competing_hypotheses) == 1
    assert ach.fused_uncertainty >= ACH_UNCERTAINTY_THRESHOLD


# ── 2. ADVERSARIAL WARGAMER AGENT ────────────────────────────────────────────

def test_adversarial_wargamer_entity_extraction_and_handle():
    async def run_test():
        agent = AdversarialWargamerAgent.__new__(AdversarialWargamerAgent)
        agent.name = "adversarial_wargamer"
        agent.logger = MagicMock()
        agent.neo4j = None
        agent.redis = MagicMock()
        agent.redis.raw = AsyncMock()

        async def mock_execute_telemetry(message, system_prompt, user_prompt, schema, temperature=0.0):
            # The personas arrive as one board rather than three separate
            # calls; see tests/test_wargame_slot_cost.py for why.
            if schema == SimulationBoard:
                return SimulationBoard(moves=[
                    SimulationMove(
                        persona_name="State_Saboteur", proposed_counter_action="Target supply lines",
                        target_entity_id="NVDA", disruption_potential_percent=80,
                        strategic_rationale="Exploit bottleneck",
                    ),
                    SimulationMove(
                        persona_name="Asymmetric_Defender", proposed_counter_action="Dual-source fab capacity",
                        target_entity_id="NVDA", disruption_potential_percent=30,
                        strategic_rationale="Remove the single point of failure",
                    ),
                ])
            return WargameSimulationOutput(
                primary_vulnerability_isolated="Supply chain choke point", cascade_failure_probability=75,
                predicted_next_target_entity_id="NVDA", remediation_recommendation="Hedge positions"
            )

        agent._execute_with_telemetry = AsyncMock(side_effect=mock_execute_telemetry)
        agent.get_cross_agent_context = AsyncMock(return_value="")
        agent.record_prediction = AsyncMock()
        agent.publish_bulletin = AsyncMock()

        message = {"primary_entity_id": "NVDA", "headline": "Semiconductor export controls", "correlation_id": "test_123"}
        result = await agent.handle(message)
        assert result["predicted_next_target_entity_id"] == "NVDA"

    asyncio.run(run_test())


# ── 3. EDGE VALIDATOR ENGINE ──────────────────────────────────────────────────

def test_edge_validator_samples_below_min_threshold_untouched():
    async def run_test():
        edges = [{"source_id": "HORMUZ", "predicate": "SUPPLIES", "ticker": "XOM", "confidence": 0.5, "samples": 2}]
        neo4j = MagicMock()
        neo4j.query = AsyncMock(return_value=edges)
        ts = MagicMock()
        ts.query = AsyncMock(return_value=[])

        res = await validate_edges(neo4j, ts, MagicMock())
        assert res["validated"] == 0

    asyncio.run(run_test())


# ── 4. MACRO INTELLIGENCE ENGINE ─────────────────────────────────────────────

def test_decoupling_z_score_clamped_and_enriched():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.set = AsyncMock()

        pipe_mock = AsyncMock()
        pipe_mock.execute = AsyncMock(return_value=[None, None, None, None, [100.0 + i * 0.1 for i in range(35)], [150.0 + i * 0.15 for i in range(35)]])
        redis_mock.raw.pipeline = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=pipe_mock), __aexit__=AsyncMock()))
        producer_mock = AsyncMock()

        agent = MacroIntelligenceEngine(
            agent_name="macro", input_topics=["events.enriched"], redis_client=redis_mock,
            db_client=MagicMock(), neo4j_client=MagicMock(), producer=producer_mock,
            consumer=AsyncMock(), dlq=AsyncMock()
        )

        with patch("shared.utils.quant_calc.engle_granger_cointegration") as mock_eg:
            mock_eg.return_value = {"is_cointegrated": True, "beta": 1.0, "alpha": 0.0, "half_life": 5.0, "spread_std": 1.0}
            await agent._process_pair_cointegration("ZC=F", "TIP", 103.4, 155.1)
            assert producer_mock.send.call_count == 1
            payload = producer_mock.send.call_args[0][1]
            assert payload["z_score"] == 5.0

    asyncio.run(run_test())


# ── 5. SOURCE SCORECARD & EVIDENCE TRAIL ─────────────────────────────────────

def test_source_scorecard_and_evidence_trail():
    async def run_test():
        b1 = AgentBulletin(bulletin_id="b1", bulletin_type="signal", agent_name="quant", ticker="AAPL", expected_direction="bullish", conviction=0.85)
        b2 = AgentBulletin(bulletin_id="b2", bulletin_type="signal", agent_name="macro", ticker="AAPL", expected_direction="bullish", conviction=0.75)

        class MockRedisStore:
            def __init__(self):
                self.raw = self
            async def scan(self, cursor=0, match="", count=100): return 0, ["key1", "key2"]
            async def mget(self, keys): return [b1.model_dump_json(), b2.model_dump_json()]
            async def get(self, key): return None
            async def set(self, key, val, ex=None): pass

        engine = ConsensusEngine(MockRedisStore())
        report = await engine.analyze()
        assert len(report.consensus_signals) == 1
        signal = report.consensus_signals[0]
        assert hasattr(signal, "evidence_trail")
        assert len(signal.evidence_trail) == 2

    asyncio.run(run_test())

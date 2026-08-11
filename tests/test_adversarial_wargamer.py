"""
tests/test_adversarial_wargamer.py

Unit tests for AdversarialWargamerAgent message schema extraction and simulation execution.
Verifies payloads with primary_entity_id, ticker, asset, or headlines are correctly handled.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from services.agents.adversarial_wargamer import AdversarialWargamerAgent, WargameSimulationOutput, SimulationMove


def test_adversarial_wargamer_entity_extraction_and_handle():
    async def run_test():
        agent = AdversarialWargamerAgent.__new__(AdversarialWargamerAgent)
        agent.name = "adversarial_wargamer"
        agent.logger = MagicMock()
        agent.neo4j = None
        agent.redis = MagicMock()
        agent.redis.raw = AsyncMock()

        # Mock telemetry LLM response dynamically based on requested schema
        async def mock_execute_telemetry(message, system_prompt, user_prompt, schema, temperature=0.0):
            if schema == SimulationMove:
                return SimulationMove(
                    persona_name="State_Saboteur",
                    proposed_counter_action="Target supply lines",
                    target_entity_id="NVDA",
                    disruption_potential_percent=80,
                    strategic_rationale="Exploit trade bottleneck"
                )
            return WargameSimulationOutput(
                primary_vulnerability_isolated="Supply chain choke point",
                cascade_failure_probability=75,
                predicted_next_target_entity_id="NVDA",
                remediation_recommendation="Hedge positions & reroute logistics"
            )

        agent._execute_with_telemetry = AsyncMock(side_effect=mock_execute_telemetry)
        agent.get_cross_agent_context = AsyncMock(return_value="")
        agent.record_prediction = AsyncMock()
        agent.publish_bulletin = AsyncMock()

        # Test message with ticker/headline instead of entity_ids
        message = {
            "primary_entity_id": "NVDA",
            "headline": "Semiconductor export controls escalating in South China Sea",
            "correlation_id": "test_corr_123"
        }

        result = await agent.handle(message)

        assert result is not None
        assert result["predicted_next_target_entity_id"] == "NVDA"
        assert result["cascade_failure_probability"] == 75
        assert agent.record_prediction.called
        assert agent.publish_bulletin.called

    asyncio.run(run_test())

"""
tests/test_rule_synthesizer_waits_for_a_slot.py

The rule synthesiser asked for a slot once per message and was always shed.

Every other inferring agent defers through its candidate buffer when the shared
slot is held. This one called `_execute_with_telemetry` directly, whose claim
fails whenever another agent is mid-inference -- most of the time on this host.
Measured across 20-21 September: 127 messages processed, buffer offered 0, zero
completed inferences, while the rest of the tier completed 655. RULE_CANDIDATES
-- the topic this agent exists to serve -- moved from 24 to 30 messages, and
none was ever synthesised.
"""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import services.agents.rule_agent as rule_agent


def _agent(capacity: bool):
    agent = rule_agent.RuleSynthesizerAgent.__new__(rule_agent.RuleSynthesizerAgent)
    agent.capacity_or_defer = AsyncMock(return_value=capacity)
    agent._execute_with_telemetry = AsyncMock(return_value=None)
    agent.logger = rule_agent.logging.getLogger("test.rule_synth")
    return agent


CANDIDATE = {
    "type": "rule_candidate",
    "event_type_a": "options_flow",
    "event_type_b": "equity_block",
    "times_seen": 6,
}


def test_a_held_slot_defers_instead_of_shedding(monkeypatch):
    agent = _agent(capacity=False)
    vocab = AsyncMock(return_value=([], []))
    monkeypatch.setattr(rule_agent, "_observed_vocabulary", vocab)
    asyncio.run(agent.handle(dict(CANDIDATE)))
    agent.capacity_or_defer.assert_awaited_once()
    agent._execute_with_telemetry.assert_not_awaited()
    vocab.assert_not_awaited()


def test_a_rule_candidate_is_held_ahead_of_narrative_inputs(monkeypatch):
    agent = _agent(capacity=False)
    monkeypatch.setattr(rule_agent, "_observed_vocabulary", AsyncMock(return_value=([], [])))
    asyncio.run(agent.handle(dict(CANDIDATE)))
    assert agent.capacity_or_defer.await_args.kwargs["score"] == 1.0


def test_an_assessment_is_held_at_its_conviction(monkeypatch):
    agent = _agent(capacity=False)
    monkeypatch.setattr(rule_agent, "_observed_vocabulary", AsyncMock(return_value=([], [])))
    msg = {"assessment": {"macro_asset": "CL=F", "equity_ticker": "XOM", "conviction": 0.62}}
    asyncio.run(agent.handle(msg))
    assert agent.capacity_or_defer.await_args.kwargs["score"] == 0.62


def test_a_free_slot_proceeds_to_inference(monkeypatch):
    agent = _agent(capacity=True)
    monkeypatch.setattr(rule_agent, "_observed_vocabulary", AsyncMock(return_value=([], [])))
    asyncio.run(agent.handle(dict(CANDIDATE)))
    agent._execute_with_telemetry.assert_awaited_once()


def test_an_unroutable_message_never_occupies_the_buffer():
    agent = _agent(capacity=False)
    asyncio.run(agent.handle({"something": "else"}))
    agent.capacity_or_defer.assert_not_awaited()

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
    # The daily synthesis cap (RULE_SYNTH_DAILY_CAP, 30 Oct 2026) counts in
    # Redis after the slot is granted; first call of the day here.
    agent.redis = AsyncMock()
    agent.redis.raw.incr = AsyncMock(return_value=1)
    _queue(agent)
    return agent


def _queue(agent):
    """Contexts wait in a ranked queue and one synthesis every slot takes the
    strongest (30 Oct 2026); an in-memory stand-in, with the slot free."""
    q = {}

    async def zadd(key, mapping, gt=False):
        for m, s in mapping.items():
            q[m] = max(s, q.get(m, s)) if gt else s

    async def zpopmax(key, k):
        top = sorted(q.items(), key=lambda kv: -kv[1])[:k]
        for m, _ in top:
            q.pop(m)
        return top

    agent.redis.raw.zadd = zadd
    agent.redis.raw.zpopmax = zpopmax
    agent.redis.raw.exists = AsyncMock(return_value=0)
    agent.redis.raw.set = AsyncMock(return_value=True)
    agent.redis.raw.zremrangebyrank = AsyncMock()
    agent.redis.raw.expire = AsyncMock()
    agent.queue = q


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


def test_over_the_daily_cap_no_inference_is_spent(monkeypatch):
    agent = _agent(capacity=True)
    agent.redis.raw.incr = AsyncMock(return_value=rule_agent.SYNTH_DAILY_CAP + 1)
    monkeypatch.setattr(rule_agent, "_observed_vocabulary", AsyncMock(return_value=([], [])))
    asyncio.run(agent.handle(dict(CANDIDATE)))
    agent._execute_with_telemetry.assert_not_awaited()


def test_a_taken_slot_queues_the_trigger_without_inference(monkeypatch):
    agent = _agent(capacity=True)
    agent.redis.raw.exists = AsyncMock(return_value=1)          # synthesised within the slot
    monkeypatch.setattr(rule_agent, "_observed_vocabulary", AsyncMock(return_value=([], [])))
    asyncio.run(agent.handle(dict(CANDIDATE)))
    agent._execute_with_telemetry.assert_not_awaited()
    assert len(agent.queue) == 1 and list(agent.queue.values()) == [6.0]   # ranked by times seen


def test_one_synthesis_reads_the_strongest_queued_contexts(monkeypatch):
    agent = _agent(capacity=True)
    agent.queue.update({"weak observation": 1.0, "strong observation": 40.0})
    monkeypatch.setattr(rule_agent, "_observed_vocabulary", AsyncMock(return_value=([], [])))
    asyncio.run(agent.handle(dict(CANDIDATE)))
    prompt = agent._execute_with_telemetry.await_args.kwargs["user_prompt"]
    assert prompt.index("strong observation") < prompt.index("TIMES SEEN TOGETHER") < prompt.index("weak observation")

"""
tests/test_focus_reaches_the_buffer.py

The candidate buffers never consulted the focus set.

`shared.utils.focus` exists so that a subject one agent finds is looked at by a
second -- its contract is that agents "consult the focus set first". The
buffers, where each agent actually picks its next inference, ranked purely by
anomaly score. Measured on 1 October: 18 consensus signals, every one from a
single agent. And `offer_focus` took `offered_by` and dropped it, so nothing
could tell another agent's subject from one an agent had offered itself.
"""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import services.agents.base as base
from shared.utils.focus import FOCUS_OFFERER_KEY, focus_offers, offer_focus


class _Agent(base.SentinelAgent):
    async def handle(self, message):
        return None

    @property
    def output_topic(self):
        return "unused"


def _agent(offers):
    agent = _Agent.__new__(_Agent)
    agent.name = "quant_trading_engine"
    agent.logger = base.logging.getLogger("test.focus_lift")
    agent.redis = MagicMock()
    agent._budget_instance = MagicMock()
    agent._budget_instance.is_available = AsyncMock(return_value=False)
    agent._candidates = MagicMock()
    base_focus = base.focus_offers

    async def fake(redis, domains=None):
        return offers

    base.focus_offers = fake
    return agent, base_focus


def _offered_score(agent, message):
    asyncio.run(agent.capacity_or_defer(message))
    return agent._candidates.offer.call_args.args[1]


def test_a_subject_another_agent_focused_outranks_any_unfocused_one():
    agent, restore = _agent({"AAPL": "radar_agent"})
    try:
        focused = _offered_score(agent, {"ticker": "aapl", "anomaly_score": 0.3})
        agent._focus_cache_at = -1e9
        unfocused = _offered_score(agent, {"ticker": "MSFT", "anomaly_score": 0.99})
    finally:
        base.focus_offers = restore
    assert focused > unfocused
    assert focused == 0.3 + base.FOCUS_CANDIDATE_LIFT


def test_an_agents_own_subject_is_not_lifted():
    agent, restore = _agent({"AAPL": "quant_trading_engine"})
    try:
        assert _offered_score(agent, {"ticker": "AAPL", "anomaly_score": 0.3}) == 0.3
    finally:
        base.focus_offers = restore


def test_the_lift_is_counted():
    agent, restore = _agent({"XOM": "stock_correlation_agent"})
    try:
        _offered_score(agent, {"primary_entity": {"id": "XOM"}, "anomaly_score": 0.5})
    finally:
        base.focus_offers = restore
    assert agent._focus_lifted == 1


def test_the_focus_read_is_cached():
    calls = []

    async def fake(redis, domains=None):
        calls.append(1)
        return {}

    agent, restore = _agent({})
    base.focus_offers = fake
    try:
        for _ in range(5):
            asyncio.run(agent.capacity_or_defer({"ticker": "AAPL"}))
    finally:
        base.focus_offers = restore
    assert len(calls) == 1


def test_a_free_slot_skips_the_buffer_entirely():
    agent, restore = _agent({"AAPL": "radar_agent"})
    agent._budget_instance.is_available = AsyncMock(return_value=True)
    try:
        assert asyncio.run(agent.capacity_or_defer({"ticker": "AAPL"})) is True
    finally:
        base.focus_offers = restore
    agent._candidates.offer.assert_not_called()


class _FakeRedis:
    def __init__(self):
        self.z, self.h = {}, {}

    async def zadd(self, key, mapping, xx=False, gt=False):
        self.z.setdefault(key, {}).update(mapping)

    async def hset(self, key, field, value):
        self.h.setdefault(key, {})[field] = value

    async def hmget(self, key, fields):
        return [self.h.get(key, {}).get(f) for f in fields]

    async def zremrangebyscore(self, *a):
        pass

    async def zrange(self, key, a, b):
        return list(self.z.get(key, {}))

    async def zrangebyscore(self, key, lo, hi):
        return sorted(self.z.get(key, {}), key=lambda m: self.z[key][m])

    async def zrem(self, *a):
        pass

    async def hdel(self, *a):
        pass

    async def zremrangebyrank(self, *a):
        pass

    async def expire(self, *a):
        pass


def test_the_offerer_is_recorded_and_read_back():
    r = _FakeRedis()
    asyncio.run(offer_focus(r, "aapl", 0.9, offered_by="radar_agent", domain="tradfi"))
    assert r.h[FOCUS_OFFERER_KEY]["AAPL"] == "radar_agent"
    assert asyncio.run(focus_offers(r)) == {"AAPL": "radar_agent"}

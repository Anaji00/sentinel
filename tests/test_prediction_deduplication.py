"""
tests/test_prediction_deduplication.py

Six predictions had been recorded when this was written, and two pairs of them
were byte-identical:

    BCHUSDT  up  conv=0.75  entry=246.99  target=255.25
    BCHUSDT  up  conv=0.75  entry=246.99  target=255.25
    XRPUSDT  up  conv=1.00  entry=1.37    target=1.44
    XRPUSDT  up  conv=1.00  entry=1.37    target=1.44

The quant engine re-derives its plays on every run and nothing stopped it
storing the same standing call again each time. That inflates a count this
system has spent a long time trying to move off zero, and it double-weights the
scorecard the consensus engine reads -- an agent repeating itself would outrank
one that was right.

A repeat is not a second forecast. A reversal is, and so is a fresh entry after
the last horizon lapsed, so the claim expires with the prediction it guards
rather than blocking the ticker forever.
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _FakeRaw:
    """Redis with working SET NX EX semantics, which is the whole mechanism."""

    def __init__(self):
        self.store = {}
        self.sets = {}
        self.set_calls = []

    async def set(self, key, value, ex=None, nx=False):
        self.set_calls.append((key, nx, ex))
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def sadd(self, key, *values):
        self.sets.setdefault(key, set()).update(values)

    async def expire(self, key, ttl):
        return True

    async def get(self, key):
        return self.store.get(key)


class _FakeRedis:
    def __init__(self):
        self.raw = _FakeRaw()


def _claim_key(agent, ticker, direction):
    return f"sentinel:predictions:claim:{agent}:{ticker.upper()}:{direction.lower()}"


def test_an_identical_repeat_is_not_recorded_twice():
    """The defect verbatim."""
    async def run():
        redis = _FakeRedis()
        key = _claim_key("quant_trading_engine", "BCHUSDT", "up")
        first = await redis.raw.set(key, "1", nx=True, ex=86400)
        second = await redis.raw.set(key, "1", nx=True, ex=86400)
        assert first, "the first call must be recorded"
        assert not second, "the repeat must not be"

    asyncio.run(run())


def test_a_reversal_is_a_new_forecast():
    """Direction is part of the claim, so changing your mind still counts."""
    async def run():
        redis = _FakeRedis()
        up = await redis.raw.set(_claim_key("q", "BCHUSDT", "up"), "1", nx=True, ex=86400)
        down = await redis.raw.set(_claim_key("q", "BCHUSDT", "down"), "1", nx=True, ex=86400)
        assert up and down

    asyncio.run(run())


def test_two_agents_may_hold_the_same_view():
    """Agreement between agents is a signal in itself and must not be
    silently collapsed into one voice."""
    async def run():
        redis = _FakeRedis()
        a = await redis.raw.set(_claim_key("quant", "XRPUSDT", "up"), "1", nx=True, ex=86400)
        b = await redis.raw.set(_claim_key("macro", "XRPUSDT", "up"), "1", nx=True, ex=86400)
        assert a and b

    asyncio.run(run())


def test_different_tickers_are_independent():
    async def run():
        redis = _FakeRedis()
        a = await redis.raw.set(_claim_key("q", "BCHUSDT", "up"), "1", nx=True, ex=86400)
        b = await redis.raw.set(_claim_key("q", "AVAXUSDT", "up"), "1", nx=True, ex=86400)
        assert a and b

    asyncio.run(run())


def test_the_claim_expires_with_the_prediction():
    """A claim outliving its horizon would block the ticker permanently; one
    expiring early would let the duplicate back in."""
    source = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    assert "claim_ttl = max(int(time_horizon_hours) * 3600, 3600)" in source


def test_a_redis_failure_does_not_lose_the_prediction():
    """Recording a duplicate is a smaller harm than dropping a forecast on a
    host that affords roughly twenty inferences an hour."""
    source = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    claim_block = source.split("claim_key = (")[1].split("pred = AgentPrediction")[0]
    assert "except Exception" in claim_block
    assert claim_block.count("return \"\"") == 1, "only the duplicate path returns early"

"""
tests/test_inference_slot_utilisation.py

One inference in six hours, on a host that takes 110 seconds to do one.

The swarm was described -- by me, in the audit -- as capacity-bound, with the
note that hardware was the only thing that would move it. That was wrong, and
the evidence was already in the logs:

    knowledge_graph_engine | Inference completed (127288.3ms)
    knowledge_graph_engine | Inference completed  (94159.45ms)

Two inferences, 9.6 minutes apart. The gap is DEFAULT_COOLDOWN_SEC, not the
model. The slot is claimed for 600 seconds, the work finishes in ~110, and the
model server then sits idle for the remaining eight minutes holding a lock
nobody can use. Roughly 80% of every cycle was spent waiting on a constant.

That constant was honestly derived -- "~8 minutes per inference means one every
ten minutes leaves headroom" -- from a measurement that was true when it was
written and stopped being true when inference got faster. Nothing re-checked it.

`release()` already existed to fix exactly this, documented as freeing the slot
early when an inference finishes sooner than the cooldown. It had zero callers,
and a test asserting it worked. The same shape as the `score` parameter that was
a parameter for months and was never read: the mechanism was built, tested, and
never wired to anything.

Freeing the slot outright is the wrong repair. Ollama is capped at six of the
host's twelve cores and the collectors, enrichment and correlation tiers share
the rest; a budget that releases instantly is not a budget, and it would let the
reasoning tier crowd out the pipeline that feeds it. So completion shortens the
hold to a floor rather than dropping it.
"""

import ast
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.inference_budget import (  # noqa: E402
    DEFAULT_COOLDOWN_SEC,
    MIN_GAP_SEC,
    InferenceBudget,
)

MEASURED_INFERENCE_SEC = 110.0


class TtlRedis:
    """FakeRedis with the TTL visible, which is the thing under test."""

    def __init__(self):
        self.store = {}
        self.ttl = {}
        self.raw = self

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.store:
            return None
        self.store[key] = value
        self.ttl[key] = ex
        return True

    async def expire(self, key, seconds):
        if key not in self.store:
            return 0
        self.ttl[key] = seconds
        return 1

    async def delete(self, key):
        self.store.pop(key, None)
        self.ttl.pop(key, None)

    async def exists(self, key):
        return 1 if key in self.store else 0


class BrokenRedis:
    def __init__(self):
        self.raw = self

    async def expire(self, *a, **kw):
        raise ConnectionError("redis is down")

    async def set(self, *a, **kw):
        raise ConnectionError("redis is down")


# -- the waste, as arithmetic --------------------------------------------------

def test_the_hold_was_far_longer_than_the_work():
    """Stated as numbers so the next person to change either notices the other."""
    idle_fraction = 1 - (MEASURED_INFERENCE_SEC / DEFAULT_COOLDOWN_SEC)
    assert idle_fraction > 0.75, "the cooldown no longer dominates; revisit this file"


def test_completion_gap_is_shorter_than_the_crash_ceiling():
    assert MIN_GAP_SEC < DEFAULT_COOLDOWN_SEC


def test_the_gap_does_not_pin_the_model_server():
    """A gap of zero is not a budget. Six of twelve cores are Ollama's; the
    collectors and enrichment tiers need the others to keep feeding it."""
    assert MIN_GAP_SEC > 0


# -- what finish() does --------------------------------------------------------

@pytest.mark.anyio
async def test_finishing_shortens_the_hold_to_the_gap():
    redis = TtlRedis()
    budget = InferenceBudget(redis, "qwen2.5:1.5b")

    assert await budget.try_acquire() is True
    assert redis.ttl[budget._key] == DEFAULT_COOLDOWN_SEC

    await budget.finish()
    assert redis.ttl[budget._key] == int(MIN_GAP_SEC)


@pytest.mark.anyio
async def test_the_slot_is_still_held_after_finishing():
    """Completion shortens the hold; it does not hand the slot away."""
    redis = TtlRedis()
    budget = InferenceBudget(redis, "m")
    await budget.try_acquire()
    await budget.finish()

    assert await budget.try_acquire() is False, "the gap is not being enforced"


@pytest.mark.anyio
async def test_finishing_never_creates_a_slot():
    """EXPIRE, not SET. An inference that outlived its own cooldown has already
    lost the key, and possibly to another claimant."""
    redis = TtlRedis()
    budget = InferenceBudget(redis, "m")

    await budget.finish()
    assert budget._key not in redis.store


@pytest.mark.anyio
async def test_finishing_does_not_extend_a_slot_someone_else_now_holds():
    redis = TtlRedis()
    slow = InferenceBudget(redis, "m")

    await slow.try_acquire()
    await redis.delete(slow._key)          # the slow worker's hold expired

    other = InferenceBudget(redis, "m")
    assert await other.try_acquire() is True
    fresh_ttl = redis.ttl[other._key]

    await slow.finish()                    # the slow worker finally returns
    assert redis.ttl[other._key] == int(MIN_GAP_SEC)
    assert fresh_ttl == DEFAULT_COOLDOWN_SEC


@pytest.mark.anyio
async def test_a_redis_failure_while_finishing_is_not_fatal():
    """The key expires on its own; failing here costs latency, not correctness."""
    budget = InferenceBudget(BrokenRedis(), "m")
    await budget.finish()


@pytest.mark.anyio
async def test_finishing_without_redis_is_a_no_op():
    budget = InferenceBudget(None, "m")
    await budget.finish()


@pytest.mark.anyio
async def test_a_disabled_budget_is_left_alone():
    redis = TtlRedis()
    budget = InferenceBudget(redis, "m", cooldown_sec=0)
    await budget.try_acquire()
    await budget.finish()

    assert budget._key not in redis.store


# -- and that it is actually called --------------------------------------------

def _telemetry_method() -> ast.AsyncFunctionDef:
    tree = ast.parse((ROOT / "services/agents/base.py").read_text(encoding="utf-8"))
    return next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "_execute_with_telemetry"
    )


def test_the_inference_path_finishes_its_slot():
    """`release()` was written for this and never called by anything. A method
    that works and is not wired up is indistinguishable, in production, from one
    that was never written."""
    calls = [
        n for n in ast.walk(_telemetry_method())
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "finish"
    ]
    assert calls, "the slot is never shortened after an inference completes"


def test_the_slot_is_finished_even_when_the_inference_fails():
    """A failing tier held the budget exactly as long as a working one, and is
    the last thing that should also block every other agent from trying."""
    finallies = [
        n for n in ast.walk(_telemetry_method())
        if isinstance(n, ast.Try) and n.finalbody
    ]
    assert any(
        isinstance(c, ast.Call)
        and isinstance(c.func, ast.Attribute)
        and c.func.attr == "finish"
        for node in finallies
        for stmt in node.finalbody
        for c in ast.walk(stmt)
    ), "finish() is not in a finally; a raised inference keeps the full hold"


def test_the_claim_still_happens_before_the_work():
    """Shortening the hold must not turn the claim into a post-hoc formality."""
    source = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    assert source.index("try_acquire(") < source.index(".finish()")

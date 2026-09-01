"""
tests/test_inference_fairness.py

Every completed inference in the system belonged to one agent.

Traced end to end, this is why no prediction has ever been recorded:

  1. `record_prediction` works. Called directly against live Redis it writes the
     key and the ticker index with a 36-hour TTL. The end of the path is fine.

  2. Messages worth simulating do arrive. Twelve consecutive messages taken off
     the wargamer's own input topics all passed `_is_worth_simulating`. Its
     significance gate is not the blocker either.

  3. The wargamer then peeks at the budget, finds the slot held, and returns
     None -- every time, for hours. Zero "WARGAME SIMULATION" lines.

  4. The slot is held by knowledge_graph_engine, continuously. Over two minutes
     of agents.telemetry: two COMPLETE and one THINKING, all its own, and no
     FAILED from anyone.

So nothing is broken between the wargamer and the prediction. The wargamer never
gets a turn.

The arithmetic decides it. knowledge_graph_engine consumes its stream at 7.9
messages a second, the wargamer at 0.07 -- a hundred to one -- and KGE's batch
path claims the slot directly while the wargamer politely calls `is_available()`
first and backs off. The peek was added so an agent would not build expensive
context for work that cannot run, which is right; what it also did was make the
polite caller *invisible*. Backing off left no trace, so the busy agent had no
way to know anyone else wanted a turn, and on a slot that frees for an instant
every minute the agent polling a hundred times more often takes all of it.

This failure mode is already documented one class up: the `lane` docstring
describes reasoning being starved by the agents in exactly the same way, and the
fix there was a dedicated lane. Lanes do not generalise -- each one is another
concurrent request against an Ollama running OLLAMA_NUM_PARALLEL=1, so handing
every starved agent its own lane converts starvation into queueing. Turn-taking
is the version that scales.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.inference_budget import InferenceBudget  # noqa: E402


class FakeRedis:
    """SET NX EX, plus the set operations fairness needs."""

    def __init__(self):
        self.store = {}
        self.sets = {}
        self.hashes = {}
        self.ttl = {}
        self.raw = self

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.store:
            return None
        self.store[key] = value
        self.ttl[key] = ex
        return True

    async def get(self, key):
        return self.store.get(key)

    async def delete(self, key):
        self.store.pop(key, None)

    async def exists(self, key):
        return 1 if key in self.store else 0

    async def expire(self, key, seconds):
        if key in self.store:
            self.ttl[key] = seconds
            return 1
        return 0

    async def zadd(self, key, mapping, nx=False):
        z = self.sets.setdefault(key, {})
        for member, score in mapping.items():
            if nx and member in z:
                continue
            z[member] = score
        return len(mapping)

    async def zrem(self, key, *values):
        z = self.sets.get(key, {})
        for v in values:
            z.pop(v, None)
        return len(values)

    async def zrange(self, key, start, end):
        z = self.sets.get(key, {})
        ordered = [m for m, _ in sorted(z.items(), key=lambda kv: kv[1])]
        return ordered[start:end + 1] if end >= 0 else ordered[start:]

    async def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value
        return 1

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def hdel(self, key, *fields):
        h = self.hashes.get(key, {})
        for f in fields:
            h.pop(f, None)
        return len(fields)

    async def members(self, key):
        return set(self.sets.get(key, {}))


class BrokenRedis:
    def __init__(self):
        self.raw = self

    async def set(self, *a, **kw):
        raise ConnectionError("down")

    async def get(self, *a, **kw):
        raise ConnectionError("down")

    async def exists(self, *a, **kw):
        raise ConnectionError("down")

    async def zadd(self, *a, **kw):
        raise ConnectionError("down")

    async def zrange(self, *a, **kw):
        raise ConnectionError("down")

    async def hset(self, *a, **kw):
        raise ConnectionError("down")

    async def hgetall(self, *a, **kw):
        raise ConnectionError("down")

    async def expire(self, *a, **kw):
        raise ConnectionError("down")


def _budget(redis, owner):
    return InferenceBudget(redis, "qwen2.5:1.5b", owner=owner)


# -- the starvation, reproduced ------------------------------------------------

@pytest.mark.anyio
async def test_a_busy_agent_no_longer_takes_every_consecutive_slot():
    """The measured behaviour: one agent held the slot for hours."""
    redis = FakeRedis()
    busy = _budget(redis, "knowledge_graph_engine")
    slow = _budget(redis, "adversarial_wargamer")

    assert await busy.try_acquire() is True          # busy wins the first
    assert await slow.is_available() is False        # slow finds it held, and says so
    await busy.finish()
    await redis.delete(busy._key)                    # the hold lapses

    assert await busy.try_acquire() is False, "the busy agent took a second turn"
    assert await slow.try_acquire() is True, "the waiting agent still never runs"


@pytest.mark.anyio
async def test_backing_off_registers_interest():
    """A polite caller that leaves no trace cannot be given a turn."""
    redis = FakeRedis()
    busy = _budget(redis, "knowledge_graph_engine")
    slow = _budget(redis, "adversarial_wargamer")

    await busy.try_acquire()
    await slow.is_available()

    assert "adversarial_wargamer" in await redis.members(busy._waiters_key)


@pytest.mark.anyio
async def test_a_denied_claim_also_registers_interest():
    """Not every caller peeks first; both routes must count as waiting."""
    redis = FakeRedis()
    busy = _budget(redis, "knowledge_graph_engine")
    other = _budget(redis, "quant_trading_engine")

    await busy.try_acquire()
    assert await other.try_acquire() is False

    assert "quant_trading_engine" in await redis.members(busy._waiters_key)


@pytest.mark.anyio
async def test_the_turn_passes_round_robin():
    """Three agents, each gets one.

    Every agent polls each round, which is what the live ones do -- the busy one
    many times a second. An agent only counts as waiting if it actually asked,
    so a round where the others never poll is a round they have not entered.
    """
    redis = FakeRedis()
    agents = [_budget(redis, n) for n in ("kge", "wargamer", "quant")]
    winners = []

    for _ in range(3):
        round_winner = None
        for a in agents:
            if round_winner is None and await a.try_acquire():
                round_winner = a
                continue
            await a.is_available()          # everyone else registers interest
        assert round_winner is not None, f"nobody could run: {winners}"
        winners.append(round_winner.owner)
        await redis.delete(round_winner._key)

    assert len(set(winners)) == 3, f"the slot did not rotate: {winners}"


# -- and it must not cost throughput when nobody is waiting ---------------------

@pytest.mark.anyio
async def test_an_uncontended_agent_keeps_every_slot():
    """This costs throughput only in the case it exists for. With nobody else
    asking, the busy agent proceeds exactly as before."""
    redis = FakeRedis()
    only = _budget(redis, "knowledge_graph_engine")

    for _ in range(5):
        assert await only.try_acquire() is True
        await redis.delete(only._key)


@pytest.mark.anyio
async def test_a_waiter_that_stops_asking_stops_blocking():
    """Interest is removed when the waiter is served, so a departed agent does
    not hold the slot open against everyone forever."""
    redis = FakeRedis()
    busy = _budget(redis, "kge")
    slow = _budget(redis, "wargamer")

    await busy.try_acquire()
    await slow.is_available()
    await redis.delete(busy._key)

    assert await slow.try_acquire() is True
    assert "wargamer" not in await redis.members(busy._waiters_key)
    await redis.delete(slow._key)

    assert await busy.try_acquire() is True, "the served waiter still blocks the other agent"


@pytest.mark.anyio
async def test_an_agent_does_not_yield_to_itself():
    redis = FakeRedis()
    only = _budget(redis, "kge")

    await only.try_acquire()
    await only.is_available()          # registers itself, which must not count
    await redis.delete(only._key)

    assert await only.try_acquire() is True


# -- degradation ---------------------------------------------------------------

@pytest.mark.anyio
async def test_an_unnamed_caller_is_unaffected():
    """Fairness needs identity. Without it the budget behaves exactly as it did,
    rather than refusing work it cannot attribute."""
    redis = FakeRedis()
    anon = InferenceBudget(redis, "m")

    for _ in range(3):
        assert await anon.try_acquire() is True
        await redis.delete(anon._key)


@pytest.mark.anyio
async def test_a_redis_failure_does_not_block_inference():
    """Fairness is an optimisation. Failing it closed would silence the tier."""
    budget = _budget(BrokenRedis(), "kge")
    assert await budget.try_acquire() is True
    assert await budget.is_available() is True


@pytest.mark.anyio
async def test_fairness_is_checked_before_the_claim():
    """Yielding after claiming would take the slot and hand it back, which is
    the same starvation with extra steps."""
    redis = FakeRedis()
    busy = _budget(redis, "kge")
    slow = _budget(redis, "wargamer")

    await busy.try_acquire()
    await slow.is_available()
    await redis.delete(busy._key)

    assert await busy.try_acquire() is False
    assert busy._key not in redis.store, "the yielding agent claimed the slot anyway"


# -- the peek must answer the question the claim will --------------------------

@pytest.mark.anyio
async def test_the_peek_refuses_when_it_is_not_this_callers_turn():
    """Seven wargames began and none completed.

    The first version of fairness applied the rule at the claim and not at the
    peek, so a waiting agent was told the slot was free, paid for a Neo4j
    subgraph query and a Redis fetch building its context, and was then refused
    by the rule the peek had not checked. Nothing logged it: InferenceShed is a
    BaseException, so the run simply vanished -- no completion, no skip, no
    error, exactly the silent shape this audit keeps finding.
    """
    redis = FakeRedis()
    holder = _budget(redis, "quant")
    first = _budget(redis, "kge")
    second = _budget(redis, "wargamer")

    await holder.try_acquire()              # somebody else holds the slot
    await first.is_available()              # kge asks first and is refused
    await second.is_available()             # wargamer queues behind it
    await redis.delete(holder._key)         # the hold lapses; slot is free

    # Free, but kge has been waiting longer, so it is not the wargamer's turn.
    assert await second.is_available() is False, "the peek promises a turn the claim will refuse"
    assert await first.is_available() is True


@pytest.mark.anyio
async def test_the_peek_and_the_claim_agree():
    """Whatever the rule is, both gates must apply it."""
    redis = FakeRedis()
    a = _budget(redis, "kge")
    b = _budget(redis, "wargamer")

    await a.try_acquire()
    await b.is_available()
    await redis.delete(a._key)

    for budget in (a, b):
        peek = await budget.is_available()
        claimed = await budget.try_acquire()
        assert peek == claimed, f"{budget.owner}: peek={peek} claim={claimed}"
        if claimed:
            await redis.delete(budget._key)


@pytest.mark.anyio
async def test_an_agent_that_stops_asking_stops_holding_the_head():
    """Ordering never refreshes while a caller keeps asking, which is what makes
    it fair -- and would let a departed agent block the queue until the key
    expired."""
    import time as _time

    redis = FakeRedis()
    gone = _budget(redis, "departed")
    live = _budget(redis, "wargamer")

    await gone._note_interest()
    await live._note_interest()
    # The departed agent asked once and stopped. Staleness is judged by when it
    # *last* asked, kept in a separate hash -- pruning on the queue score
    # removed whoever had waited longest, which is the ordering inverted.
    redis.hashes[gone._seen_key]["departed"] = str(_time.time() - 10_000)

    assert await live._yields_to_a_waiter() is False
    assert "departed" not in await redis.members(live._waiters_key)


@pytest.mark.anyio
async def test_a_stub_that_answers_with_anything_does_not_block_everyone():
    """`is_available()` already checks the type of EXISTS rather than coercing
    it, for exactly this reason. The queue head needs the same care: a mock
    whose zrange returns a truthy object that is not a name would read as
    "somebody is ahead of you" for every caller, forever."""

    class VagueRedis:
        def __init__(self):
            self.raw = self

        async def zrange(self, *a, **kw):
            return object()          # truthy, not a list of names

        async def zremrangebyscore(self, *a, **kw):
            return 0

    assert await _budget(VagueRedis(), "kge")._yields_to_a_waiter() is False


# -- the two ways this mechanism could starve the callers it protects ---------

@pytest.mark.anyio
async def test_a_yielding_caller_joins_the_queue_it_yields_to():
    """The graph engine's batch path claims directly and never peeks.

    Yielding without registering meant it could never become head of the queue
    it kept standing aside for -- it would yield to everyone, forever. That is
    the starvation this whole mechanism exists to end, pointed the other way.
    """
    redis = FakeRedis()
    holder = _budget(redis, "quant")
    waiter = _budget(redis, "wargamer")
    direct = _budget(redis, "kge")            # claims without ever peeking

    await holder.try_acquire()
    await waiter.is_available()               # wargamer queues
    await redis.delete(holder._key)           # slot frees

    assert await direct.try_acquire() is False, "kge should yield to the older waiter"
    assert "kge" in await redis.members(direct._waiters_key), \
        "a caller that yields never enters the queue"


@pytest.mark.anyio
async def test_the_longest_waiter_is_not_the_first_pruned():
    """Ordering is by first ask; staleness is by last ask. One value cannot do
    both jobs -- scoring on first-ask and pruning on the same number removed
    whoever had waited longest and re-added them at the back."""
    import time as _time

    redis = FakeRedis()
    patient = _budget(redis, "wargamer")
    recent = _budget(redis, "kge")

    await patient._note_interest()
    await recent._note_interest()

    # wargamer asked first and has kept asking ever since
    redis.sets[patient._waiters_key]["wargamer"] = _time.time() - 10_000
    redis.hashes[patient._seen_key]["wargamer"] = str(_time.time())

    assert await recent._yields_to_a_waiter() is True, \
        "the longest waiter was pruned for waiting"
    assert "wargamer" in await redis.members(patient._waiters_key)


@pytest.mark.anyio
async def test_a_fractional_gap_does_not_become_no_gap():
    """int(0.5) is 0, and EXPIRE 0 deletes the key -- turning the floor that
    keeps the model server off 100% duty into no floor at all."""
    import shared.utils.inference_budget as mod

    redis = FakeRedis()
    budget = _budget(redis, "kge")
    monkey = mod.MIN_GAP_SEC
    try:
        mod.MIN_GAP_SEC = 0.5
        await budget.try_acquire()
        await budget.finish()
        assert redis.ttl.get(budget._key, 0) >= 1, "the gap expired the slot outright"
    finally:
        mod.MIN_GAP_SEC = monkey

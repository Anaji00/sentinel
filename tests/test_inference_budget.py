"""Keeps LLM intake inside what the host can actually perform.

Measured on this deployment: one knowledge-graph inference took 482 seconds --
about 180 a day. Its input stream carries 16,000 events every six hours, and the
anomaly distribution is flat enough that even a 0.95 threshold admits ~14,000 a
day. No threshold closes a 78x gap.

The failure mode was a stall, not slowness: the consumer paid eight minutes per
qualifying message, committed nothing meanwhile, and lost ground at 180 messages
a minute until `enriched.events` had 395,000 of backlog -- years of work that
would never be done. So the intake sheds instead of queuing.
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.inference_budget import DEFAULT_COOLDOWN_SEC, InferenceBudget  # noqa: E402


class FakeRedis:
    """Enough of SET NX EX to test the claim, including its atomicity."""

    def __init__(self):
        self.store = {}
        self.raw = self

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def delete(self, key):
        self.store.pop(key, None)


class BrokenRedis:
    def __init__(self):
        self.raw = self

    async def set(self, *a, **kw):
        raise ConnectionError("redis is down")


@pytest.mark.anyio
async def test_first_caller_is_admitted():
    budget = InferenceBudget(FakeRedis(), "qwen2.5:3b")
    assert await budget.try_acquire(0.9) is True


@pytest.mark.anyio
async def test_second_caller_is_shed_while_the_model_is_busy():
    """This is the whole point: the message is dropped, not queued behind an
    eight-minute inference."""
    redis = FakeRedis()
    budget = InferenceBudget(redis, "qwen2.5:3b")
    assert await budget.try_acquire(0.9) is True
    for _ in range(50):
        assert await budget.try_acquire(0.99) is False
    assert budget.shed == 50


@pytest.mark.anyio
async def test_separate_agents_share_one_budget_per_model():
    """Several agent processes share one Ollama. A per-process limiter would
    multiply by process count and rebuild the queue it exists to prevent."""
    redis = FakeRedis()
    a = InferenceBudget(redis, "qwen2.5:3b")
    b = InferenceBudget(redis, "qwen2.5:3b")
    assert await a.try_acquire() is True
    assert await b.try_acquire() is False, "a second process bypassed the budget"


@pytest.mark.anyio
async def test_different_models_do_not_block_each_other():
    redis = FakeRedis()
    heavy = InferenceBudget(redis, "qwen2.5:3b")
    fast = InferenceBudget(redis, "qwen2.5:1.5b")
    assert await heavy.try_acquire() is True
    assert await fast.try_acquire() is True


@pytest.mark.anyio
async def test_release_frees_the_slot_early():
    redis = FakeRedis()
    budget = InferenceBudget(redis, "m")
    await budget.try_acquire()
    await budget.release()
    assert await budget.try_acquire() is True


@pytest.mark.anyio
async def test_a_redis_outage_admits_rather_than_silencing_reasoning():
    """The budget is an optimisation. Failing closed would take the reasoning
    tier offline over an unrelated outage."""
    budget = InferenceBudget(BrokenRedis(), "m")
    assert await budget.try_acquire() is True


@pytest.mark.anyio
async def test_zero_cooldown_disables_the_limiter():
    budget = InferenceBudget(FakeRedis(), "m", cooldown_sec=0)
    for _ in range(5):
        assert await budget.try_acquire() is True


def test_default_cooldown_matches_measured_throughput():
    """~482s per inference; a shorter cooldown would rebuild the queue."""
    assert DEFAULT_COOLDOWN_SEC >= 482, (
        "cooldown is shorter than a single measured inference, so work would queue"
    )


@pytest.mark.anyio
async def test_stats_report_the_shed_rate():
    redis = FakeRedis()
    budget = InferenceBudget(redis, "m")
    await budget.try_acquire()
    await budget.try_acquire()
    s = budget.stats
    assert s["admitted"] == 1 and s["shed"] == 1
    assert s["admit_rate"] == 0.5


# ── the shed signal must survive the dispatch loop intact ────────────────────

from shared.utils.inference_budget import InferenceShed  # noqa: E402


def test_shed_is_not_caught_by_the_agents_broad_handlers():
    """Ten of fifteen inference call sites wrap the call in `except Exception`.

    If a shed were an ordinary Exception those handlers would swallow it and the
    agent would continue as though a model had answered -- writing a brief, or a
    graph edge, from nothing. Same reasoning as asyncio.CancelledError.
    """
    assert issubclass(InferenceShed, BaseException)
    assert not issubclass(InferenceShed, Exception)

    swallowed = False
    try:
        try:
            raise InferenceShed("agent", "model")
        except Exception:
            swallowed = True
    except InferenceShed:
        pass
    assert not swallowed, "a broad handler swallowed the shed signal"


def test_shed_is_not_treated_as_a_dispatch_failure():
    """The dispatch loop DLQs anything matching isinstance(r, Exception).

    A shed message is not broken -- there is simply no capacity to think about
    it -- so it must not land in the dead letter queue.
    """
    shed = InferenceShed("knowledge_graph_engine", "qwen2.5:3b")
    assert not isinstance(shed, Exception), "shed messages would be sent to the DLQ"


@pytest.mark.anyio
async def test_gather_captures_shed_so_the_consumer_survives():
    """The loop uses gather(return_exceptions=True) and commits afterwards.

    If the signal escaped gather it would kill the batch, skip the commit, and
    the same unprocessable messages would be redelivered forever.
    """
    import asyncio

    async def sheds():
        raise InferenceShed("a", "m")

    async def works():
        return "ok"

    results = await asyncio.gather(works(), sheds(), return_exceptions=True)
    assert results[0] == "ok"
    assert isinstance(results[1], InferenceShed), "shed escaped gather and would kill the batch"


def test_only_the_base_class_claims_the_budget():
    """Two claims for one inference would deny the caller with its own claim."""
    kg = (ROOT / "services/agents/knowledge_graph_engine.py").read_text(encoding="utf-8")
    assert "try_acquire" not in kg, "the knowledge-graph engine claims a slot the base class already claimed"

    base = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    assert base.count("try_acquire") == 1, "the budget is claimed more than once per inference"


# ── peeking without claiming ─────────────────────────────────────────────────

class FakeRedisWithExists(FakeRedis):
    async def exists(self, key):
        return 1 if key in self.store else 0


@pytest.mark.anyio
async def test_peek_does_not_consume_the_slot():
    """Callers peek to skip expensive preparation, then the base class claims.

    If peeking claimed, the first peek would take the budget and the real claim
    a moment later would be refused by it.
    """
    redis = FakeRedisWithExists()
    budget = InferenceBudget(redis, "m")
    for _ in range(5):
        assert await budget.is_available() is True
    assert await budget.try_acquire() is True, "peeking consumed the slot"


@pytest.mark.anyio
async def test_peek_reports_busy_once_claimed():
    redis = FakeRedisWithExists()
    budget = InferenceBudget(redis, "m")
    await budget.try_acquire()
    assert await budget.is_available() is False


@pytest.mark.anyio
async def test_peek_degrades_open_when_redis_is_unavailable():
    budget = InferenceBudget(BrokenRedis(), "m")
    assert await budget.is_available() is True


def test_the_peek_precedes_dedup_marking_in_the_engine():
    """Marking a message processed and then shedding it burns the dedup key for
    an hour, so the same headline is suppressed later when capacity exists."""
    src = (ROOT / "services/agents/knowledge_graph_engine.py").read_text(encoding="utf-8")
    peek_at = src.index("is_available()")
    mark_at = src.index("mark_processed(dedup_key")
    assert peek_at < mark_at, "the engine marks work as done before deciding to shed it"


# ── priority weighting toward the domains this platform is for ───────────────

from shared.utils.inference_budget import (  # noqa: E402
    PRIORITY_COOLDOWN_SEC,
    is_priority_domain,
)


@pytest.mark.parametrize("domain", ["news", "tradfi", "osint", "macro", "filings", "social"])
def test_the_domains_a_person_reads_are_priority(domain):
    assert is_priority_domain(domain) is True


@pytest.mark.parametrize("domain", ["maritime", "aviation", "vessel", "flight"])
def test_routine_telemetry_is_not(domain):
    """These are volume. They were producing 1,100 events/min on their own."""
    assert is_priority_domain(domain) is False


def test_event_types_are_matched_on_their_prefix():
    """Messages arrive as 'equity_block', 'vessel_position', 'crypto_trade'."""
    assert is_priority_domain("equity_block") is True
    assert is_priority_domain("crypto_trade") is True
    assert is_priority_domain("vessel_position") is False
    assert is_priority_domain("flight_anomaly") is False


def test_an_absent_domain_is_not_promoted():
    assert is_priority_domain(None) is False
    assert is_priority_domain("") is False


@pytest.mark.anyio
async def test_priority_work_releases_the_slot_sooner():
    """The share shifts through hold time, not through a second lane."""
    class RecordingRedis(FakeRedis):
        def __init__(self):
            super().__init__()
            self.ttls = []

        async def set(self, key, value, ex=None, nx=False):
            ok = await super().set(key, value, ex=ex, nx=nx)
            if ok:
                self.ttls.append(ex)
            return ok

    redis = RecordingRedis()
    b = InferenceBudget(redis, "m")
    await b.try_acquire(domain="news")
    redis.store.clear()
    await b.try_acquire(domain="maritime")
    priority_hold, routine_hold = redis.ttls
    assert priority_hold < routine_hold, "priority work holds the slot at least as long as routine"
    assert priority_hold == PRIORITY_COOLDOWN_SEC


@pytest.mark.anyio
async def test_only_one_inference_runs_at_a_time_regardless_of_priority():
    """Concurrency must not rise: the model server is single-threaded."""
    redis = FakeRedis()
    b = InferenceBudget(redis, "m")
    assert await b.try_acquire(domain="news") is True
    assert await b.try_acquire(domain="tradfi") is False, "a second priority claim ran concurrently"
    assert await b.try_acquire(domain="maritime") is False


def test_priority_hold_can_never_exceed_the_standard_hold():
    """A misconfiguration that made 'priority' wait longer would invert intent."""
    b = InferenceBudget(None, "m", cooldown_sec=100, priority_cooldown_sec=9999)
    assert b.priority_cooldown_sec <= b.cooldown_sec

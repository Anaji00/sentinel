"""
tests/test_inference_batching.py

One inference, many answers.

The scarce resource on this deployment is the model *call*, not the token. An
inference runs three to six minutes on a CPU-only host and the shared budget
releases one slot every 600 seconds, so the swarm gets roughly twenty decisions
an hour against an input of about a hundred and fifty thousand events. Measured
over thirty minutes: 275 correlations, 10 inferences, 0 bulletins.

RadarAgent was spending a whole slot deciding whether to track one ticker. Ten
tickers in one prompt is ten decisions for the same slot -- the prompt grows by
a line each, while loading the model, evaluating the system prompt and waiting
for the slot are all paid once.

What these tests pin is the part that is easy to get wrong: a batch must never
answer a question nobody asked, must never leave a caller waiting forever, and
must distinguish "no verdict was reached" from "the verdict was no".
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from services.agents.base import InferenceBatcher  # noqa: E402


def _batcher(flush_fn, max_items=10, max_wait_sec=0.2):
    return InferenceBatcher(
        name="test", flush_fn=flush_fn, max_items=max_items, max_wait_sec=max_wait_sec
    )


# -- the win -------------------------------------------------------------------

@pytest.mark.anyio
async def test_a_full_batch_costs_one_call():
    """Ten questions, one inference. This is the entire point."""
    calls = []

    async def flush(items):
        calls.append(len(items))
        return {k: f"verdict-{k}" for k, _ in items}

    b = _batcher(flush, max_items=10)
    results = await asyncio.gather(*[b.submit(f"T{i}", {}) for i in range(10)])

    assert calls == [10], "the batch did not resolve in a single call"
    assert results == [f"verdict-T{i}" for i in range(10)]


@pytest.mark.anyio
async def test_every_caller_gets_its_own_answer():
    """Answers must be routed by key, not by arrival order."""

    async def flush(items):
        return {k: k.lower() for k, _ in items}

    b = _batcher(flush, max_items=3)
    got = await asyncio.gather(b.submit("AAA", {}), b.submit("BBB", {}), b.submit("CCC", {}))
    assert got == ["aaa", "bbb", "ccc"]


# -- a lone candidate must not wait forever -----------------------------------

@pytest.mark.anyio
async def test_a_partial_batch_flushes_on_the_timer():
    """A quiet stream must not leave one candidate waiting for company."""
    seen = []

    async def flush(items):
        seen.append(len(items))
        return {k: "ok" for k, _ in items}

    b = _batcher(flush, max_items=10, max_wait_sec=0.1)
    assert await b.submit("LONE", {}) == "ok"
    assert seen == [1]


# -- failure must not hang a caller -------------------------------------------

@pytest.mark.anyio
async def test_a_failed_batch_resolves_every_caller():
    """A hung handler is a worse failure than an unanswered question.

    If the inference is shed by the budget or times out, every caller must be
    released -- not left awaiting a future that will never complete.
    """

    async def flush(items):
        raise RuntimeError("inference shed")

    b = _batcher(flush, max_items=2)
    got = await asyncio.wait_for(
        asyncio.gather(b.submit("A", {}), b.submit("B", {})), timeout=5
    )
    assert got == [None, None]


@pytest.mark.anyio
async def test_an_unanswered_key_resolves_to_none_not_false():
    """"No verdict" and "the verdict was no" are different facts.

    A ticker the model skipped must never be recorded as decided against; it
    was not decided at all.
    """

    async def flush(items):
        return {"A": "answered"}          # B is missing from the answer

    b = _batcher(flush, max_items=2)
    a, bb = await asyncio.gather(b.submit("A", {}), b.submit("B", {}))
    assert a == "answered"
    assert bb is None


@pytest.mark.anyio
async def test_a_flush_returning_nothing_still_releases_callers():
    async def flush(items):
        return None

    b = _batcher(flush, max_items=2)
    got = await asyncio.wait_for(
        asyncio.gather(b.submit("A", {}), b.submit("B", {})), timeout=5
    )
    assert got == [None, None]


# -- batches must not bleed into one another ----------------------------------

@pytest.mark.anyio
async def test_a_second_batch_starts_clean():
    batches = []

    async def flush(items):
        batches.append([k for k, _ in items])
        return {k: "ok" for k, _ in items}

    b = _batcher(flush, max_items=2, max_wait_sec=0.1)
    await asyncio.gather(b.submit("A", {}), b.submit("B", {}))
    await asyncio.gather(b.submit("C", {}), b.submit("D", {}))

    assert batches == [["A", "B"], ["C", "D"]], "an item appeared in two batches"


# -- the radar wiring ----------------------------------------------------------

def _radar_source() -> str:
    return (ROOT / "services" / "agents" / "radar_agent.py").read_text(encoding="utf-8")


def test_radar_no_longer_spends_a_slot_per_ticker():
    source = _radar_source()
    assert "self._decision_batcher.submit(" in source
    assert "schema=RadarBatchDecision" in source
    assert "schema=RadarDecision" not in source, "a single-ticker dispatch remains"


def test_radar_only_acts_on_tickers_it_asked_about():
    """A model that invents a symbol must not have that verdict acted on."""
    source = _radar_source()
    assert "wanted = {t.upper() for t in tickers}" in source
    assert "if name in wanted and name not in out:" in source


def test_radar_distinguishes_no_verdict_from_a_negative_verdict():
    source = _radar_source()
    assert "if decision is None:" in source


def test_the_batch_size_is_configurable():
    source = _radar_source()
    assert "RADAR_BATCH_SIZE" in source
    assert "RADAR_BATCH_WAIT_SEC" in source


# -- the knowledge graph wiring ------------------------------------------------

def _kg_source() -> str:
    return (ROOT / "services" / "agents" / "knowledge_graph_engine.py").read_text(
        encoding="utf-8"
    )


def test_entity_classification_is_batched():
    """The cheapest question in the swarm was costing a whole 600s slot each,
    on the agent with the highest message rate in the tier."""
    source = _kg_source()
    assert "self._classification_batcher.submit(" in source
    assert "schema=EntityClassificationBatch" in source
    assert "schema=EntityClassification," not in source, "a single-entity dispatch remains"


def test_the_ontology_only_accepts_entities_it_asked_about():
    """A classification for a name the model invented must never be merged."""
    source = _kg_source()
    assert "if key in wanted and wanted[key] not in out:" in source


def test_an_unclassified_entity_is_not_given_a_label():
    """Unclassified costs a later inference. Wrongly labelled propagates into
    the graph and is far harder to notice."""
    source = _kg_source()
    assert "if classification is None:" in source
    assert "omit it" in source, "the prompt does not tell the model to omit rather than guess"


# -- a batch must be able to fill ----------------------------------------------

def test_a_batch_cannot_need_more_waiters_than_can_exist():
    """The stall I shipped.

    Each caller holds a dispatch slot while awaiting its batch. With dispatch
    concurrency 5 and a batch size of 10, only five callers can ever be waiting,
    so the size trigger is unreachable and every batch falls through to its
    timer while holding every slot the agent has. radar_agent's processed count
    froze at 5,292 with 4,902 messages of lag behind it.
    """
    b = InferenceBatcher(name="t", flush_fn=lambda items: {}, max_items=10, max_waiters=5)
    assert b.max_items == 5


def test_a_batch_within_capacity_is_left_alone():
    b = InferenceBatcher(name="t", flush_fn=lambda items: {}, max_items=8, max_waiters=24)
    assert b.max_items == 8


def test_dispatch_concurrency_exceeds_the_default_batch_sizes():
    """The clamp is a backstop; the default configuration should not need it."""
    source = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    assert 'os.getenv("AGENT_CONCURRENCY", "24")' in source


@pytest.mark.parametrize(
    "path", ["services/agents/radar_agent.py", "services/agents/knowledge_graph_engine.py"]
)
def test_batching_agents_declare_their_capacity(path):
    assert "max_waiters=self.dispatch_concurrency" in (ROOT / path).read_text(encoding="utf-8")


# -- the bug that only real latency exposes ------------------------------------

@pytest.mark.anyio
async def test_a_timer_flush_with_a_slow_call_still_resolves_its_callers():
    """The batcher shipped with a self-cancellation.

    _flush_after_wait calls _flush, which cancelled self._timer -- the task it
    was running on. CancelledError arrived at the next await, which in
    production is a multi-minute inference, so the flush died before resolving
    any future and every caller waited forever. radar_agent queued 26 candidates
    and produced no decisions at all.

    A flush that returns instantly hides this, which is why the original tests
    passed. This one is slow on purpose.
    """
    async def slow_flush(items):
        await asyncio.sleep(0.3)          # long enough to receive a cancellation
        return {k: "ok" for k, _ in items}

    b = _batcher(slow_flush, max_items=10, max_wait_sec=0.1)
    got = await asyncio.wait_for(b.submit("ONLY", {}), timeout=5)
    assert got == "ok", "the timer-driven flush cancelled itself"


@pytest.mark.anyio
async def test_a_slow_timer_flush_resolves_every_caller_in_the_batch():
    async def slow_flush(items):
        await asyncio.sleep(0.3)
        return {k: k.lower() for k, _ in items}

    b = _batcher(slow_flush, max_items=10, max_wait_sec=0.1)
    got = await asyncio.wait_for(
        asyncio.gather(b.submit("A", {}), b.submit("B", {}), b.submit("C", {})),
        timeout=5,
    )
    assert got == ["a", "b", "c"]


@pytest.mark.anyio
async def test_a_size_triggered_flush_still_cancels_the_pending_timer():
    """The cancellation is only wrong when it targets the current task."""
    async def flush(items):
        return {k: "ok" for k, _ in items}

    b = _batcher(flush, max_items=2, max_wait_sec=30.0)
    await asyncio.wait_for(
        asyncio.gather(b.submit("A", {}), b.submit("B", {})), timeout=5
    )
    assert b._timer is None, "a stale timer survived a size-triggered flush"


# -- a reserved lane, and only where starvation was measured -------------------

def test_radar_holds_a_reserved_inference_lane():
    """Batching multiplied the value of a slot radar was not getting.

    While sharing the common budget it queued 26 candidates over ten minutes
    and not one batch reached the model: knowledge_graph_engine,
    rule_synthesizer and stock_correlation_agent kept winning the shared key.
    """
    from services.agents.radar_agent import RadarAgent

    assert RadarAgent.INFERENCE_LANE == "radar"


def test_agents_share_the_common_budget_by_default():
    """Each lane is one more inference in flight against a single-threaded
    server. Lanes are granted on measured starvation, never by default."""
    from services.agents.base import SentinelAgent

    assert SentinelAgent.INFERENCE_LANE is None


def test_a_lane_gets_its_own_budget_key():
    from shared.utils.inference_budget import InferenceBudget

    shared = InferenceBudget(None, "qwen2.5:1.5b")._key
    reserved = InferenceBudget(None, "qwen2.5:1.5b", lane="radar")._key
    assert shared != reserved
    assert "radar" in reserved


def test_only_the_agents_that_need_one_declare_a_lane():
    """A guard against lanes accumulating quietly until the budget means
    nothing."""
    import services.agents.radar_agent as radar
    import services.agents.knowledge_graph_engine as kg
    import services.agents.consensus_engine as consensus

    assert radar.RadarAgent.INFERENCE_LANE == "radar"
    assert kg.KnowledgeGraphEngine.INFERENCE_LANE is None
    assert consensus.ConsensusEngine.INFERENCE_LANE is None


# -- a parked caller must never wedge the consumer -----------------------------

@pytest.mark.anyio
async def test_a_batch_that_never_resolves_releases_its_callers():
    """The stall this closes.

    A caller parked on a batch holds a dispatch slot, and the consume loop
    blocks once MAX_INFLIGHT_DISPATCHES of them accumulate. So a batch that
    never resolves does not just lose its own answers -- it stops the agent
    reading its topic at all. radar_agent sat at processed=5 for 29 minutes,
    consumer live and assigned, offsets frozen across all three partitions
    while lag climbed past 9,000.
    """
    never = asyncio.Event()

    async def hangs(items):
        await never.wait()                 # never set
        return {}

    b = InferenceBatcher(
        name="t", flush_fn=hangs, max_items=1, max_wait_sec=0.5, max_stall_sec=0.6
    )
    assert await asyncio.wait_for(b.submit("A", {}), timeout=5) is None


@pytest.mark.anyio
async def test_the_bound_does_not_cut_a_normal_batch_short():
    """A flush slower than the batch window but inside the ceiling still wins."""
    async def slow(items):
        await asyncio.sleep(0.4)
        return {k: "ok" for k, _ in items}

    b = InferenceBatcher(
        name="t", flush_fn=slow, max_items=1, max_wait_sec=0.1, max_stall_sec=5.0
    )
    assert await asyncio.wait_for(b.submit("A", {}), timeout=5) == "ok"


def test_the_stall_ceiling_is_configurable():
    source = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    assert "BATCH_MAX_STALL_SEC" in source
    assert "asyncio.wait_for(future, timeout=self.max_stall_sec)" in source

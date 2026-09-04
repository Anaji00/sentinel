"""Closes the two feedback loops that were recording data and never using it.

Both had the same shape: the writer was wired, the reader was wired, and the
step that turns observation into learning had no callers at all.

  1. Agents recorded predictions with a TTL and never scored them.
     update_scorecard() existed, uncalled, so every scorecard sat at its
     constructed default. That is not merely missing bookkeeping -- the
     consensus engine fuses agent opinions *weighted by these scorecards*, so a
     weighting that never moves means an agent that is consistently wrong
     carries exactly as much influence as one that is consistently right.

  2. Discovered correlation edges were registered for survival tracking and
     never re-tested. evaluate() and due_for_retest() had no callers, so an
     edge found once stayed in the graph as though it were still true.
"""
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

BASE = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
DISCOVERY = (ROOT / "services/correlation/statistical_discovery.py").read_text(encoding="utf-8")
CORRELATION_MAIN = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")


# ── harness for scoring behaviour ────────────────────────────────────────────
#
# The scorers are the part of the learning loop that decides what "correct"
# means, so they are exercised directly rather than asserted about in source
# text. Only redis and a logger are needed: neither scorer touches Kafka, the
# database or the model.

def _run(coro):
    import asyncio
    return asyncio.run(coro)


class _FakeRedisRaw:
    def __init__(self, store):
        self._store = store

    async def get(self, key):
        return self._store.get(key)


class _FakeRedis:
    def __init__(self, store):
        self.raw = _FakeRedisRaw(store)


def _scoring_agent(price=None, distribution=None):
    """A SentinelAgent with just enough wired to score a prediction."""
    import json
    import logging
    from services.agents.base import SentinelAgent

    store = {}
    if price is not None:
        store["sentinel:quotes:latest:TEST"] = json.dumps({"price": price})
    if distribution is not None:
        store["sentinel:prediction:outcomes:race"] = json.dumps(distribution)

    class _Concrete(SentinelAgent):
        @property
        def output_topic(self):
            return "test.topic"

        async def handle(self, message):
            return None

    agent = _Concrete.__new__(_Concrete)
    agent.redis = _FakeRedis(store)
    agent.logger = logging.getLogger("test.scoring")
    return agent


def _prediction(direction, entry_price, **kwargs):
    from services.agents.base import AgentPrediction
    return AgentPrediction(
        agent_name="test", ticker="TEST", direction=direction,
        conviction=0.6, entry_price=entry_price, **kwargs
    )


# ── agent self-calibration ───────────────────────────────────────────────────

def test_predictions_are_actually_scored():
    """update_scorecard had zero call sites anywhere in the codebase."""
    assert "await self.update_scorecard(" in BASE, (
        "predictions are still recorded and never scored"
    )


def test_the_resolver_runs_on_its_own():
    """A resolver nothing starts is the same defect one level up."""
    assert "_resolve_predictions_loop" in BASE
    # safe_create_task, not a bare create_task: the helper logs the exception
    # instead of letting a dead loop fail silently, which is the same defect
    # class this test exists to catch.
    assert "safe_create_task(self._resolve_predictions_loop())" in BASE


def test_an_unverifiable_prediction_is_not_counted():
    """Scoring against a price we do not have would manufacture a track record.

    Exercised rather than string-matched: the previous version asserted on the
    resolver's source text and broke the moment the scoring moved into its own
    method, without the property it guards having changed at all.
    """
    agent = _scoring_agent(price=None)
    assert _run(agent._score_directional(_prediction("up", 100.0))) is None


def test_direction_is_scored_in_both_directions():
    """A resolver that only understands "up" marks every short call wrong."""
    up = _scoring_agent(price=110.0)
    down = _scoring_agent(price=90.0)
    assert _run(up._score_directional(_prediction("up", 100.0))) is True
    assert _run(up._score_directional(_prediction("down", 100.0))) is False
    assert _run(down._score_directional(_prediction("down", 100.0))) is True
    assert _run(down._score_directional(_prediction("up", 100.0))) is False


def test_a_flat_close_is_not_a_win_for_down():
    """`moved_up = current > entry` scored every bearish call correct on no move.

    An unchanged price answers no directional question, so it is left uncounted
    rather than handed to whichever side sat on the false branch.
    """
    agent = _scoring_agent(price=100.0)
    assert _run(agent._score_directional(_prediction("down", 100.0))) is None
    assert _run(agent._score_directional(_prediction("up", 100.0))) is None
    assert _run(agent._score_directional(_prediction("flat", 100.0))) is True


def test_an_unrecognised_direction_is_not_silently_scored_as_down():
    """Anything that was not "up" used to take the `not moved_up` branch."""
    agent = _scoring_agent(price=90.0)
    assert _run(agent._score_directional(_prediction("sideways-ish", 100.0))) is None



def test_open_predictions_are_left_alone():
    """Scoring before the horizon elapses grades an unfinished answer."""
    fn = BASE[BASE.index("async def resolve_due_predictions"):][:4000]
    assert "time_horizon_hours * 3600" in fn


# ── edge survival ────────────────────────────────────────────────────────────

def test_registered_edges_are_retested():
    assert "async def retest_due_edges" in DISCOVERY
    assert "await self.survival.evaluate(" in DISCOVERY, "no verdict is ever recorded"
    assert "due_for_retest(" in DISCOVERY


def test_the_retest_is_scheduled():
    assert "_edge_retest_loop" in CORRELATION_MAIN
    assert "safe_create_task(_edge_retest_loop())" in CORRELATION_MAIN
    assert "discovery_engine.retest_due_edges()" in CORRELATION_MAIN


def test_discovery_and_retest_share_one_statistic():
    """Discovery correlates *returns*. A re-test on prices would fail edges that
    never changed, and the difference would read as decay rather than as a bug."""
    assert "def pearson_on_returns(" in DISCOVERY
    assert DISCOVERY.count("pearson_on_returns(") >= 2, (
        "the retest computes its own statistic instead of sharing discovery's"
    )


def test_the_shared_statistic_uses_returns_not_prices():
    fn = DISCOVERY[DISCOVERY.index("def pearson_on_returns"):]
    fn = fn[:fn.index("\ndef ", 10)]
    assert "np.diff(" in fn, "correlating raw prices would find spurious trends"
    assert "np.corrcoef(" in fn


def test_an_edge_without_data_is_pending_not_failed():
    """Absence of evidence is not a dead edge."""
    fn = DISCOVERY[DISCOVERY.index("async def retest_due_edges"):]
    fn = fn[:fn.index("\n    async def ", 10)]
    assert "skipped" in fn
    assert re.search(r"if coefficient is None:\s*\n\s*skipped \+= 1\s*\n\s*continue", fn), (
        "an edge with no usable series would be scored as a failure"
    )

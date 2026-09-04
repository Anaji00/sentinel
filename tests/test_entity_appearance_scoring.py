"""The wargamer's predictions must be able to resolve.

It names an entity it expects to be targeted next and records that through
record_prediction with entry_price=0.0. The resolver then handed it to
_score_directional, whose first guard is:

    if current is None or not pred.entry_price: return None

0.0 is falsy, so every wargame prediction returned None before anything was
looked up -- and even past that guard, the "ticker" is a name like "EVER VISTA"
or "airlines, airports", which has no price. Recorded, stored, never once
resolved, so the agent's Brier score sat at its 0.5 starting value forever
regardless of whether it was right.

Measured on the live corpus: 11 of 30 predictions name an entity that matches
events.primary_entity_name exactly, so there is a real loop to close here.
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.base import AgentPrediction, SentinelAgent  # noqa: E402


def _run(coro):
    """The suite's convention; pytest-asyncio is not a dependency here."""
    return asyncio.run(coro)


class _Db:
    """Stands in for TimescaleDB, recording what it was asked."""

    def __init__(self, known=True, hit=False):
        self.known, self.hit = known, hit
        self.queries = []

    async def query(self, sql, *params):
        self.queries.append((sql, params))
        if "occurred_at >" in sql:
            return [{"?column?": 1}] if self.hit else []
        return [{"?column?": 1}] if self.known else []


class _Agent(SentinelAgent):
    """SentinelAgent is abstract; the scorer under test needs neither method."""

    async def handle(self, event):
        return None

    @property
    def output_topic(self):
        return "test.topic"


def _agent(db):
    agent = _Agent.__new__(_Agent)
    agent.db = db
    import logging
    agent.logger = logging.getLogger("test")
    return agent


def _pred(ticker="EVER VISTA", hours=24, age_hours=48):
    created = datetime.now(timezone.utc) - timedelta(hours=age_hours)
    return AgentPrediction(
        agent_name="adversarial_wargamer",
        ticker=ticker,
        direction="bearish",
        conviction=0.73,
        entry_price=0.0,
        time_horizon_hours=hours,
        prediction_kind="entity_appearance",
        created_at=created.isoformat(),
    )


def test_an_entity_that_was_targeted_scores_correct():
    agent = _agent(_Db(known=True, hit=True))
    assert _run(agent._score_entity_appearance(_pred())) is True


def test_a_known_entity_that_stayed_quiet_scores_wrong():
    """Absence is a real answer here, unlike a price call. This is the case that
    lets the loop close: the horizon elapsed and nothing happened."""
    agent = _agent(_Db(known=True, hit=False))
    assert _run(agent._score_entity_appearance(_pred())) is False


def test_an_entity_the_platform_cannot_see_is_uncounted():
    """Not False. Scoring an unmatchable name as wrong would grade the agent on
    whether its phrasing happened to match a database column."""
    agent = _agent(_Db(known=False))
    assert _run(agent._score_entity_appearance(_pred("airlines, airports"))) is None


def test_the_window_starts_at_the_prediction_and_ends_at_the_horizon():
    db = _Db(known=True, hit=True)
    agent = _agent(db)
    pred = _pred(hours=24, age_hours=48)
    _run(agent._score_entity_appearance(pred))
    window = [q for q in db.queries if "occurred_at >" in q[0]]
    assert window, "no time-bounded lookup was made"
    start, end = window[0][1][0], window[0][1][1]
    assert (end - start) == timedelta(hours=24)
    assert start == datetime.fromisoformat(pred.created_at)


def test_only_anomalous_events_count_as_being_targeted():
    db = _Db(known=True, hit=True)
    _run(_agent(db)._score_entity_appearance(_pred()))
    window = [q for q in db.queries if "occurred_at >" in q[0]][0]
    assert "anomaly_score >" in window[0]
    assert window[1][2] == SentinelAgent.APPEARANCE_ANOMALY_FLOOR


def test_a_database_failure_is_uncounted_not_wrong():
    class _Broken:
        async def query(self, *a):
            raise RuntimeError("connection reset")
    assert _run(_agent(_Broken())._score_entity_appearance(_pred())) is None


def test_no_database_is_uncounted():
    assert _run(_agent(None)._score_entity_appearance(_pred())) is None


def test_the_model_annotations_the_model_appends_are_stripped():
    """Both recur in live data and both name a real entity."""
    assert SentinelAgent._entity_claim("CSN5086 (Centrality Multiplier: 1.00x)") == "CSN5086"
    assert SentinelAgent._entity_claim("MEA305 Entity") == "MEA305"
    assert SentinelAgent._entity_claim("  EVER VISTA  ") == "EVER VISTA"


def test_nothing_fuzzier_than_that_is_attempted():
    """Substring matching is what made the old resolution signals fire on 89%
    of events and resolve nothing honestly."""
    assert SentinelAgent._entity_claim("EVER VISTA") != "EVER"
    assert SentinelAgent._entity_claim("airlines, airports") == "airlines, airports"


def test_the_kind_defaults_to_price_so_stored_predictions_are_unaffected():
    """Every prediction already in Redis predates this field."""
    pred = AgentPrediction(agent_name="a", ticker="AAPL", direction="up",
                           conviction=0.6, entry_price=100.0)
    assert pred.prediction_kind == "price"


# ── The routing ───────────────────────────────────────────────────────────────
#
# Everything above calls the scorer directly, so deleting the branch in
# resolve_due_predictions that reaches it left all of them passing. The branch
# is the part that actually closes the loop, and it needs its own test.


class _Raw:
    def __init__(self, blob):
        self._blob, self.writes = blob, []

    async def scan_iter(self, match=None, count=None):
        for key in ("sentinel:predictions:adversarial_wargamer:p1",):
            yield key

    async def get(self, key):
        return self._blob

    async def set(self, key, value, ex=None):
        self.writes.append((key, value))


class _Redis:
    def __init__(self, blob):
        self.raw = _Raw(blob)


def _resolver(pred, scorer_result):
    agent = _Agent.__new__(_Agent)
    import logging
    agent.logger = logging.getLogger("test")
    agent.db = _Db(known=True, hit=True)
    agent.redis = _Redis(pred.model_dump_json())
    agent.name = "adversarial_wargamer"
    called = []

    async def _entity(p):
        called.append("entity")
        return scorer_result

    async def _directional(p):
        called.append("directional")
        return None

    async def _categorical(p):
        called.append("categorical")
        return None

    async def _scorecard(prediction_correct, conviction):
        called.append(f"scored:{prediction_correct}")

    agent._score_entity_appearance = _entity
    agent._score_directional = _directional
    agent._score_categorical = _categorical
    agent.update_scorecard = _scorecard
    return agent, called


def test_an_entity_prediction_reaches_the_entity_scorer():
    agent, called = _resolver(_pred(), True)
    resolved = _run(agent.resolve_due_predictions())
    assert "entity" in called, f"routed to {called} instead of the entity scorer"
    assert "directional" not in called
    assert resolved == 1


def test_the_result_reaches_the_scorecard():
    """Which is the point: the Brier score sat at 0.5 forever because nothing
    ever arrived here."""
    agent, called = _resolver(_pred(), False)
    _run(agent.resolve_due_predictions())
    assert "scored:False" in called, called


def test_a_price_prediction_still_routes_to_the_directional_scorer():
    pred = _pred()
    pred.prediction_kind = "price"
    agent, called = _resolver(pred, True)
    _run(agent.resolve_due_predictions())
    assert "directional" in called and "entity" not in called

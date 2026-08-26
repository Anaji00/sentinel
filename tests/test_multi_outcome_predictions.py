"""
tests/test_multi_outcome_predictions.py

Not every prediction market is a yes/no bet.

Three separate defects made the platform treat every market as binary:

  1. The collector read `market["outcomeNames"]`, a field the Gamma API does not
     return. Verified against the live API: the field is `outcomes`. Every token
     therefore fell through to an `f"Outcome {i}"` placeholder, and 158 stored
     events carried exactly two distinct outcome values.
  2. `outcomePrices` -- the market's own odds across the whole field -- was never
     read at all, so `outcome_prices` was null on every event.
  3. Both the collector and the enricher then mapped index 0 to YES and index 1
     to NO, and the enricher did it by *substring*, so "None of the above" and
     "Another candidate" were scored as NO bets.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load(name: str, relative: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# ── collector: parse what the API actually sends ─────────────────────────────

@pytest.fixture(scope="module")
def collector():
    return _load("collector_prediction_main", "services/collector-prediction/main.py")


def test_gamma_encodes_arrays_as_strings(collector):
    """Gamma sends these as JSON text, not arrays."""
    assert collector._json_list('["Yes", "No"]') == ["Yes", "No"]
    assert collector._json_list(["Yes", "No"]) == ["Yes", "No"]
    assert collector._json_list("") == []
    assert collector._json_list(None) == []
    assert collector._json_list("not json") == []
    assert collector._json_list('{"a": 1}') == []   # an object is not a field


def test_price_map_pairs_names_with_odds(collector):
    got = collector._price_map(["Yes", "No"], ["0.049", "0.951"])
    assert got == {"Yes": 0.049, "No": 0.951}


def test_price_map_refuses_to_guess_when_arrays_disagree(collector):
    """A misaligned pairing reads as authoritative and is worse than nothing."""
    assert collector._price_map(["A", "B", "C"], ["0.5", "0.5"]) == {}
    assert collector._price_map([], []) == {}


def test_only_a_true_two_sided_market_is_binary(collector):
    assert collector._is_binary_market(["Yes", "No"]) is True
    assert collector._is_binary_market(["No", "Yes"]) is True
    assert collector._is_binary_market(["Newsom", "Kelly", "Cuban"]) is False
    assert collector._is_binary_market(["Newsom", "Kelly"]) is False
    assert collector._is_binary_market(["Yes"]) is False
    assert collector._is_binary_market([]) is False


def test_collector_reads_outcomes_not_outcomenames():
    """The bug that produced 'Outcome 0' on every stored event.

    Asserted against the real shape of a Gamma market: `outcomes` present,
    `outcomeNames` absent.
    """
    source = (ROOT / "services/collector-prediction/main.py").read_text(encoding="utf-8")
    assert 'market.get("outcomes")' in source
    # The placeholder must no longer be a *probability* input anywhere.
    assert '"outcome 0"' not in source
    assert '"outcome 1"' not in source


# ── enricher: never invent a side that was not priced ────────────────────────

@pytest.fixture(scope="module")
def enricher():
    return _load("enrichment_prediction_enricher",
                 "services/enrichment/enrichers/prediction.py")


@pytest.mark.parametrize("name", [
    "None of the above", "Another candidate", "Nominee TBD",
    "Norway", "November winner", "Nothing happens",
])
def test_ordinary_outcome_names_are_not_no_bets(enricher, name):
    """`"no" in outcome` matched all of these. They are candidates, not a NO side."""
    assert name.strip().lower() not in enricher._NO_TOKENS


def test_binary_detection_requires_both_sides(enricher):
    assert enricher._is_binary_outcome_set(["Yes", "No"]) is True
    assert enricher._is_binary_outcome_set(["Newsom", "Kelly", "Cuban"]) is False
    assert enricher._is_binary_outcome_set(["Yes", "Maybe"]) is False


def test_multi_outcome_leaves_yes_no_unset():
    """1 - price is "any other candidate", not "no".

    PredictionMarketData makes both Optional precisely so a market that never
    priced a NO side does not have one attributed to it.
    """
    from shared.models import PredictionMarketData
    m = PredictionMarketData(
        market_id="x", question="Who wins the nomination?", outcome="Gavin Newsom",
        shares_traded=100.0, price_usd=0.1495,
        outcome_prices={"Gavin Newsom": 0.1495, "Mark Kelly": 0.09, "Other": 0.76},
    )
    assert m.yes_probability is None
    assert m.no_probability is None
    assert m.outcome_prices["Mark Kelly"] == 0.09


# ── agent scoring: a categorical call is not an up/down call ─────────────────

@pytest.fixture(scope="module")
def agent_base():
    from services.agents import base
    return base


def test_prediction_carries_an_outcome_space(agent_base):
    p = agent_base.AgentPrediction(
        agent_name="a", ticker="nomination-2028", direction="",
        conviction=0.6,
        outcome_space=["Newsom", "Kelly", "Cuban"],
        predicted_outcome="Newsom",
        market_key="nomination-2028",
    )
    assert p.outcome_space == ["Newsom", "Kelly", "Cuban"]
    assert p.predicted_outcome == "Newsom"
    assert p.resolved_outcome is None


# ── categorical resolution ───────────────────────────────────────────────────

def _run(coro):
    import asyncio
    return asyncio.run(coro)


def _categorical_agent(distribution):
    import json, logging
    from services.agents.base import SentinelAgent

    store = {}
    if distribution is not None:
        store["sentinel:prediction:outcomes:race"] = json.dumps(distribution)

    class _FakeRaw:
        async def get(self, key):
            return store.get(key)

    class _FakeRedis:
        raw = _FakeRaw()

    class _Concrete(SentinelAgent):
        @property
        def output_topic(self):
            return "test.topic"

        async def handle(self, message):
            return None

    agent = _Concrete.__new__(_Concrete)
    agent.redis = _FakeRedis()
    agent.logger = logging.getLogger("test.categorical")
    return agent


def _categorical(predicted):
    from services.agents.base import AgentPrediction
    return AgentPrediction(
        agent_name="test", ticker="race", direction="", conviction=0.6,
        outcome_space=["Newsom", "Kelly", "Cuban"],
        predicted_outcome=predicted, market_key="race",
    )


def test_a_named_outcome_is_scored_against_the_field():
    """The winner is the leader of the distribution, not "price went up"."""
    agent = _categorical_agent({"Newsom": 0.62, "Kelly": 0.21, "Cuban": 0.17})
    assert _run(agent._score_categorical(_categorical("Newsom"))) is True
    assert _run(agent._score_categorical(_categorical("Kelly"))) is False


def test_a_race_that_is_still_a_contest_is_not_resolved():
    """0.34 against 0.33 has not settled anything, so nobody is graded."""
    agent = _categorical_agent({"Newsom": 0.34, "Kelly": 0.33, "Cuban": 0.33})
    assert _run(agent._score_categorical(_categorical("Newsom"))) is None


def test_a_market_with_no_distribution_is_uncounted():
    """Same policy as a missing price: unverifiable means uncounted."""
    agent = _categorical_agent(None)
    assert _run(agent._score_categorical(_categorical("Newsom"))) is None


def test_the_resolved_winner_is_recorded_for_audit():
    agent = _categorical_agent({"Newsom": 0.62, "Kelly": 0.21, "Cuban": 0.17})
    pred = _categorical("Kelly")
    _run(agent._score_categorical(pred))
    assert pred.resolved_outcome == "Newsom"


def test_a_categorical_prediction_never_takes_the_price_path():
    """The bug this whole change is about: judging a candidate on up/down."""
    import inspect
    from services.agents.base import SentinelAgent
    src = inspect.getsource(SentinelAgent.resolve_due_predictions)
    assert "_score_categorical" in src and "_score_directional" in src
    assert "pred.outcome_space" in src, "shape of the market never consulted"


# ── the field lives at the event, not the market ─────────────────────────────
#
# Verified live: 23 of 25 open events carry 3+ sibling markets, and all 350 of
# their markets have a groupItemTitle. A "who wins" question is priced as one
# yes/no market per candidate, so flattening events to markets -- which the
# collector did -- turned one race into N unrelated coin flips. That is exactly
# what the stored data showed: three separate markets for the 2028 Democratic
# nomination, each scored on its own.

def _event(*legs):
    """A Gamma event: title plus one binary market per choice."""
    import json
    return {
        "title": "2028 Democratic nomination",
        "slug": "dem-nomination-2028",
        "markets": [
            {
                "groupItemTitle": name,
                "outcomes": json.dumps(["Yes", "No"]),
                "outcomePrices": json.dumps([str(p), str(round(1 - p, 4))]),
            }
            for name, p in legs
        ],
    }


def test_sibling_markets_are_recognised_as_one_field(collector):
    ev = _event(("Newsom", 0.15), ("Kelly", 0.09), ("Cuban", 0.04))
    ctx = collector._choice_context(ev["markets"][0], ev)

    assert ctx["is_multi_choice"] is True
    assert ctx["choice_name"] == "Newsom"
    assert ctx["choice_space"] == ["Newsom", "Kelly", "Cuban"]
    assert ctx["choice_prices"] == {"Newsom": 0.15, "Kelly": 0.09, "Cuban": 0.04}
    assert ctx["event_slug"] == "dem-nomination-2028"


def test_every_leg_sees_the_whole_field(collector):
    """A trade on any candidate must carry the others, or it is a coin flip again."""
    ev = _event(("Newsom", 0.15), ("Kelly", 0.09), ("Cuban", 0.04))
    for leg in ev["markets"]:
        ctx = collector._choice_context(leg, ev)
        assert len(ctx["choice_prices"]) == 3


def test_a_lone_market_is_not_a_field(collector):
    """One market under an event is a plain binary and must not be dressed up."""
    ev = _event(("Newsom", 0.15))
    ctx = collector._choice_context(ev["markets"][0], ev)
    assert ctx["is_multi_choice"] is False
    assert ctx["choice_space"] == []


def test_a_market_with_no_parent_event_is_handled(collector):
    ctx = collector._choice_context({"groupItemTitle": None}, None)
    assert ctx["is_multi_choice"] is False
    assert ctx["event_slug"] == ""


def test_choice_probability_is_the_legs_yes_price(collector):
    """p(Newsom wins) is p(yes) on the Newsom leg, not its NO side."""
    ev = _event(("Newsom", 0.15), ("Kelly", 0.09))
    ctx = collector._choice_context(ev["markets"][0], ev)
    assert ctx["choice_prices"]["Newsom"] == 0.15      # not 0.85


# ── grouping a flat response back into its question ──────────────────────────
#
# /markets?event_slug=X answers with a flat list even when every row is one leg
# of the same question: 20 rows came back for the 2028 Democratic nomination,
# one per candidate, with no event wrapper. The grouping key is each market's
# own nested events[0], which carries the real title.

def _flat_leg(name, p, event_slug="democratic-presidential-nominee-2028"):
    import json
    return {
        "groupItemTitle": name,
        "outcomes": json.dumps(["Yes", "No"]),
        "outcomePrices": json.dumps([str(p), str(round(1 - p, 4))]),
        "events": [{"title": "Democratic Presidential Nominee 2028", "slug": event_slug}],
    }


def test_a_flat_response_is_regrouped_into_one_field(collector):
    data = [_flat_leg("Newsom", 0.15), _flat_leg("Ocasio-Cortez", 0.22),
            _flat_leg("Pritzker", 0.01)]
    pairs = collector._pair_markets_with_events(data, "queried-slug")

    assert len(pairs) == 3
    for market, parent in pairs:
        ctx = collector._choice_context(market, parent)
        assert ctx["is_multi_choice"] is True, "still an isolated coin flip"
        assert ctx["event_title"] == "Democratic Presidential Nominee 2028"
        assert ctx["event_slug"] == "democratic-presidential-nominee-2028"
        assert set(ctx["choice_prices"]) == {"Newsom", "Ocasio-Cortez", "Pritzker"}


def test_unrelated_events_are_not_merged(collector):
    """Grouping on the response instead of the event would fuse two questions."""
    data = [_flat_leg("Newsom", 0.15),
            _flat_leg("Vance", 0.40, event_slug="republican-nominee-2028")]
    pairs = collector._pair_markets_with_events(data, "queried-slug")

    # Each is alone under its own event, so neither is a field -- and, the point
    # of the test, neither can see the other's candidate.
    for market, parent in pairs:
        ctx = collector._choice_context(market, parent)
        assert ctx["is_multi_choice"] is False
        assert ctx["choice_prices"] == {}
    assert {p["slug"] for _, p in pairs} == {
        "democratic-presidential-nominee-2028", "republican-nominee-2028",
    }


def test_a_market_without_event_metadata_falls_back_to_the_queried_slug(collector):
    data = [{"groupItemTitle": "Solo", "outcomes": '["Yes", "No"]'}]
    pairs = collector._pair_markets_with_events(data, "queried-slug")
    _, parent = pairs[0]
    assert parent["slug"] == "queried-slug"


def test_nested_event_responses_still_work(collector):
    """The /events shape must keep working; only the flat path was broken."""
    ev = _event(("Newsom", 0.15), ("Kelly", 0.09))
    pairs = collector._pair_markets_with_events([ev], "queried-slug")
    assert len(pairs) == 2
    assert all(parent is ev for _, parent in pairs)

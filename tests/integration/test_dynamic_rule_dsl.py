"""The rules this platform writes for itself, and whether they survive the trip.

The shipped rules are a seed set. The rule synthesiser is supposed to be where
most of the rule set comes from -- it observes what the platform is actually
emitting and proposes correlations over it -- and every one of those rules is
validated by `services.agents.rule_agent.CorrelationDef` before it is stored.

That validator declared five fields while the correlation engine reads ten, and
Pydantic drops what it is not told about. The synthesiser's own prompt asks the
model, in as many words, to set `"same_entity": true` for a rule about one
company, and shows it in the worked example -- so the model supplied the join,
the validator deleted it, and every synthetic rule reached the engine asserting
that a shared 48-hour window was the relationship. The engine's cross-domain
fallback then joined them on whatever subject it could find, which is a safety
net doing the work the rule was supposed to do.

These tests pin the three things that failure needed:

  * the two vocabularies are identical, so neither can grow without the other
  * a rule carrying joins survives validation, storage and reload intact
  * a synthesised rule, put through the real validator and the real evaluator,
    fires on the situation it describes and not on the one it does not
"""
import json

import pytest

from tests.integration.market_scenarios import (
    Beat,
    Scenario,
    cluster_for,
    load_scenario,
)

pytestmark = pytest.mark.anyio

HOUR = 60.0


# ── The contract between the writer and the reader ──────────────────────────


def test_the_dsl_the_agent_writes_is_the_dsl_the_engine_reads():
    """Neither vocabulary may grow without the other.

    `CorrelationDef` is the only thing standing between a synthesised rule and
    the rule cache. A key the engine reads and the model does not declare is
    silently deleted on the way through; a key the model declares and the
    engine never reads is a promise to an operator that nothing keeps.
    """
    from services.agents.rule_agent import CorrelationDef

    declared = set(CorrelationDef.model_fields)

    # Every key `evaluate_dynamic_rules` and its helpers look up on a clause.
    read_by_engine = {
        "event_types", "hours", "min_anomaly", "tags", "region",
        "same_entity", "shared_tags", "proximity_km",
        "precedes_trigger", "follows_trigger", "within_minutes",
    }

    assert declared == read_by_engine, (
        f"only the agent declares: {sorted(declared - read_by_engine)}\n"
        f"only the engine reads:   {sorted(read_by_engine - declared)}\n"
        "A clause key the engine reads but the model does not declare is "
        "dropped in validation without a word."
    )


def test_the_engine_reads_every_key_the_model_declares():
    """The other direction, checked against the source rather than a list.

    The set above is written by hand, so it can drift from the engine the same
    way the model did. This reads the engine.
    """
    import inspect

    from services.agents.rule_agent import CorrelationDef
    from services.correlation import main
    from shared.models import correlation_rules as dsl

    # Both, because the engine delegates the join predicates to the shared
    # module. Reading only `main` would report every join key as unread the
    # moment that delegation happened, which is the opposite of the truth.
    source = inspect.getsource(main) + inspect.getsource(dsl)
    unread = [
        field for field in CorrelationDef.model_fields
        if f'"{field}"' not in source
    ]
    assert not unread, (
        f"{unread} can be written by the rule agent and are never read by the "
        f"correlation engine. A rule declaring one is making a claim nothing "
        f"enforces."
    )


def test_a_join_survives_validation():
    """The specific deletion. The prompt asks for this key by name."""
    from services.agents.rule_agent import CorrelationDef

    clause = CorrelationDef(**{
        "event_types": ["options_flow", "equity_block"],
        "hours": 24,
        "min_anomaly": 0.5,
        "same_entity": True,
        "precedes_trigger": True,
        "within_minutes": 2880,
    })
    stored = json.loads(json.dumps(clause.model_dump()))
    assert stored["same_entity"] is True
    assert stored["precedes_trigger"] is True
    assert stored["within_minutes"] == 2880


def test_a_misspelled_join_is_refused_rather_than_dropped():
    """`same_entitiy` used to validate cleanly and disarm the rule."""
    from pydantic import ValidationError

    from services.agents.rule_agent import CorrelationDef

    with pytest.raises(ValidationError):
        CorrelationDef(
            event_types=["options_flow"], hours=24, min_anomaly=0.5,
            same_entitiy=True,  # noqa - the typo is the test
        )


def test_a_synthesised_rule_can_trigger_on_several_event_types():
    """Declared as a bare string, it could not.

    Every cyber rule and every sequence rule in the shipped set triggers on a
    list. A synthesiser that can only write single-trigger rules cannot
    reproduce the rules it is being shown as examples.
    """
    from services.agents.rule_agent import DynamicRule

    rule = DynamicRule(
        rule_id="syn_multi", rule_name="Multi-trigger",
        trigger_event_type=["ransomware", "breach_detected"],
        conditions={"min_anomaly": 0.3},
        correlations=[{
            "event_types": ["price_anomaly"], "hours": 24,
            "min_anomaly": 0.3, "shared_tags": True,
        }],
        alert_tier="ELEVATED", tags=["cyber"],
    )
    assert rule.trigger_event_type == ["ransomware", "breach_detected"]


# ── A synthesised rule, end to end ──────────────────────────────────────────


def _synthesised(**overrides) -> dict:
    """A rule shaped exactly as the synthesiser's own prompt example.

    Put through the real validator, so what reaches the evaluator is what
    would reach it in production -- including anything validation removes.
    """
    from services.agents.rule_agent import DynamicRule

    spec = {
        "rule_id": "syn_insider_into_options",
        "rule_name": "Insider Sale Into Options Accumulation",
        "trigger_event_type": ["price_anomaly", "equity_block"],
        "conditions": {"min_anomaly": 0.4},
        "correlations": [{
            "event_types": ["options_flow", "equity_block"],
            "hours": 24,
            "min_anomaly": 0.5,
            "same_entity": True,
            "precedes_trigger": True,
            "within_minutes": 2880,
        }],
        "alert_tier": "ELEVATED",
        "tags": ["equity", "insider"],
    }
    spec.update(overrides)
    rule = DynamicRule(**spec)
    # Through JSON, as Redis stores it and `_listen_for_rule_updates` reads it.
    stored = json.loads(json.dumps(rule.model_dump()))
    stored["expires_at"] = rule.expires_at
    return stored


async def _fire(scenario, rule: dict):
    from services.correlation.main import _dynamic_rules_cache, evaluate_dynamic_rules

    _dynamic_rules_cache.clear()
    _dynamic_rules_cache[rule["rule_id"]] = rule
    store, trigger = await load_scenario(scenario)
    try:
        return await evaluate_dynamic_rules(trigger, store)
    finally:
        _dynamic_rules_cache.clear()


SAME_NAME = Scenario(
    name="Options accumulation in the name that then moved",
    domain="tradfi",
    why="What the synthesiser's own example rule is describing.",
    trigger=Beat("price_anomaly", "ACME", 0, 0.72, source="alpaca",
                 headline="ACME -9% on guidance"),
    evidence=[
        Beat("options_flow", "ACME", 20 * HOUR, 0.61, source="alpaca_options",
             headline="ACME puts, 9x average"),
        Beat("equity_block", "ACME", 14 * HOUR, 0.55, source="finnhub_equities",
             headline="ACME block, seller"),
    ],
    expect_rule="syn_insider_into_options",
)

OTHER_NAME = Scenario(
    name="Options accumulation in a different name entirely",
    domain="tradfi",
    why=(
        "The case `same_entity` exists for. Without it the rule correlates a "
        "move in one company with options activity in another and publishes "
        "the result under the first company's name -- which is what every "
        "synthesised rule did, because validation deleted the key."
    ),
    trigger=Beat("price_anomaly", "ACME", 0, 0.72, source="alpaca",
                 headline="ACME -9% on guidance"),
    evidence=[
        Beat("options_flow", "ZENITH", 20 * HOUR, 0.61, source="alpaca_options",
             headline="ZENITH puts, 9x average"),
        Beat("equity_block", "ZENITH", 14 * HOUR, 0.55, source="finnhub_equities",
             headline="ZENITH block, seller"),
    ],
    expect_rule="",
)

AFTER_THE_MOVE = Scenario(
    name="The same options activity, after the move instead of before",
    domain="tradfi",
    why=(
        "Options flow that follows a price move is a reaction to public news. "
        "The rule claims positioning ahead of one, and `precedes_trigger` is "
        "the only thing that tells the two apart."
    ),
    trigger=Beat("price_anomaly", "ACME", 0, 0.72, source="alpaca",
                 headline="ACME -9% on guidance"),
    evidence=[
        Beat("options_flow", "ACME", -20 * HOUR, 0.61, source="alpaca_options",
             headline="ACME puts, 9x average"),
        Beat("equity_block", "ACME", -14 * HOUR, 0.55, source="finnhub_equities",
             headline="ACME block, seller"),
    ],
    expect_rule="",
)


async def test_a_synthesised_rule_fires_on_what_it_describes():
    clusters = await _fire(SAME_NAME, _synthesised())
    assert cluster_for(clusters, "syn_insider_into_options") is not None, (
        "A rule written exactly as the synthesiser's prompt shows produces "
        "nothing on the situation that prompt describes."
    )


async def test_a_synthesised_rule_holds_its_same_entity_join():
    clusters = await _fire(OTHER_NAME, _synthesised())
    assert cluster_for(clusters, "syn_insider_into_options") is None, (
        "The rule correlated a move in ACME with options activity in ZENITH. "
        "`same_entity` was declared by the model and asked for by the prompt; "
        "if this fires, it did not survive the trip to the engine."
    )


async def test_a_synthesised_rule_holds_its_ordering():
    clusters = await _fire(AFTER_THE_MOVE, _synthesised())
    assert cluster_for(clusters, "syn_insider_into_options") is None, (
        "Evidence that followed the trigger satisfied a clause declaring "
        "`precedes_trigger`."
    )


# ── What the synthesiser is allowed to store ────────────────────────────────


def test_a_cross_domain_rule_with_no_join_is_refused_at_synthesis():
    """The shape that produced 69% of the correlation layer's output.

    The engine has a fallback for it, and the fallback is not the point: a
    stored rule with no join is one whose author never decided what it means,
    and it stays in the rule set asserting co-occurrence under a name that
    promises a relationship.
    """
    from services.agents.rule_agent import DynamicRule, _is_reusable_rule

    rule = DynamicRule(
        rule_id="syn_no_join", rule_name="Cyber Market Convergence",
        trigger_event_type="ransomware",
        conditions={"min_anomaly": 0.3},
        correlations=[{
            "event_types": ["price_anomaly", "options_flow"],
            "hours": 48, "min_anomaly": 0.3,
        }],
        alert_tier="ELEVATED", tags=["cyber"],
    )
    reason = _is_reusable_rule(rule)
    assert reason and "join" in reason, (
        f"A cross-domain rule with no join was accepted (reason={reason!r})."
    )


def test_a_single_domain_rule_with_no_join_is_still_allowed():
    """The requirement is scoped to where the failure is.

    Several options prints on one name are related by the name whether or not
    the clause says so. Rejecting those would be a different rule set, not a
    stricter one.
    """
    from services.agents.rule_agent import DynamicRule, _is_reusable_rule

    rule = DynamicRule(
        rule_id="syn_single_domain", rule_name="Options Print Convergence",
        trigger_event_type="options_flow",
        conditions={"min_anomaly": 0.3},
        correlations=[{
            "event_types": ["dark_pool", "equity_block"],
            "hours": 24, "min_anomaly": 0.3,
        }],
        alert_tier="ALERT", tags=["equity"],
    )
    assert _is_reusable_rule(rule) is None


def test_a_contagion_rule_joins_on_a_bounded_ordering():
    """The one cross-domain case that legitimately has no subject join.

    A liquidation in BTC and a move in COIN are never the same entity and
    share no tag. What connects them is that one followed the other inside the
    session, and a rule that says so has said something.
    """
    from services.agents.rule_agent import DynamicRule, _is_reusable_rule

    rule = DynamicRule(
        rule_id="syn_contagion", rule_name="Liquidation Spillover",
        trigger_event_type="crypto_liquidation",
        conditions={"min_anomaly": 0.3},
        correlations=[{
            "event_types": ["price_anomaly", "equity_block"],
            "hours": 24, "min_anomaly": 0.3,
            "follows_trigger": True, "within_minutes": 240,
        }],
        alert_tier="ELEVATED", tags=["crypto"],
    )
    assert _is_reusable_rule(rule) is None

    # The same rule without the bound is not making that claim: "afterwards,
    # at some point in the next day" is the window it already had.
    unbounded = DynamicRule(
        rule_id="syn_contagion_loose", rule_name="Liquidation Spillover",
        trigger_event_type="crypto_liquidation",
        conditions={"min_anomaly": 0.3},
        correlations=[{
            "event_types": ["price_anomaly", "equity_block"],
            "hours": 24, "min_anomaly": 0.3, "follows_trigger": True,
        }],
        alert_tier="ELEVATED", tags=["crypto"],
    )
    assert _is_reusable_rule(unbounded)


def test_the_prompt_teaches_every_join_the_dsl_accepts():
    """A key the model is never told about is a key it will never use.

    The prompt named exactly one of the six, which is most of why the stored
    rules express only that one -- and it was the one validation deleted.
    """
    import inspect

    from services.agents import rule_agent

    source = inspect.getsource(rule_agent.RuleSynthesizerAgent)
    prompt = source[source.index("=== SYNTHETIC RULE GENERATION ==="):]
    prompt = prompt[: prompt.index('"""', 10)]
    for key in ("same_entity", "region", "shared_tags", "proximity_km",
                "precedes_trigger", "follows_trigger", "within_minutes"):
        assert key in prompt, f"the prompt never mentions {key}"

"""Can each rule fire against the events this platform actually emits?

The scenario suite constructs its own events, which is what lets it test the
evaluator without a running stack -- and is exactly why it cannot answer this
question. A rule naming an event type no collector produces passes every
scenario written for it and fires never.

Twenty of the platform's forty-three declared event types have no producer.
`price_anomaly` is one: price moves arrive as `market_anomaly`. `dark_pool` is
another: dark-pool prints arrive as `equity_block`. Both appear in shipped
rules. They are harmless there because each of those rules names a live type
beside them -- but nothing was checking, and the difference between "names a
dead type among live ones" and "names only dead types" is the difference
between a narrower rule and a rule that has never once fired.

Two properties, and the second is the subtle one:

  * a rule must declare at least one trigger the platform emits
  * every clause must be able to reach the convergence bar from live types
    alone -- and a clause that pins `same_entity` cannot converge on subjects,
    because every hit it can return is the same subject, so it needs the bar
    in distinct *types*
"""
import pytest

from shared.models.events import EventType, POSITION_TELEMETRY_TYPES
from shared.models.correlation_rules import (
    CONVERGENCE_TYPE_TARGET,
    clause_event_types,
    trigger_event_types,
)


@pytest.fixture(scope="module")
def live_types() -> set:
    """Event types something in the tree actually constructs."""
    import sys
    from pathlib import Path

    tests_dir = Path(__file__).resolve().parents[1]
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))
    from test_event_type_producers import _constructed_event_types

    return {
        EventType[name].value
        for name in _constructed_event_types()
        if name in EventType.__members__
    }


@pytest.fixture(scope="module")
def rules():
    from services.correlation.main import SHIPPED_RULES

    return SHIPPED_RULES


def _matchable(clause) -> set:
    """Clause types a result set can actually contain.

    Position telemetry is stripped from every result set before a rule sees it,
    so listing it neither helps a clause match nor counts toward its breadth.
    """
    return set(clause_event_types(clause)) - POSITION_TELEMETRY_TYPES


def test_every_rule_declares_a_trigger_the_platform_emits(rules, live_types):
    dead = []
    for rule in rules:
        declared = set(trigger_event_types(rule))
        if not declared & live_types:
            dead.append(f"{rule['rule_id']}: triggers only on {sorted(declared)}")
    assert not dead, (
        "These rules can never be reached, because nothing constructs any of "
        "the event types they trigger on:\n  " + "\n  ".join(dead)
    )


def test_every_clause_can_reach_the_convergence_bar(rules, live_types):
    """The one that catches a rule narrowed into silence.

    A clause listing several types is an OR and must match
    CONVERGENCE_TYPE_TARGET of them -- or that many distinct subjects, which is
    the same bar reached a different way. `same_entity` closes the second door:
    every hit is the one entity the clause asked for, so the count of distinct
    subjects is one by construction and only types remain.
    """
    unsatisfiable = []
    for rule in rules:
        for i, clause in enumerate(rule.get("correlations", []) or []):
            declared = _matchable(clause)
            live = declared & live_types
            if not live:
                unsatisfiable.append(
                    f"{rule['rule_id']} clause {i}: no live type among "
                    f"{sorted(declared)}"
                )
            elif (
                len(declared) > 1
                and clause.get("same_entity")
                and len(live) < CONVERGENCE_TYPE_TARGET
            ):
                unsatisfiable.append(
                    f"{rule['rule_id']} clause {i}: same_entity pins every hit to "
                    f"one subject, so convergence needs {CONVERGENCE_TYPE_TARGET} "
                    f"live types; it has {sorted(live)} of {sorted(declared)}"
                )
    assert not unsatisfiable, (
        "These clauses cannot reach the convergence bar from types the platform "
        "emits:\n  " + "\n  ".join(unsatisfiable)
    )


def test_a_clause_that_leans_on_subject_convergence_is_recorded(rules, live_types):
    """Which rules depend on *two different subjects* rather than two types.

    Not a defect -- two tankers going dark in one strait is the finding. It is
    a dependency worth naming, because adding `same_entity` to such a clause
    would silence it without changing anything visible, and because it is the
    reason `rule_maritime_chokepoint_evasion` works at all while two of its
    three declared types have no producer.
    """
    leaning = []
    for rule in rules:
        for i, clause in enumerate(rule.get("correlations", []) or []):
            declared = _matchable(clause)
            live = declared & live_types
            if (
                len(declared) > 1
                and not clause.get("same_entity")
                and len(live) < CONVERGENCE_TYPE_TARGET
            ):
                leaning.append(f"{rule['rule_id']}#{i}")

    # Pinned so the list cannot grow silently. Each of these fires only when it
    # finds the bar's worth of distinct subjects.
    assert set(leaning) <= {"rule_maritime_chokepoint_evasion#0"}, (
        f"New clauses depending on subject convergence: {sorted(leaning)}. "
        f"Each needs {CONVERGENCE_TYPE_TARGET} distinct subjects to fire, because "
        f"it cannot reach that many distinct live types."
    )


def test_every_type_a_scenario_uses_is_one_the_platform_emits():
    """The scenarios must describe the platform, not an idealised one.

    A scenario built from types no collector produces tests the evaluator
    against a world that does not exist -- it will pass forever and guarantee
    nothing. Where a scenario needs a type the platform cannot yet emit, that
    is a collector gap and belongs in the list below with a reason, not in a
    green test.
    """
    import sys
    from pathlib import Path

    tests_dir = Path(__file__).resolve().parents[1]
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))
    from test_event_type_producers import _constructed_event_types

    from tests.integration import scenarios as S

    live = {
        EventType[name].value
        for name in _constructed_event_types()
        if name in EventType.__members__
    }

    # Types a scenario legitimately uses that the platform cannot yet emit.
    # Each is a missing collector, not a missing rule: the correlation side is
    # ready and the feed is not.
    AWAITING_A_PRODUCER = {
        "vessel_sts": "no co-location detector; STS zones only score dark gaps",
        "vessel_spoof": "no AIS identity-spoof detector",
        "climate_stress": "no climate feed",
        "supply_chain_metric": "freight rates are enriched onto other events",
        "price_anomaly": "price moves arrive as market_anomaly",
        "dark_pool": "dark-pool prints arrive as equity_block",
    }

    used = set()
    for scenario in S.ALL_SCENARIOS:
        used.add(scenario.trigger.event_type)
        used |= {b.event_type for b in scenario.evidence}

    unaccounted = sorted(used - live - set(AWAITING_A_PRODUCER))
    assert not unaccounted, (
        f"Scenarios use {unaccounted}, which nothing in the platform emits and "
        f"which are not recorded as awaiting a producer. A scenario built on a "
        f"type no collector produces cannot fail."
    )

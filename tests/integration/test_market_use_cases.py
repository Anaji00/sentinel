"""Does this platform see the things it exists to see?

Every scenario in `scenarios.py` is a situation that recurs in real markets and
whose every leg this platform already collects. The rule evaluator, the event
store and the domain resolution that run here are the shipped ones; only Redis
and Postgres are stood in for, and those two are executed rather than mocked.

A failure here is not a broken test. It means a pattern the product claims to
find produces nothing, and the fix is in the platform.
"""
import pytest

from tests.integration import scenarios as S
from tests.integration.market_scenarios import (
    cluster_for,
    describe,
    evidence_types,
    run_scenario,
)

pytestmark = pytest.mark.anyio


def _ids(scen_list):
    return [s.name for s in scen_list]


POSITIVE = [s for s in S.ALL_SCENARIOS if s.expect_rule]


@pytest.mark.parametrize("scenario", POSITIVE, ids=_ids(POSITIVE))
async def test_the_platform_produces_a_finding(scenario):
    """The situation happened. Something must be said about it."""
    clusters, store, trigger = await run_scenario(scenario)

    assert clusters, (
        f"\n{scenario.name}\n"
        f"  domain: {scenario.domain}\n"
        f"  why this matters: {scenario.why}\n"
        f"  the platform said: nothing\n"
        f"  every leg of this scenario is an event type this platform "
        f"collects and scores."
    )


@pytest.mark.parametrize("scenario", POSITIVE, ids=_ids(POSITIVE))
async def test_the_expected_rule_is_the_one_that_fires(scenario):
    """Firing on the wrong rule is a different claim, published under a name."""
    clusters, store, trigger = await run_scenario(scenario)
    hit = cluster_for(clusters, scenario.expect_rule)

    assert hit is not None, (
        f"\n{scenario.name}\n"
        f"  expected: {scenario.expect_rule}\n"
        f"  got:      {describe(clusters)}\n"
        f"  why this matters: {scenario.why}"
    )


@pytest.mark.parametrize("scenario", POSITIVE, ids=_ids(POSITIVE))
async def test_the_finding_surfaces_rather_than_being_buried(scenario):
    """A finding published at WATCH has been found and not delivered.

    The engine publishes `min(declared tier, tier the evidence earns)`, so a
    rule declaring CRITICAL does not guarantee one and should not -- that is
    the demotion working. What must hold is that a textbook instance of a named
    pattern clears the tier at which anyone sees it.
    """
    from services.correlation.main import _TIER_ORDER

    clusters, store, trigger = await run_scenario(scenario)
    hit = cluster_for(clusters, scenario.expect_rule)
    assert hit is not None, f"{scenario.name}: {scenario.expect_rule} did not fire"

    tier = hit.alert_tier.value if hasattr(hit.alert_tier, "value") else str(hit.alert_tier)
    order = [t.value for t in _TIER_ORDER]  # CRITICAL first
    assert order.index(tier) <= order.index(scenario.expect_min_tier), (
        f"\n{scenario.name}\n"
        f"  published at: {tier} (confidence {hit.confidence_score:.2f})\n"
        f"  needs at least: {scenario.expect_min_tier}\n"
        f"  why this matters: {scenario.why}"
    )


async def test_every_rule_can_publish_the_tier_it_declares():
    """A declared tier nothing can reach is a promise the engine cannot keep.

    Measured over the shipped set before this was fixed, four rules could not
    reach their declared tier under *any* evidence: a perfect trigger, every
    declared type matched, each from its own source.

        rule_informed_trading_sequence      CRITICAL      ceiling 0.503
        rule_insider_options_convergence    INTELLIGENCE  ceiling 0.556
        rule_institutional_position_shift   INTELLIGENCE  ceiling 0.534
        rule_maritime_chokepoint_evasion    CRITICAL      ceiling 0.534

    All four are single-domain by design -- one company's filings, options and
    prints are all tradfi; vessels in one strait are all maritime -- and the
    confidence formula reserved a quarter of its scale for spanning domains,
    which those rules cannot do and should not have to. The most valuable
    pattern the platform looks for, an insider sale preceding a price move, was
    capped two tiers below its own declaration by arithmetic.
    """
    from shared.models.events import AlertTier, event_domain
    from services.correlation.main import (
        SHIPPED_RULES, _rule_confidence, _structural_completeness,
        _tier_supported_by,
    )

    class _PerfectTrigger:
        anomaly_score = 1.0
        corroboration = None

    unreachable = []
    for rule in SHIPPED_RULES:
        trigger = rule["trigger_event_type"]
        trigger = trigger if isinstance(trigger, list) else [trigger]
        domains = {event_domain(trigger[0])}
        types = set()
        for clause in rule.get("correlations", []):
            for t in clause.get("event_types", []):
                types.add(t)
                domains.add(event_domain(t))

        # The best case: one event per declared type, each from its own source,
        # every clause satisfied.
        support = [{"source": f"src{i}", "type": t} for i, t in enumerate(sorted(types))]
        clauses = len(rule.get("correlations", []))
        declared = [
            len(set(c.get("event_types") or []))
            for c in rule.get("correlations", [])
            if len(set(c.get("event_types") or [])) > 1
        ]
        conf = _rule_confidence(
            _PerfectTrigger(), support, domains,
            structure=_structural_completeness(clauses, clauses, declared, declared),
        )
        want = AlertTier[str(rule["alert_tier"]).strip().upper()]
        got = _tier_supported_by(conf, want)
        if got != want:
            unreachable.append(f"{rule['rule_id']}: declares {want.value}, "
                               f"ceiling {conf:.3f} = {got.value}")

    assert not unreachable, (
        "These rules declare a tier their best possible evidence cannot earn:\n  "
        + "\n  ".join(unreachable)
    )


@pytest.mark.parametrize(
    "scenario",
    [s for s in POSITIVE if s.expect_evidence_types],
    ids=_ids([s for s in POSITIVE if s.expect_evidence_types]),
)
async def test_the_finding_rests_on_the_right_evidence(scenario):
    """A rule that fires on the wrong leg is right by accident.

    The sequence rules are the clear case: `rule_informed_trading_sequence`
    firing without the insider leg is not a weaker version of informed
    trading, it is a different and unsupported claim.
    """
    clusters, store, trigger = await run_scenario(scenario)
    hit = cluster_for(clusters, scenario.expect_rule)
    assert hit is not None, f"{scenario.name}: {scenario.expect_rule} did not fire"

    found = evidence_types(store, hit)
    missing = [t for t in scenario.expect_evidence_types if t not in found]
    assert not missing, (
        f"\n{scenario.name}\n"
        f"  fired on: {sorted(found) or 'no resolvable evidence'}\n"
        f"  missing:  {missing}\n"
        f"  why this matters: {scenario.why}"
    )


@pytest.mark.parametrize(
    "scenario",
    [s for s in POSITIVE if s.expect_min_domains > 1],
    ids=_ids([s for s in POSITIVE if s.expect_min_domains > 1]),
)
async def test_a_cross_domain_claim_actually_spans_domains(scenario):
    """The platform's headline claim, measured rather than asserted.

    `domain_count` is what the frontend and the executive brief report as the
    cross-domain rate. An undercount here is a rule claiming less than it
    found; an overcount is worse.
    """
    clusters, store, trigger = await run_scenario(scenario)
    hit = cluster_for(clusters, scenario.expect_rule)
    assert hit is not None, f"{scenario.name}: {scenario.expect_rule} did not fire"

    counted = hit.metrics_summary.get("domain_count")
    assert counted and counted >= scenario.expect_min_domains, (
        f"\n{scenario.name}\n"
        f"  domains spanned: {hit.metrics_summary.get('domains')} "
        f"(counted {counted})\n"
        f"  expected at least {scenario.expect_min_domains}\n"
        f"  This is reported as the platform's cross-domain rate."
    )


# ── The negative cases ──────────────────────────────────────────────────────
#
# A platform that fires on everything has found nothing. These assert the
# absence that makes the presences above mean something.


NEGATIVE = [s for s in S.ALL_SCENARIOS if not s.expect_rule]


@pytest.mark.parametrize("scenario", NEGATIVE, ids=_ids(NEGATIVE))
async def test_unrelated_events_in_one_window_produce_nothing(scenario):
    clusters, store, trigger = await run_scenario(scenario)
    assert not clusters, (
        f"\n{scenario.name}\n"
        f"  the platform said: {describe(clusters)}\n"
        f"  why this is wrong: {scenario.why}"
    )


async def test_the_informed_trading_sequence_requires_its_order():
    """Reverse the sequence and the claim evaporates. It must stop firing.

    The rule's entire value over the co-occurrence rules next to it is the
    ordering: insider selling *before* the move. Options and insider activity
    that follow a price move are a reaction to public information, which is
    the opposite finding.
    """
    import dataclasses

    reversed_scenario = dataclasses.replace(
        S.INFORMED_TRADING,
        name="The same three events, after the move instead of before",
        evidence=[
            dataclasses.replace(b, minutes_before=-b.minutes_before)
            for b in S.INFORMED_TRADING.evidence
        ],
    )
    clusters, store, trigger = await run_scenario(reversed_scenario)

    assert cluster_for(clusters, "rule_informed_trading_sequence") is None, (
        "Insider selling and options flow that FOLLOW a price move are a "
        "reaction to public news. The sequence rule fired on them anyway, so "
        "its ordering constraint is not being applied and it is indistinguishable "
        "from the co-occurrence rules beside it."
    )


async def test_every_domain_the_platform_claims_is_covered_by_a_scenario():
    """The coverage check that found three missing rules.

    The platform's own `Domain` enum is the contract. A domain with no
    scenario triggering from it is a domain nobody has asked the question
    about.
    """
    from shared.models.events import CROSS_DOMAIN_MEMBERS, RETIRED_DOMAINS

    claimed = set(CROSS_DOMAIN_MEMBERS) - RETIRED_DOMAINS
    missing = sorted(claimed - S.COVERED_DOMAINS)
    assert not missing, (
        f"No use case triggers from: {missing}. Every domain in the enum is "
        f"collected, scored and stored; one with no scenario is one nobody has "
        f"checked can produce a finding."
    )

    # And the retirement has to be real in both directions. A retired domain
    # keeps its situations -- that is how the cost of retiring it stays
    # visible -- but none of them may expect a finding, or the domain is not
    # retired, it is merely unmonitored.
    for domain in sorted(RETIRED_DOMAINS):
        assert domain in S.DOMAINS_WITH_SCENARIOS, (
            f"{domain} was retired and its situations deleted with it. Keep "
            f"them asserting silence, so what was given up is still written "
            f"down and the acceptance criteria survive if it returns."
        )
        assert domain not in S.COVERED_DOMAINS, (
            f"{domain} is listed as retired and still has a scenario expecting "
            f"a rule to fire. One of the two is wrong."
        )

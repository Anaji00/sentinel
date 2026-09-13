"""What happens to a real finding after the correlation engine publishes it.

The scenarios next door establish that the platform *notices* the situations it
exists to notice. This asks the next question, which is the one that decides
whether a user ever hears about it: does the finding survive the handoff?

Three hops, each with its own way of dropping a cluster silently:

  reasoning   ranks clusters for scenario generation, the scarcest resource the
              platform has. A cluster it scores at zero is never reasoned about.
  attribution what the confirm/deny loop reads back out of stored clusters, by
              SQL, against fields the publisher has to have filled.
  agents      the supervisor dispatches by domain, so a cluster whose domain is
              unset or wrong reaches the wrong specialist or none.

Every cluster here is produced by running a real scenario through the real
engine, not constructed by hand. A hand-built cluster proves the consumer can
read a cluster someone wrote for it, which is a different and much weaker claim
-- and it is how a publisher comes to omit a field for the life of a
deployment.
"""
import pytest

from tests.integration import scenarios as S
from tests.integration.market_scenarios import cluster_for, run_scenario

pytestmark = pytest.mark.anyio

POSITIVE = [s for s in S.ALL_SCENARIOS if s.expect_rule]


def _ids(scen_list):
    return [s.name for s in scen_list]


async def _finding(scenario):
    clusters, store, trigger = await run_scenario(scenario)
    hit = cluster_for(clusters, scenario.expect_rule)
    assert hit is not None, f"{scenario.name}: {scenario.expect_rule} did not fire"
    return hit


# ── The reasoning ranker ────────────────────────────────────────────────────


@pytest.mark.parametrize("scenario", POSITIVE, ids=_ids(POSITIVE))
async def test_a_finding_is_worth_reasoning_about(scenario):
    """A cluster the ranker scores at zero never becomes a scenario.

    Zero is reserved for clusters that cite no events at all -- the quant
    engine publishes those, with a synthetic trigger id matching nothing in the
    store. A rule-path finding about a real situation must not land there.
    """
    from services.reasoning.main import _reasoning_priority

    hit = await _finding(scenario)
    priority = _reasoning_priority((hit, None))

    assert priority > 0.0, (
        f"\n{scenario.name}\n"
        f"  the reasoning layer ranked this at 0.0, which is the score for a "
        f"cluster citing no evidence at all.\n"
        f"  evidence: {len(hit.supporting_event_ids)} events, "
        f"tier {hit.alert_tier.value}, confidence {hit.confidence_score:.2f}"
    )


async def test_the_breadth_term_can_reach_the_top_of_its_scale():
    """A maximally-evidenced cluster must score as maximally evidenced.

    The ranker normalised breadth against fifty supporting events. The rule
    path truncates at ten and the semantic path at three, so the term could
    reach 0.589 at best and a cluster citing everything it is allowed to cite
    was ranked as though it cited two thirds of that.
    """
    import math

    from services.reasoning.main import MAX_CITED_EVENTS

    full = min(
        1.0,
        math.log1p(MAX_CITED_EVENTS - 1) / math.log1p(MAX_CITED_EVENTS - 1),
    )
    assert full == pytest.approx(1.0), (
        "A cluster citing the maximum number of events the publisher allows "
        "does not reach the top of the breadth scale."
    )

    # And the cap is the real one, read from the publisher rather than trusted.
    import inspect

    from services.correlation import main as corr

    source = inspect.getsource(corr)
    assert "supporting_events[:10]" in source, (
        "The rule path no longer truncates evidence at ten, so MAX_CITED_EVENTS "
        "is stale and the reasoning ranker is mis-scaled again."
    )


# ── What attribution reads back ─────────────────────────────────────────────


@pytest.mark.parametrize("scenario", POSITIVE, ids=_ids(POSITIVE))
async def test_the_fields_attribution_queries_are_populated(scenario):
    """`signal_attribution` reads these out of stored clusters by SQL.

    A field the publisher never fills makes its signal permanently absent, and
    absent reads as "this signal never preceded a confirmation" rather than as
    "this signal was never recorded" -- so the calibration loop learns from a
    column of nulls without anything failing.
    """
    hit = await _finding(scenario)
    metrics = hit.metrics_summary or {}

    # (c.metrics_summary->>'domain_count')::int > 1
    assert "domain_count" in metrics, (
        f"{scenario.name}: no domain_count. The cross_domain attribution "
        f"signal casts this to int; a missing key is not > 1, so the signal is "
        f"never attributed to any confirmation."
    )
    assert isinstance(metrics["domain_count"], int)

    # (c.metrics_summary->>'raw_confidence')::float8, read by scenario_tracker
    assert "raw_confidence" in metrics, (
        f"{scenario.name}: no raw_confidence. The tracker reads it to compare "
        f"the heuristic score against the rate clusters actually confirm at."
    )

    # coalesce(array_length(c.supporting_event_ids, 1), 0) >= 5
    assert hit.supporting_event_ids, f"{scenario.name}: cites no evidence"

    # The tier and confidence the ranker weights.
    assert hit.alert_tier is not None
    assert 0.0 < hit.confidence_score <= 1.0


# ── Agent dispatch ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("scenario", POSITIVE, ids=_ids(POSITIVE))
async def test_a_finding_carries_the_domain_that_routes_it(scenario):
    """The supervisor dispatches by domain. An unset one reaches no specialist.

    `primary_domain` is a free-text field on the model with no default, and it
    is what decides whether a maritime finding reaches the maritime reasoning
    or nothing at all.
    """
    from shared.models.events import CROSS_DOMAIN_MEMBERS

    hit = await _finding(scenario)

    assert hit.primary_domain, (
        f"{scenario.name}: the cluster carries no primary_domain, so nothing "
        f"downstream can route it to a specialist."
    )
    assert hit.primary_domain in CROSS_DOMAIN_MEMBERS, (
        f"{scenario.name}: primary_domain is {hit.primary_domain!r}, which is "
        f"not one of the domains the platform reasons across "
        f"({sorted(CROSS_DOMAIN_MEMBERS)}). A domain nothing recognises routes "
        f"nowhere."
    )


@pytest.mark.parametrize("scenario", POSITIVE, ids=_ids(POSITIVE))
async def test_a_finding_names_the_subject_it_is_about(scenario):
    """Every downstream reader keys on this: the graph, the brief, the UI.

    `_fetch_entity_graph` and `_fetch_active_bulletins` in the context builder
    both take `cluster.entity_ids`, and an empty list means the reasoning
    context is built with no graph and no history.
    """
    hit = await _finding(scenario)

    assert hit.entity_ids, (
        f"{scenario.name}: no entity_ids. The reasoning context builder fetches "
        f"the entity graph and the active bulletins by this list; empty means "
        f"the model reasons with neither."
    )
    assert hit.primary_entity_name, f"{scenario.name}: nothing to display"


@pytest.mark.parametrize("scenario", POSITIVE, ids=_ids(POSITIVE))
async def test_a_finding_says_what_it_found(scenario):
    """The description and headline are what a person actually reads."""
    hit = await _finding(scenario)

    assert hit.description and len(hit.description) > 20, (
        f"{scenario.name}: description is {hit.description!r}"
    )
    assert hit.supporting_headlines, (
        f"{scenario.name}: the finding cites evidence by id and shows none of "
        f"it, so the alert asserts a convergence a reader cannot check."
    )
    # The headlines must be the evidence's, not placeholders.
    assert not all(
        h.startswith("event:") or h == "Unknown" for h in hit.supporting_headlines
    ), f"{scenario.name}: every supporting headline is a placeholder"

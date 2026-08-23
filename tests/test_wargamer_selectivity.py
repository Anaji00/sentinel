"""Keeps the most expensive agent in the swarm pointed at things worth its cost.

A wargame is four model calls -- three persona turns plus an arbitration -- so
at swarm capacity it needs roughly four budget slots, about forty minutes. It
was running on every inbound message: each one paid a Neo4j subgraph query, a
Redis cross-agent fetch and three shed persona attempts, then logged
"WARGAME SKIPPED" and discarded the work. The consumer moved at 384 messages an
hour against 157,000 of backlog.

Both gates are cheap and ordered cheapest first: significance, then capacity.
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.adversarial_wargamer import (  # noqa: E402
    _MIN_CONFIDENCE_TO_SIMULATE,
    _SIMULATION_WORTHY_TIERS,
    _is_worth_simulating,
)


@pytest.mark.parametrize("tier", sorted(_SIMULATION_WORTHY_TIERS))
def test_serious_tiers_are_simulated(tier):
    assert _is_worth_simulating({"alert_tier": tier}) is True


@pytest.mark.parametrize("tier", ["WATCH", "ALERT"])
def test_routine_tiers_are_not(tier):
    """These are the bulk of the stream; simulating them starves the rest."""
    assert _is_worth_simulating({"alert_tier": tier}) is False


def test_tier_matching_is_case_insensitive():
    assert _is_worth_simulating({"alert_tier": "critical"}) is True


def test_confidence_is_used_when_no_tier_is_present():
    """Briefs, scenarios and news carry severity under different names."""
    assert _is_worth_simulating({"confidence_score": 0.95}) is True
    assert _is_worth_simulating({"confidence_score": 0.20}) is False
    assert _is_worth_simulating({"anomaly_score": 0.99}) is True


def test_integer_severity_scales_are_understood():
    assert _is_worth_simulating({"severity": 5}) is True
    assert _is_worth_simulating({"severity": 2}) is False


def test_routine_telemetry_without_a_tier_is_still_excluded():
    """Position fixes arrive in the tens of thousands per hour and describe no
    situation to simulate."""
    assert _is_worth_simulating({"type": "vessel_position"}) is False
    assert _is_worth_simulating({"type": "flight_anomaly", "primary_domain": "aviation"}) is False
    assert _is_worth_simulating({"primary_domain": "maritime"}) is False


def test_a_described_situation_without_a_tier_is_simulated():
    """My first gate demanded an explicit severity field and rejected this.

    A news headline about export controls on a named company carries no tier and
    is precisely what an adversarial simulation is for -- an existing test caught
    the over-restriction.
    """
    assert _is_worth_simulating({
        "primary_entity_id": "NVDA",
        "headline": "Semiconductor export controls",
    }) is True


def test_an_unparseable_severity_falls_back_to_the_telemetry_test():
    """The policy is "exclude routine telemetry", not "require a severity".

    A malformed score is treated as no score, so the message is judged on
    whether it is positional telemetry -- which is the check that actually
    protects the expensive path.
    """
    assert _is_worth_simulating({"confidence_score": "very high"}) is True
    assert _is_worth_simulating({"confidence_score": "very high",
                                 "type": "vessel_position"}) is False


def test_the_threshold_is_meaningfully_selective():
    assert 0.5 < _MIN_CONFIDENCE_TO_SIMULATE <= 0.95


def test_both_gates_run_before_any_expensive_context_fetch():
    """Ordering is the whole point: the cost was in the setup, not the model."""
    src = (ROOT / "services/agents/adversarial_wargamer.py").read_text(encoding="utf-8")
    worth = src.index("_is_worth_simulating(message)")
    budget = src.index("_inference_budget.is_available()")
    subgraph = src.index("_fetch_subgraph_context(entity_ids)")
    cross = src.index("get_cross_agent_context(")
    assert worth < budget < subgraph, "context is fetched before the gates decide"
    assert budget < cross, "cross-agent context is fetched before the gates decide"

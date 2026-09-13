"""The platform's floor, and the agent's freedom, held apart.

Rules are meant to be dynamic here, and they should be: the synthesiser watches
what the platform is actually emitting and proposes correlations over it, which
is a capability worth more than any fixed list. The shipped rules are not a
replacement for that. They are the floor -- what a cold-started platform must be
able to see on day one, before the agent has observed anything.

Both live in one Redis hash, and nothing distinguished them. Two consequences,
and the first is the one that matters:

  * `_evaluate_and_prune_rules` handed the whole hash to a model asked to
    "identify any rules that are obsolete, contradictory, or duplicate" and
    deleted every id it named. One prune pass could remove the only rule that
    lets this platform see a tanker go dark in a chokepoint, and nothing would
    restore it until the correlation service next restarted.

  * `_enforce_rule_ceiling` counted both against a budget named
    MAX_ACTIVE_SYNTHETIC_RULES. Seventeen shipped rules turned a budget of forty
    into a budget of twenty-three -- so every rule added to the build quietly
    cost the synthesiser one of its own, and nothing said so.

The second is the subtler intelligence loss: growing the floor was silently
shrinking the ceiling.
"""
import json

import pytest

from shared.models.correlation_rules import SEED_RULE_KEY, is_seed_rule

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _Raw:
    """The three Redis commands the two prune paths use."""

    def __init__(self, rules):
        self.store = {rid: json.dumps(r) for rid, r in rules.items()}
        self.published = []

    async def hgetall(self, key):
        return dict(self.store)

    async def hset(self, key, rid, payload):
        self.store[rid] = payload

    async def hdel(self, key, rid):
        self.store.pop(rid, None)

    async def publish(self, channel, payload):
        self.published.append(json.loads(payload))


class _Redis:
    def __init__(self, rules):
        self.raw = _Raw(rules)


def _seed(rule_id, expires=2_000_000_000):
    return {"rule_id": rule_id, "rule_name": rule_id, "expires_at": expires,
            SEED_RULE_KEY: True, "trigger_event_type": ["vessel_dark"],
            "correlations": []}


def _synthetic(rule_id, expires=1_000_000_000):
    return {"rule_id": rule_id, "rule_name": rule_id, "expires_at": expires,
            "trigger_event_type": "options_flow", "correlations": []}


def _agent(rules):
    from services.agents.rule_agent import RuleSynthesizerAgent

    agent = RuleSynthesizerAgent.__new__(RuleSynthesizerAgent)
    agent.redis = _Redis(rules)
    import logging
    agent.logger = logging.getLogger("test.rule_agent")
    return agent


# ── The build ships a floor ─────────────────────────────────────────────────


def test_every_shipped_rule_is_marked_as_a_seed():
    """Marked in one place, so a rule added to the list cannot be forgotten."""
    from services.correlation.main import SHIPPED_RULES

    unmarked = [r["rule_id"] for r in SHIPPED_RULES if not is_seed_rule(r)]
    assert not unmarked, (
        f"{unmarked} ship with the build and are not marked as seed rules, so "
        f"the synthesiser may retire them."
    )


def test_a_synthesised_rule_is_not_a_seed_rule():
    from services.agents.rule_agent import DynamicRule

    rule = DynamicRule(
        rule_id="syn_1", rule_name="Something The Agent Noticed",
        trigger_event_type="options_flow", conditions={"min_anomaly": 0.3},
        correlations=[{"event_types": ["equity_block"], "hours": 24,
                       "min_anomaly": 0.3, "same_entity": True}],
        alert_tier="ALERT", tags=["equity"],
    )
    assert not is_seed_rule(rule.model_dump())


# ── The ceiling ─────────────────────────────────────────────────────────────


async def test_the_ceiling_counts_only_what_the_agent_wrote():
    """Growing the floor must not shrink the agent's headroom.

    With the shipped set counted, adding six rules to the build took six slots
    from the synthesiser -- a direct reduction in the platform's capacity to
    discover anything new, caused by improving what it already knows.
    """
    from services.agents.rule_agent import MAX_ACTIVE_SYNTHETIC_RULES

    rules = {f"seed_{i}": _seed(f"seed_{i}") for i in range(20)}
    rules.update({
        f"syn_{i}": _synthetic(f"syn_{i}", expires=1_000_000_000 + i)
        for i in range(MAX_ACTIVE_SYNTHETIC_RULES)
    })

    agent = _agent(rules)
    await agent._enforce_rule_ceiling()

    assert len(agent.redis.raw.store) == 20 + MAX_ACTIVE_SYNTHETIC_RULES, (
        "the ceiling retired rules while the agent was exactly at its budget, "
        "because it was counting the twenty seed rules against it"
    )


async def test_the_ceiling_still_retires_the_agents_oldest():
    """The bound it exists for is unchanged."""
    from services.agents.rule_agent import MAX_ACTIVE_SYNTHETIC_RULES

    over = 5
    rules = {f"seed_{i}": _seed(f"seed_{i}") for i in range(17)}
    rules.update({
        f"syn_{i:03d}": _synthetic(f"syn_{i:03d}", expires=1_000_000_000 + i)
        for i in range(MAX_ACTIVE_SYNTHETIC_RULES + over)
    })

    agent = _agent(rules)
    await agent._enforce_rule_ceiling()

    survivors = set(agent.redis.raw.store)
    assert len([r for r in survivors if r.startswith("syn_")]) == MAX_ACTIVE_SYNTHETIC_RULES
    assert len([r for r in survivors if r.startswith("seed_")]) == 17
    # Oldest first, by the expiry it carries.
    assert "syn_000" not in survivors
    assert f"syn_{MAX_ACTIVE_SYNTHETIC_RULES + over - 1:03d}" in survivors


async def test_a_seed_rule_is_never_retired_by_the_ceiling():
    """Even when the agent has written nothing and the hash is all seed."""
    from services.agents.rule_agent import MAX_ACTIVE_SYNTHETIC_RULES

    rules = {f"seed_{i}": _seed(f"seed_{i}")
             for i in range(MAX_ACTIVE_SYNTHETIC_RULES + 25)}
    agent = _agent(rules)
    await agent._enforce_rule_ceiling()
    assert len(agent.redis.raw.store) == MAX_ACTIVE_SYNTHETIC_RULES + 25
    assert not agent.redis.raw.published


# ── The LLM prune pass ──────────────────────────────────────────────────────


async def test_the_model_is_never_shown_a_seed_rule_to_prune():
    """The candidate list is what bounds what can be deleted.

    Filtering the model's answer afterwards would be the weaker fix: the model
    would still be reasoning about rules it cannot retire, and spending its
    attention arguing against the platform's own floor.
    """
    shown = {}

    async def _capture(active_rules, current_context):
        shown.update(active_rules)

    rules = {
        "rule_maritime_chokepoint_evasion": _seed("rule_maritime_chokepoint_evasion"),
        "rule_informed_trading_sequence": _seed("rule_informed_trading_sequence"),
        "syn_something": _synthetic("syn_something"),
    }
    agent = _agent(rules)
    agent._evaluate_and_prune_rules = _capture
    agent.PRUNE_COOLDOWN_SEC = 0

    async def _not_recently(*_a, **_kw):
        return False

    async def _mark(*_a, **_kw):
        return None

    agent.is_recently_processed = _not_recently
    agent.mark_processed = _mark

    await agent._maybe_prune_rules("quiet market")

    assert set(shown) == {"syn_something"}, (
        f"the prune pass offered {sorted(shown)} to the model. A seed rule in "
        f"that list is one the model can delete, and losing "
        f"rule_maritime_chokepoint_evasion costs the platform the maritime "
        f"domain until the correlation service next restarts."
    )


async def test_a_prune_pass_with_nothing_of_its_own_does_not_run():
    """No candidates is not the same as no rules, and it must not cost an inference.

    The prune engine costs a full inference on a host that manages about
    thirty-five an hour. Running one to be told there is nothing to retire is
    the most expensive way to do nothing.
    """
    called = []

    async def _capture(active_rules, current_context):
        called.append(active_rules)

    agent = _agent({"rule_cpi": _seed("rule_cpi"), "rule_nfp": _seed("rule_nfp")})
    agent._evaluate_and_prune_rules = _capture
    agent.PRUNE_COOLDOWN_SEC = 0

    async def _not_recently(*_a, **_kw):
        return False

    async def _mark(*_a, **_kw):
        return None

    agent.is_recently_processed = _not_recently
    agent.mark_processed = _mark

    await agent._maybe_prune_rules("quiet market")
    assert not called


# ── The floor is still restored ─────────────────────────────────────────────


async def test_a_missing_seed_rule_is_written_back_at_startup():
    """The recovery path, which is now the backstop rather than the only defence.

    Reconciliation runs when the correlation service starts. Before this, that
    was the *only* thing standing between an LLM prune and a permanently
    missing domain -- and a service can run for weeks.
    """
    from services.correlation.main import SHIPPED_RULES, _dynamic_rules_cache
    from services.correlation import main as corr

    missing = "rule_maritime_chokepoint_evasion"
    assert any(r["rule_id"] == missing for r in SHIPPED_RULES)

    redis = _Redis({})
    _dynamic_rules_cache.clear()
    try:
        await corr._reconcile_shipped_rules(redis)
        assert missing in redis.raw.store
        restored = json.loads(redis.raw.store[missing])
        assert is_seed_rule(restored)
    finally:
        _dynamic_rules_cache.clear()

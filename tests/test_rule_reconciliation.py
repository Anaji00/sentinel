"""A rule definition in code must be able to reach production.

Correlation rules live in Redis and are hot-reloadable, and the seed ran only
when Redis held none at all. Once seeded, the shipped definitions were never
compared against again -- so a rule change in code was permanently inert.

Adding "same_entity" to three financial rules was a silent no-op for exactly
that reason: the deployed code contained the flag, the running system kept the
definitions it had been seeded with, and six new clusters after the deploy were
still correlating across unrelated tickers.

Reconciliation is version-gated rather than unconditional, because these rules
are meant to be edited at runtime and overwriting an operator's change on every
restart would be worse than the problem it fixes.
"""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "correlation_main", ROOT / "services" / "correlation" / "main.py"
)


def _mod():
    if "correlation_main" in sys.modules:
        return sys.modules["correlation_main"]
    m = importlib.util.module_from_spec(_spec)
    sys.modules["correlation_main"] = m
    _spec.loader.exec_module(m)
    return m


class _Raw:
    def __init__(self):
        self.written = {}

    async def hset(self, key, field, value):
        self.written[field] = json.loads(value)


class _Redis:
    def __init__(self):
        self.raw = _Raw()


def test_the_shipped_rules_are_reachable_outside_the_seed_branch():
    """They were declared inside it, which is why nothing could compare
    against them once seeding had happened."""
    m = _mod()
    assert len(m.SHIPPED_RULES) >= 5
    assert m._shipped_rules_cache


def test_every_shipped_rule_carries_a_version():
    m = _mod()
    for rule in m.SHIPPED_RULES:
        assert int(rule.get("definition_version", 0)) > 0, rule["rule_id"]


def test_a_stale_stored_rule_is_replaced():
    m = _mod()
    rule_id = "rule_financial_block_volume_spike"
    m._dynamic_rules_cache.clear()
    m._dynamic_rules_cache[rule_id] = {"rule_id": rule_id, "definition_version": 1}
    redis = _Redis()
    asyncio.run(m._reconcile_shipped_rules(redis))
    assert rule_id in redis.raw.written
    assert any("same_entity" in c for c in redis.raw.written[rule_id]["correlations"])


def test_a_rule_with_no_version_is_treated_as_older():
    """Every rule seeded before this mechanism existed."""
    m = _mod()
    rule_id = "rule_options_darkpool_surge"
    m._dynamic_rules_cache.clear()
    m._dynamic_rules_cache[rule_id] = {"rule_id": rule_id}
    redis = _Redis()
    asyncio.run(m._reconcile_shipped_rules(redis))
    assert rule_id in redis.raw.written


def test_a_current_rule_is_left_alone():
    m = _mod()
    rule_id = "rule_financial_block_volume_spike"
    m._dynamic_rules_cache.clear()
    m._dynamic_rules_cache[rule_id] = {
        "rule_id": rule_id, "definition_version": m.RULE_DEFINITION_VERSION,
    }
    redis = _Redis()
    asyncio.run(m._reconcile_shipped_rules(redis))
    assert redis.raw.written == {}


def test_an_operator_edit_at_a_higher_version_survives():
    """Pinning a rule ahead of the shipped version keeps it."""
    m = _mod()
    rule_id = "rule_financial_block_volume_spike"
    m._dynamic_rules_cache.clear()
    m._dynamic_rules_cache[rule_id] = {
        "rule_id": rule_id, "definition_version": m.RULE_DEFINITION_VERSION + 5,
    }
    redis = _Redis()
    asyncio.run(m._reconcile_shipped_rules(redis))
    assert redis.raw.written == {}


def test_a_runtime_synthesised_rule_is_never_touched():
    """The rule synthesiser writes rules this build never shipped."""
    m = _mod()
    m._dynamic_rules_cache.clear()
    m._dynamic_rules_cache["rule_invented_by_the_agent"] = {"rule_id": "rule_invented_by_the_agent"}
    redis = _Redis()
    asyncio.run(m._reconcile_shipped_rules(redis))
    assert redis.raw.written == {}


def test_a_redis_failure_does_not_stop_the_other_rules():
    m = _mod()

    class _Failing(_Raw):
        async def hset(self, key, field, value):
            if field == "rule_financial_block_volume_spike":
                raise RuntimeError("connection reset")
            await super().hset(key, field, value)

    redis = _Redis()
    redis.raw = _Failing()
    m._dynamic_rules_cache.clear()
    for rid in ("rule_financial_block_volume_spike", "rule_options_darkpool_surge"):
        m._dynamic_rules_cache[rid] = {"rule_id": rid, "definition_version": 1}
    asyncio.run(m._reconcile_shipped_rules(redis))
    assert "rule_options_darkpool_surge" in redis.raw.written


def test_reconciliation_runs_when_rules_already_exist():
    """The branch that previously did nothing but load."""
    source = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    assert "await _reconcile_shipped_rules(redis_client)" in source

"""
tests/test_rule_deprecation_holds.py

A deprecation that only deletes from the store does not hold.

The rule synthesiser retires a rule by deleting it from
`sentinel:correlation:dynamic_rules`. Measured on 1 October, twenty minutes
after a deploy:

  * rule_maritime_chokepoint_evasion was deprecated at 33 negative of 34
    verdicts -- and the correlation engine's reconciliation reinstalls any
    shipped rule absent from the store on every restart, so the verdict would
    have been undone by the next deploy.
  * SEMANTIC_001 was "deprecated" nine times at 28 negative of 29, and fired 52
    times in the same window: it is emitted by code, not read from the store,
    so the deletion was a no-op that logged as a success.

Both now read the verdict record itself, which is the one place the decision
lives regardless of where a rule comes from.
"""

import ast
import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.rule_feedback import deprecated_by_evidence


def _redis(records):
    """A fake whose hgetall answers from {rule_id: {field: count}}."""
    r = MagicMock()

    async def hgetall(key):
        rule_id = key.rsplit(":", 1)[1]
        return {k.encode(): str(v).encode() for k, v in records.get(rule_id, {}).items()}

    r.raw.hgetall = AsyncMock(side_effect=hgetall)
    r.raw.hset = AsyncMock()
    return r


def test_a_rule_denied_33_of_34_is_retired():
    r = _redis({"rule_x": {"total": 34, "negative": 33}})
    assert asyncio.run(deprecated_by_evidence(r, "rule_x")) is True


def test_a_rule_with_a_middling_record_is_kept():
    """37% confirmed is the second-best financial rule; it must survive."""
    r = _redis({"rule_x": {"total": 77, "negative": 48}})
    assert asyncio.run(deprecated_by_evidence(r, "rule_x")) is False


def test_too_few_verdicts_retire_nothing():
    r = _redis({"rule_x": {"total": 5, "negative": 5}})
    assert asyncio.run(deprecated_by_evidence(r, "rule_x")) is False


def test_an_unjudged_rule_is_not_retired():
    assert asyncio.run(deprecated_by_evidence(_redis({}), "rule_x")) is False


def _correlation_main():
    import services.correlation.main as m
    return m


def test_reconciliation_does_not_reinstall_a_retired_rule():
    m = _correlation_main()
    shipped = {"rule_id": "rule_retired", "definition_version": 1}
    fresh = {"rule_id": "rule_fresh", "definition_version": 1}
    r = _redis({"rule_retired": {"total": 34, "negative": 33}})
    saved_shipped, saved_dynamic = dict(m._shipped_rules_cache), dict(m._dynamic_rules_cache)
    try:
        m._shipped_rules_cache.clear()
        m._shipped_rules_cache.update({"rule_retired": shipped, "rule_fresh": fresh})
        m._dynamic_rules_cache.clear()
        asyncio.run(m._reconcile_shipped_rules(r))
        installed = [c.args[1] for c in r.raw.hset.await_args_list]
        assert installed == ["rule_fresh"]
        assert "rule_retired" not in m._dynamic_rules_cache
    finally:
        m._shipped_rules_cache.clear()
        m._shipped_rules_cache.update(saved_shipped)
        m._dynamic_rules_cache.clear()
        m._dynamic_rules_cache.update(saved_dynamic)


def test_the_semantic_path_reads_its_own_record():
    m = _correlation_main()
    m._semantic_deprecation.update({"checked_at": 0.0, "deprecated": False})
    r = _redis({"SEMANTIC_001": {"total": 29, "negative": 28}})
    assert asyncio.run(m._semantic_rule_deprecated(r)) is True


def test_the_semantic_check_is_cached():
    m = _correlation_main()
    m._semantic_deprecation.update({"checked_at": 0.0, "deprecated": False})
    r = _redis({})
    asyncio.run(m._semantic_rule_deprecated(r))
    asyncio.run(m._semantic_rule_deprecated(r))
    assert r.raw.hgetall.await_count == 1


def test_the_semantic_check_fails_open():
    """A Redis error must not silence the main correlation path."""
    m = _correlation_main()
    m._semantic_deprecation.update({"checked_at": 0.0, "deprecated": False})
    r = MagicMock()
    r.raw.hgetall = AsyncMock(side_effect=ConnectionError("down"))
    assert asyncio.run(m._semantic_rule_deprecated(r)) is False


def test_the_semantic_gate_is_wired():
    src = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    gate = src.split("skip_semantic = (")[1].split("\n                    )")[0]
    assert "_semantic_rule_deprecated(redis_client)" in gate


def test_no_function_shadows_a_quiet_failure_recorder():
    """`dropped = len(...)` made `dropped` local to the correlation consume
    loop, so the parse-error handler's `dropped(...)` call raised
    UnboundLocalError -- inside the handler written for exactly that case."""
    offenders = []
    for path in list((ROOT / "services").rglob("*.py")) + list((ROOT / "shared").rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            assigned = {
                t.id for node in ast.walk(fn) if isinstance(node, ast.Assign)
                for t in node.targets if isinstance(t, ast.Name)
            }
            called = {
                node.func.id for node in ast.walk(fn)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            }
            for name in ("dropped", "swallowed"):
                if name in assigned and name in called:
                    offenders.append(f"{path.relative_to(ROOT)}:{fn.name}")
    assert offenders == []

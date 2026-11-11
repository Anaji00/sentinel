"""Retire stored synthesized rules that fail the current reuse guard.

Dry run by default: lists what would go. With --apply it removes each from the live hash and publishes the same
tombstone the rule agent uses, so the correlation engine drops it without a restart. Run inside a service container:
    docker exec -w /app -e PYTHONPATH=/app sentinel-agents-heavy python scripts/retire_unreusable_rules.py [--apply]
"""
import asyncio
import json
import sys

from shared.db import get_redis
from services.agents.rule_agent import DynamicRule, _is_reusable_rule

KEY = "sentinel:correlation:dynamic_rules"


async def main(apply: bool) -> None:
    r = await get_redis()
    stored = await r.raw.hgetall(KEY)
    failing = []
    for rid, raw in stored.items():
        body = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        if not str(body.get("rule_id", rid)).startswith("syn_"):
            continue                                    # shipped rules are not synthesized and not this script's to judge
        try:
            # The hash-derived probation id looks event-shaped to the id check; judge the content under a neutral id.
            fields = {k: v for k, v in body.items() if k in DynamicRule.model_fields}
            fields["rule_id"] = "syn_content_check"
            rule = DynamicRule(**fields)
        except Exception as exc:
            failing.append((body.get("rule_id", rid), body.get("rule_name"), f"does not parse: {exc}"))
            continue
        reason = _is_reusable_rule(rule)
        if reason:
            failing.append((body["rule_id"], body.get("rule_name"), reason))
    print(f"{len(failing)} of {len(stored)} stored rules fail the guard")
    for rid, name, reason in failing:
        print(f"  {rid}  {name!r}: {reason[:110]}")
        if apply:
            await r.raw.hdel(KEY, rid)
            await r.raw.publish("sentinel:correlation:rule_updates", json.dumps({"rule_id": rid, "deprecated": True}))
    print("removed" if apply else "dry run, nothing removed")


asyncio.run(main("--apply" in sys.argv))

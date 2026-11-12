import asyncio
import json

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    rows = await db.query("SELECT rule_id, primary_entity_id, detected_at, metrics_summary, tags FROM correlations WHERE rule_id LIKE 'DIVERGENCE%' ORDER BY detected_at DESC LIMIT 2")
    for r in rows or []:
        ms = r["metrics_summary"] if isinstance(r["metrics_summary"], dict) else json.loads(r["metrics_summary"] or "{}")
        print(r["rule_id"], r["primary_entity_id"], str(r["detected_at"])[:19], "| tags", list(r["tags"] or [])[:8])
        print("  keys:", {k: (str(v)[:40]) for k, v in ms.items() if k not in ("brief", "members")})
    n = await db.query_one("SELECT count(*) AS n FROM correlations WHERE rule_id LIKE 'DIVERGENCE%' AND detected_at > NOW() - INTERVAL '14 days'")
    print("divergence alerts in 14 days:", n["n"])


asyncio.run(main())

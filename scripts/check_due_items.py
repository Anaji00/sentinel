"""Read-only: the items that wait on a date (21 Nov 2026): CFTC COT latest report date, grade backlog, scenarios with initial_state, latest headline primaries.
Run: MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/check_due_items.py"""
import asyncio
import sys

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402


async def main() -> None:
    db = await get_timescale()
    print("cftc_cot max report_date:", (await db.query_one("SELECT max(report_date) AS d FROM cftc_cot"))["d"])
    r = await db.query_one("SELECT count(*) FILTER (WHERE resolved_at IS NOT NULL AND excess_graded_at IS NULL) AS no_excess, "
                           "count(*) FILTER (WHERE excess_graded_at IS NOT NULL AND excess_beta IS NULL) AS no_beta FROM agent_predictions")
    print("grade backlog:", dict(r))
    r = await db.query_one("SELECT count(*) FILTER (WHERE initial_state IS NOT NULL) AS with_state, count(*) AS total FROM scenarios")
    print("initial_state:", dict(r))
    for row in await db.query("SELECT primary_entity_id AS pid, primary_entity_type AS pt, left(headline, 60) AS h, created_at FROM events "
                              "WHERE type IN ('headline','social_signal') AND created_at > NOW() - INTERVAL '40 minutes' ORDER BY created_at DESC LIMIT 8"):
        print(" ", str(row["created_at"])[11:16], row["pid"], row["pt"], row["h"])


asyncio.run(main())

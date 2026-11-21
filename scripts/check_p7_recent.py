"""Read-only: the primary entity and type of the latest headline events (use after a change to news resolution).
Run: MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/check_p7_recent.py"""
import asyncio
import sys

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402


async def main() -> None:
    db = await get_timescale()
    rows = await db.query("SELECT primary_entity_id AS pid, primary_entity_type AS pt, left(headline, 70) AS h, created_at FROM events "
                          "WHERE type IN ('headline', 'social_signal') ORDER BY created_at DESC LIMIT 15")
    for r in rows:
        print(r["created_at"], "|", r["pid"], "|", r["pt"], "|", r["h"])


asyncio.run(main())

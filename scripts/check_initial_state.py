"""Read-only: how many scenarios carry initial_state (P6 Phase 0), and the latest one.
Run: MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/check_initial_state.py"""
import asyncio
import sys

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402


async def main() -> None:
    db = await get_timescale()
    r = await db.query_one("SELECT count(*) FILTER (WHERE initial_state IS NOT NULL) AS with_state, count(*) AS total, max(created_at) FILTER (WHERE initial_state IS NOT NULL) AS latest FROM scenarios")
    print(dict(r))
    last = await db.query_one("SELECT scenario_id, initial_state, confidence_overall FROM scenarios WHERE initial_state IS NOT NULL ORDER BY created_at DESC LIMIT 1")
    print(dict(last) if last else "no scenario with initial_state yet")


asyncio.run(main())

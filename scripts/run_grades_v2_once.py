"""Runs the grades v2 job once now (instead of waiting for its daily pass) and prints the agent rows it stored.
Run: docker exec -i -w /app sentinel-factor-model python - < scripts/run_grades_v2_once.py
"""
import asyncio
import json
import sys

sys.path.insert(0, "/app")
from services.agents.grades_v2_job import GRADES_V2_KEY, GradesV2Job  # noqa: E402
from shared.db import get_redis, get_timescale  # noqa: E402


async def main() -> None:
    db = await get_timescale()
    redis = await get_redis()
    print(await GradesV2Job(db, redis).run_once())
    blob = json.loads(await getattr(redis, "raw", redis).get(GRADES_V2_KEY))
    for r in blob["agents"]["rows"]:
        if r["horizon_h"] is None:
            print(f"{r['basis']:11s} {r['agent'][:38]:38s} calls {r['calls']:4d} dates {r['dates']:3d} bps {r['mean_signed_bps']:+8.1f} t {r['t_over_dates']} | {r['verdict']}")


asyncio.run(main())

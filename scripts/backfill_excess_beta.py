"""Fills the beta-adjusted excess (migration 0099) on rows already graded against SPY. Idempotent and bounded as grade_beta_pending is.
Run: docker exec -i -w /app sentinel-factor-model python - < scripts/backfill_excess_beta.py
"""
import asyncio
import sys

sys.path.insert(0, "/app")
from services.agents.excess_grading import grade_beta_pending  # noqa: E402
from shared.db import get_timescale  # noqa: E402


async def main() -> None:
    db = await get_timescale()
    total = {"selected": 0, "graded": 0, "no_beta": 0, "failed": 0}
    for i in range(40):
        out = await grade_beta_pending(db, 500)
        for k in total:
            total[k] += out[k]
        print(f"batch {i + 1}: {out}", flush=True)
        if out["selected"] == 0 or out["graded"] + out["no_beta"] + out["failed"] == 0:
            break
    print("totals:", total)


asyncio.run(main())

"""Registered phase 5 test: five daily-path variants of B1 on the eight RB1 calendars (Upgrade pre-registrations - 12 Nov). Read-only.
Run in its own capped container, never inside factor-model, and not between 22:00 and 06:00 UTC (it builds the whole panel):

    docker run --rm --memory 9g --network sentinel-network --env-file .env -v <repo>/shared:/app/shared:ro \
        -v <repo>/services:/app/services:ro -v <repo>/scripts:/app/scripts:ro -w /app -e PYTHONPATH=/app \
        sentinel-factor-model python scripts/run_b1_variants.py [--model lgbm_rank] [--offsets 0,5,10,15,2,7,12,17]

Bar: a variant is a candidate only if it beats V0's net Sharpe on at least 6 of the 8 calendars and by at least 0.10 on average.
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils import b1_variants as bv  # noqa: E402
from shared.utils import factor_model as fm  # noqa: E402

BAR_CALENDARS = 6
BAR_MEAN_GAIN = 0.10


def verdict(per_offset: dict, variant: str, n_calendars: int) -> dict:
    """Wins against V0 and mean gain over the calendars where both have a Sharpe."""
    diffs = [row[variant] - row["V0"] for row in per_offset.values() if variant in row and "V0" in row]
    wins = sum(1 for d in diffs if d > 0)
    gain = sum(diffs) / len(diffs) if diffs else 0.0
    need = min(BAR_CALENDARS, n_calendars)
    return {"wins": wins, "of": len(diffs), "mean_gain": round(gain, 3), "candidate": bool(len(diffs) >= need and wins >= need and gain >= BAR_MEAN_GAIN)}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="lgbm_rank")
    ap.add_argument("--offsets", default="0,5,10,15,2,7,12,17")
    args = ap.parse_args()
    offsets = [int(x) for x in args.offsets.split(",")]
    from services.agents.factor_engine import FactorEngine

    db, redis = await get_timescale(), await get_redis()
    panel = await FactorEngine(db, redis)._panel()
    if panel is None:
        print("no panel")
        return
    panel = fm.tradable_subset(panel)
    feats = fm.features(panel)
    spec = fm.HORIZON_SPECS["monthly"]
    market_col = panel.tickers.index("SPY")
    per_offset = {}
    for off in offsets:
        positions: list = []
        fm.walk_forward(panel, args.model, spec=spec, feats=feats, offset=off, collect=positions)
        sims = bv.simulate(positions, panel.close, market_col, spec.horizon)
        per_offset[off] = {v: round(bv.sharpe(r, spec.horizon), 3) for v, r in sims.items() if len(r) >= 3}
        print(off, json.dumps(per_offset[off]), flush=True)
    print(json.dumps({v: verdict(per_offset, v, len(offsets)) for v in ("V1", "V2", "V3", "V4")}, indent=1))


if __name__ == "__main__":
    asyncio.run(main())

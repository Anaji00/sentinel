"""Read-only: how many of the live B1 candidates (every name with a monthly score) and picks (the b1_book rule on those scores, and the stored book) had a flat
(open = high = low = close) or zero-volume daily bar in the last 3 completed sessions. Output is JSON.
Run: MSYS_NO_PATHCONV=1 docker run --rm --network sentinel-network --env-file .env -v <repo>/shared:/app/shared:ro -v <repo>/scripts:/app/scripts:ro -w /app -e PYTHONPATH=/app sentinel-agents-heavy python scripts/trial_quant_flat_live.py
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils import auto_trade as at  # noqa: E402
from shared.utils.signal_store import AUTO_TRADE_BOOK_KEY, factor_scores_key  # noqa: E402

_SQL = """
    WITH d AS (SELECT DISTINCT day FROM daily_bars WHERE ticker = 'SPY' AND day < CURRENT_DATE ORDER BY day DESC LIMIT 3)
    SELECT ticker, day, (open = close AND high = close AND low = close) AS flat, coalesce(volume, 0) = 0 AS zv
    FROM daily_bars WHERE day IN (SELECT day FROM d) AND ticker = ANY($1::text[])
"""


async def main() -> None:
    from shared.utils import model_portfolio as mp
    from shared.utils.asset_universe import load_asset_meta

    db, redis = await get_timescale(), await get_redis()
    raw = getattr(redis, "raw", redis)
    scores = json.loads(await raw.get(factor_scores_key(mp.HORIZON)) or "{}")
    pct = {str(t).upper(): float(v["percentile"]) for t, v in scores.items() if isinstance(v, dict) and v.get("percentile") is not None}
    meta = await load_asset_meta(raw)
    borrowable = {t for t, m in meta.items() if m[1] and m[2]}
    longs, shorts = at.b1_book(pct, borrowable)
    book = json.loads(await raw.get(AUTO_TRADE_BOOK_KEY) or "{}")
    names = sorted(set(pct) | set(book.get("long", [])) | set(book.get("short", [])))
    rows = await db.query(_SQL, names) or []
    hit = {}
    for r in rows:
        if r["flat"] or r["zv"]:
            h = hit.setdefault(r["ticker"], {"flat": 0, "zero_volume": 0})
            h["flat"] += int(bool(r["flat"]))
            h["zero_volume"] += int(bool(r["zv"]))

    def count(group):
        g = [t for t in group if t in hit]
        return {"n": len(group), "with_flat_or_zero_volume_bar": len(g), "names": sorted(g)[:30]}
    print(json.dumps({"candidates": count(list(pct)), "b1_long_from_scores": count(longs), "b1_short_from_scores": count(shorts),
                      "stored_book_long": count(book.get("long", [])), "stored_book_short": count(book.get("short", []))}, indent=1))


if __name__ == "__main__":
    asyncio.run(main())

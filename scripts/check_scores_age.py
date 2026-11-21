"""Prints the factor model report's as_of, the stored B1 book's dates, and how many completed sessions the scores are behind (what the trader's guard reads).
Run: docker exec -i -w /app sentinel-factor-model python - < scripts/check_scores_age.py
"""
import asyncio
import json
import sys
from datetime import date

sys.path.insert(0, "/app")
from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils.signal_store import AUTO_TRADE_BOOK_KEY, FACTOR_REPORT_KEY  # noqa: E402


async def main() -> None:
    raw = getattr(await get_redis(), "raw", None) or await get_redis()
    db = await get_timescale()
    report = json.loads(await raw.get(FACTOR_REPORT_KEY) or "{}")
    book = json.loads(await raw.get(AUTO_TRADE_BOOK_KEY) or "{}")
    as_of = str(report.get("as_of"))[:10]
    row = await db.query_one("SELECT count(DISTINCT day) AS n FROM daily_bars WHERE ticker = 'SPY' AND day > $1::date", date.fromisoformat(as_of))
    print("report as_of", as_of, "| sessions after it", (row or {}).get("n"), "| book", {k: book.get(k) for k in ("rebalanced_on", "scores_as_of")},
          "| longs", len(book.get("long") or []), "shorts", len(book.get("short") or []))


asyncio.run(main())

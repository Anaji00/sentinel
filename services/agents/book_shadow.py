"""
services/agents/book_shadow.py

Records the S5 (conviction-weighted) shadow of B1 each time the trader's stored book changes: the two weight sets (S0 equal, S5 conviction) go into
`book_shadow_books`, nothing else. No orders, no sizing change; the trader never reads it. Registered 13 Nov 2026 (reports/Upgrade pre-registrations - 12 Nov.md,
'Conviction weights (S5), one forward test'); the owner approves any use beyond recording. Judged by scripts/report_book_shadow.py.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date
from typing import Any, Dict, Optional

from shared.utils import book_shadow as bs
from shared.utils import model_portfolio as mp
from shared.utils.quiet_failures import dropped as count_dropped
from shared.utils.quiet_failures import swallowed
from shared.utils.signal_store import AUTO_TRADE_BOOK_KEY, FACTOR_REPORT_KEY, factor_scores_key

logger = logging.getLogger("agent.book_shadow")

INTERVAL_SEC = 3600
BOOK_KEY = AUTO_TRADE_BOOK_KEY                  # the trader's stored book: {"rebalanced_on", "long", "short"}
_COUNT_SQL = "SELECT count(*) AS n FROM book_shadow_books WHERE rebalanced_on = $1::date AND variant = ANY($2::text[])"
# One statement writes every variant, so a failure cannot leave one variant without the other; a half-recorded day is completed by the conflict clause.
_INSERT_SQL = """
    INSERT INTO book_shadow_books (rebalanced_on, variant, weights, scores_as_of)
    SELECT $1::date, u.variant, u.w::jsonb, $4::date FROM unnest($2::text[], $3::text[]) AS u(variant, w)
    ON CONFLICT (rebalanced_on, variant) DO NOTHING
"""


class BookShadow:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(120)
        while True:
            try:
                logger.info("Book shadow: %s", await self.run_once())
            except Exception as exc:
                swallowed("agents.book_shadow.pass", exc, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self) -> Dict[str, Any]:
        stored = _loads(await self.raw.get(BOOK_KEY)) if self.raw is not None else None
        if not stored or not stored.get("rebalanced_on") or not (stored.get("long") or stored.get("short")):
            return {"recorded": 0, "why": "no stored book"}
        day = date.fromisoformat(str(stored["rebalanced_on"])[:10])
        done = await self.db.query_one(_COUNT_SQL, day, list(bs.VARIANTS))
        if done and int(done.get("n") or 0) >= len(bs.VARIANTS):
            return {"recorded": 0, "why": "already recorded", "rebalanced_on": day.isoformat()}
        report = _loads(await self.raw.get(FACTOR_REPORT_KEY)) or {}
        as_of: Optional[str] = str(report.get("as_of"))[:10] if report.get("as_of") else None
        book_as_of = str(stored.get("scores_as_of") or "")[:10] or None
        # The weights must come from the scores that formed the book; the scores in Redis now may be newer (a restart days after the rebalance).
        if not book_as_of or book_as_of != as_of:
            count_dropped("agents.book_shadow.scores_mismatch", "scores in Redis are not the ones that formed the book; rebalance not recorded", logger,
                          detail=f"{day} book={book_as_of} now={as_of}")
            return {"recorded": 0, "why": "scores are not those that formed the book", "rebalanced_on": day.isoformat()}
        scores = _loads(await self.raw.get(factor_scores_key(mp.HORIZON))) or {}
        pct = {str(t).upper(): float(v["percentile"]) for t, v in scores.items() if isinstance(v, dict) and v.get("percentile") is not None}
        if not pct:
            return {"recorded": 0, "why": "no model scores to weight by"}
        sets = bs.shadow_weights(stored.get("long") or [], stored.get("short") or [], pct)
        variants = list(sets)
        await self.db.execute(_INSERT_SQL, day, variants, [json.dumps({t: round(w, 6) for t, w in sets[v].items()}) for v in variants],
                              date.fromisoformat(as_of))
        return {"recorded": len(sets), "rebalanced_on": day.isoformat(), "names": len(sets["S0"])}


def _loads(raw: Any) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    except ValueError:
        return None


__all__ = ["BookShadow"]

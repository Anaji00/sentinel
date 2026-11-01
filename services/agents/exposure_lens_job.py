"""
services/agents/exposure_lens_job.py

Every six hours: the B1 book's net macro and market exposures
(shared/utils/exposure_lens.py) into REPORT_KEY. Market beta is each name's
126-session beta to SPY from daily bars, weighted like the book. A flagged
exposure is logged when it first appears.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from services.agents.auto_trader import BOOK_KEY
from shared.utils import exposure_lens as el
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.exposure_lens")

INTERVAL_SEC = 6 * 3600
BETA_SESSIONS = 126
REPORT_KEY = "sentinel:exposure_lens:report"
_BETAS_SQL = "SELECT ticker, macro, beta FROM macro_exposures WHERE ticker = ANY($1::text[])"
_BARS_SQL = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2 ORDER BY ticker, day"


def market_beta(closes: Dict[str, List[Tuple[date, float]]], weights: Dict[str, float]) -> float:
    """The book's beta to SPY: each name's beta over BETA_SESSIONS, weighted."""
    spy = dict(closes.get("SPY", []))
    days = sorted(spy)[-(BETA_SESSIONS + 1):]
    rs = [math.log(spy[b] / spy[a]) for a, b in zip(days, days[1:])]
    if len(rs) < 60:
        return 0.0
    mean_s = sum(rs) / len(rs)
    var_s = sum((x - mean_s) ** 2 for x in rs)
    total = 0.0
    for t, w in weights.items():
        px = dict(closes.get(t, []))
        pairs = [(math.log(px[b] / px[a]), math.log(spy[b] / spy[a])) for a, b in zip(days, days[1:])
                 if a in px and b in px and px[a] > 0 and px[b] > 0]
        if len(pairs) < 60 or not var_s:
            continue
        ms = sum(s for _, s in pairs) / len(pairs)
        mt = sum(x for x, _ in pairs) / len(pairs)
        cov = sum((x - mt) * (s - ms) for x, s in pairs)
        vs = sum((s - ms) ** 2 for _, s in pairs)
        if vs:
            total += w * cov / vs
    return round(total, 4)


class ExposureLensJob:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)
        self._flagged: set = set()

    async def run(self) -> None:
        await asyncio.sleep(600)
        while True:
            try:
                rep = await self.run_once()
                if rep.get("rows"):
                    logger.info("Exposure lens: market beta %s, flagged %s", rep["market_beta"], rep["flagged"])
            except Exception as e:
                swallowed("agents.exposure_lens.pass", e, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self) -> Dict[str, Any]:
        book = json.loads(await self.raw.get(BOOK_KEY) or "{}")
        longs, shorts = list(book.get("long") or []), list(book.get("short") or [])
        if not longs or not shorts:
            return {"rows": []}
        weights = el.book_weights(longs, shorts)
        names = sorted(weights)
        betas = [dict(r) for r in await self.db.query(_BETAS_SQL, names) or []]
        net = el.net_exposures(weights, betas)
        coverage: Dict[str, int] = defaultdict(int)
        for b in betas:
            coverage[str(b["macro"])] += 1
        closes: Dict[str, List[Tuple[date, float]]] = defaultdict(list)
        for r in await self.db.query(_BARS_SQL, names + ["SPY"], date.today() - timedelta(days=260)) or []:
            closes[str(r["ticker"]).upper()].append((r["day"], float(r["close"])))
        beta = await asyncio.to_thread(market_beta, closes, weights)
        rows = el.lens(net, beta, coverage)
        flagged = [str(r["instrument"]) for r in rows if r["flag"]]
        for f in set(flagged) - self._flagged:
            logger.warning("B1 exposure over its limit: %s", next(r for r in rows if r["instrument"] == f))
        self._flagged = set(flagged)
        rep = {"generated_at": datetime.now(timezone.utc).isoformat(), "rebalanced_on": book.get("rebalanced_on"),
               "names": len(names), "market_beta": beta, "flagged": flagged, "limit": el.LIMIT, "rows": rows}
        await self.raw.set(REPORT_KEY, json.dumps(rep, default=str))
        return rep


__all__ = ["REPORT_KEY", "ExposureLensJob", "market_beta"]

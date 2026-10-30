"""
services/agents/case_file_job.py

Builds a case file (shared/utils/case_file.py) every INTERVAL_SEC for the
stocks worth an agent's attention -- B1's book, stocks with an alert in the last
day, and the strongest headline reads -- in a handful of batched queries, and
writes each to Redis for agents (the analyst first) to read instead of
re-querying.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from services.agents.auto_trader import BOOK_KEY
from shared.utils import case_file as cf
from shared.utils import producer_grades as pg
from shared.utils.quiet_failures import swallowed
from shared.utils.signal_store import PRODUCER_GRADES_KEY, factor_scores_key
from shared.utils.stock_features import REGISTRY

logger = logging.getLogger("agent.case_file")

INTERVAL_SEC = 15 * 60
MAX_TICKERS = 400

_ALERT_TICKERS_SQL = """
    SELECT DISTINCT primary_entity_id AS t FROM correlations
    WHERE detected_at > NOW() - INTERVAL '1 day' AND primary_entity_id ~ '^[A-Z]{1,5}$'
      AND rule_id IS DISTINCT FROM 'USER_SCENARIO_REQUEST'
"""
_NEWS_TICKERS_SQL = """
    SELECT ticker AS t FROM stock_features_daily WHERE feature = 'llm_news_30'
      AND day = (SELECT max(day) FROM stock_features_daily WHERE feature = 'llm_news_30')
    ORDER BY abs(value) DESC LIMIT 100
"""
_STORED_RANK_SQL = """
    SELECT ticker, percentile, as_of FROM factor_scores_daily WHERE horizon = 'monthly' AND ticker = ANY($1::text[])
      AND as_of = (SELECT max(as_of) FROM factor_scores_daily WHERE horizon = 'monthly')
"""
_CLOSES_SQL = """
    SELECT ticker, close FROM (
        SELECT ticker, day, close, row_number() OVER (PARTITION BY ticker ORDER BY day DESC) AS k
        FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 45 AND close > 0
    ) x WHERE k <= 22 ORDER BY ticker, day DESC
"""
_NEWS_SQL = """
    SELECT ticker, count(*) AS n, avg(score) AS mean, max(created_at) AS as_of FROM stock_news_llm
    WHERE ticker = ANY($1::text[]) AND created_at > NOW() - INTERVAL '30 days' GROUP BY ticker
"""
_HEADLINES_SQL = """
    SELECT s.ticker, s.created_at, n.headline, s.score FROM stock_news_llm s JOIN stock_news n ON n.id = s.news_id
    WHERE s.ticker = ANY($1::text[]) AND s.created_at > NOW() - INTERVAL '3 days' ORDER BY s.created_at DESC
"""
_ALERTS_SQL = """
    SELECT primary_entity_id AS ticker, detected_at, rule_name FROM correlations
    WHERE primary_entity_id = ANY($1::text[]) AND detected_at > NOW() - INTERVAL '7 days'
      AND rule_id IS DISTINCT FROM 'USER_SCENARIO_REQUEST' ORDER BY detected_at DESC
"""
_FEATURES_SQL = """
    SELECT DISTINCT ON (ticker, feature) ticker, feature, value, day FROM stock_features_daily
    WHERE ticker = ANY($1::text[]) AND feature = ANY($2::text[]) AND day > CURRENT_DATE - 45
    ORDER BY ticker, feature, day DESC
"""
_CUSTOMERS_SQL = """
    SELECT DISTINCT ON (supplier, customer_ticker) supplier, customer_ticker, pct FROM customer_links
    WHERE supplier = ANY($1::text[]) AND customer_ticker IS NOT NULL ORDER BY supplier, customer_ticker, filed DESC
"""


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v or "")


class CaseFileJob:
    def __init__(self, db: Any, redis: Any):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(240)
        while True:
            try:
                n = await self.run_once()
                logger.info("Case files: %s written", n)
            except Exception as e:
                swallowed("agents.case_file", e, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def universe(self) -> List[str]:
        names: List[str] = []
        try:
            book = json.loads(_s(await self.raw.get(BOOK_KEY)) or "{}")
            names += list(book.get("long") or []) + list(book.get("short") or [])
        except ValueError as e:
            swallowed("agents.case_file.book", e, logger)
        names += [str(r["t"]) for r in await self.db.query(_ALERT_TICKERS_SQL) or []]
        names += [str(r["t"]) for r in await self.db.query(_NEWS_TICKERS_SQL) or []]
        seen: Set[str] = set()
        out = [t.upper() for t in names if not (t.upper() in seen or seen.add(t.upper()))]
        return out[:MAX_TICKERS]

    async def run_once(self) -> int:
        tickers = await self.universe()
        if not tickers:
            return 0
        rank = await self._ranks(tickers)
        closes: Dict[str, List[float]] = defaultdict(list)
        for r in await self.db.query(_CLOSES_SQL, tickers) or []:
            closes[str(r["ticker"])].append(float(r["close"]))
        news = {str(r["ticker"]): {"n": int(r["n"]), "mean": float(r["mean"]), "as_of": str(r["as_of"])[:10]}
                for r in await self.db.query(_NEWS_SQL, tickers) or []}
        heads: Dict[str, List[Tuple[str, str, int]]] = defaultdict(list)
        for r in await self.db.query(_HEADLINES_SQL, tickers) or []:
            heads[str(r["ticker"])].append((str(r["created_at"])[:16], str(r["headline"]), int(r["score"])))
        flagging, tendencies = await self._flagging()
        alerts: Dict[str, List[Tuple[str, str, bool, str]]] = defaultdict(list)
        for r in await self.db.query(_ALERTS_SQL, tickers) or []:
            fam = pg.rule_family(str(r["rule_name"]))
            if all(a[1] != fam for a in alerts[str(r["ticker"])]):
                alerts[str(r["ticker"])].append((str(r["detected_at"])[:16], fam, fam in flagging, tendencies.get(fam, "")))
        feats: Dict[str, Dict[str, Tuple[float, str, str]]] = defaultdict(dict)
        for r in await self.db.query(_FEATURES_SQL, tickers, sorted(REGISTRY)) or []:
            f = str(r["feature"])
            feats[str(r["ticker"])][f] = (float(r["value"]), str(r["day"]), REGISTRY[f].description)
        custs: Dict[str, List[Tuple[str, Optional[float]]]] = defaultdict(list)
        for r in await self.db.query(_CUSTOMERS_SQL, tickers) or []:
            custs[str(r["supplier"])].append((str(r["customer_ticker"]), float(r["pct"]) if r["pct"] is not None else None))
        now = datetime.now(timezone.utc)
        async with self.raw.pipeline() as pipe:
            for t in tickers:
                case = cf.build(t, rank=rank.get(t), closes=closes.get(t, []), news=news.get(t),
                                headlines=heads.get(t, []), alerts=alerts.get(t, []), features=feats.get(t, {}),
                                customers=custs.get(t, []), now=now)
                pipe.set(cf.CASE_KEY.format(ticker=t), json.dumps(case), ex=cf.CASE_TTL_SEC)
            pipe.delete(cf.CASE_INDEX_KEY)
            pipe.sadd(cf.CASE_INDEX_KEY, *tickers)
            pipe.expire(cf.CASE_INDEX_KEY, cf.CASE_TTL_SEC)
            await pipe.execute()
        return len(tickers)

    async def _ranks(self, tickers: List[str]) -> Dict[str, Tuple[float, str]]:
        """Live monthly percentiles, else the stored ones."""
        try:
            live = json.loads(_s(await self.raw.get(factor_scores_key("monthly"))) or "{}")
        except ValueError:
            live = {}
        out = {t.upper(): (float(v["percentile"]), "live") for t, v in live.items()
               if isinstance(v, dict) and v.get("percentile") is not None and t.upper() in set(tickers)}
        if not out:
            out = {str(r["ticker"]): (float(r["percentile"]), str(r["as_of"]))
                   for r in await self.db.query(_STORED_RANK_SQL, tickers) or [] if r["percentile"] is not None}
        return out

    async def _flagging(self) -> Tuple[set, Dict[str, str]]:
        """Families graded to precede bigger moves, and each graded family's direction tendency."""
        try:
            blob = await self.raw.get(PRODUCER_GRADES_KEY)
            grades = json.loads(blob) if blob else None
            return pg.move_flagging_families(grades), pg.follow_tendencies(grades)
        except Exception as e:
            swallowed("agents.case_file.grades", e, logger)
            return set(), {}


__all__ = ["CaseFileJob"]

"""
services/agents/filing_change_job.py

Every half hour, BATCH stocks: each comparable pair of 10-K / 10-Q filings over
the last HISTORY_YEARS (shared/utils/filing_change.py), as the fil_change and
fil_change_risk features dated at the later filing. Ranked stocks of $1bn and
up only. Each pair is measured once; a stock's filing list is rechecked weekly,
so a new filing costs two document fetches. SEC requests go through sec_slot
with SEC_USER_AGENT; text extraction runs in a worker thread.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import aiohttp

from shared.utils import filing_change as fc
from shared.utils import filing_extract as fx
from shared.utils import market_cap as mc
from shared.utils import text_peers as tp
from shared.utils.quiet_failures import dropped, swallowed
from shared.utils.sec_rate import sec_slot
from shared.utils.signal_store import factor_scores_key

logger = logging.getLogger("agent.filing_change")

INTERVAL_SEC = 30 * 60
BATCH = 15
HISTORY_YEARS = 4
RECHECK_SEC = 7 * 86400
SOURCE = "filing_change"
DONE_KEY = "sentinel:filing_change:done"                  # "earlier|later" accession pairs measured
CHECKED_KEY = "sentinel:filing_change:checked:{ticker}"
SEC_HEADERS = {"User-Agent": os.getenv("SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local")}
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
DOC_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{doc}"
PAGE_URL = "https://data.sec.gov/submissions/{name}"
_CIK_SQL = "SELECT cik FROM issuer_ticker WHERE ticker = $1 LIMIT 1"
_UPSERT_FEATURE = """
    INSERT INTO stock_features_daily (ticker, day, feature, value, source) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (ticker, day, feature) DO UPDATE SET value = EXCLUDED.value, source = EXCLUDED.source
"""


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v or "")


class FilingChangeJob:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(900)
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    out = await self.run_once(session)
                    if out.get("pairs"):
                        logger.info("Filing change: %s", out)
                except Exception as e:
                    swallowed("agents.filing_change.pass", e, logger)
                await asyncio.sleep(INTERVAL_SEC)

    async def _universe(self) -> List[str]:
        """Ranked stocks of $1bn and up; empty without either list."""
        try:
            ranked = set(json.loads(_s(await self.raw.get(factor_scores_key("monthly"))) or "{}"))
        except ValueError:
            ranked = set()
        large = await mc.large_caps(self.raw) or set()
        return sorted(t.upper() for t in ranked if t.upper() in large)

    async def _get(self, session: aiohttp.ClientSession, url: str, as_json: bool) -> Any:
        await sec_slot()
        async with session.get(url, headers=SEC_HEADERS, timeout=aiohttp.ClientTimeout(total=120)) as resp:
            if resp.status != 200:
                return None
            return await resp.json(content_type=None) if as_json else await resp.text(errors="replace")

    async def run_once(self, session: aiohttp.ClientSession) -> Dict[str, int]:
        out = {"stocks": 0, "pairs": 0}
        todo = [t for t in await self._universe() if not await self.raw.exists(CHECKED_KEY.format(ticker=t))]
        for ticker in todo[:BATCH]:
            try:
                out["pairs"] += await self._stock(session, ticker)
                out["stocks"] += 1
                await self.raw.set(CHECKED_KEY.format(ticker=ticker), "1", ex=RECHECK_SEC)
            except Exception as e:
                swallowed("agents.filing_change.stock", e, logger, detail=ticker)
        return out

    async def _stock(self, session: aiohttp.ClientSession, ticker: str) -> int:
        row = await self.db.query_one(_CIK_SQL, ticker)
        if not row or not row.get("cik"):
            dropped("agents.filing_change.no_cik", "no SEC company id for the ticker", logger, detail=ticker)
            return 0
        cik = int(row["cik"])
        subs = await self._get(session, SUBMISSIONS_URL.format(cik=cik), as_json=True)
        if not subs:
            return 0
        since = date.today() - timedelta(days=365 * HISTORY_YEARS)
        filings = fc.recent_filings(subs, since)
        for name in fc.older_pages(subs, since):
            page = await self._get(session, PAGE_URL.format(name=name), as_json=True)
            if page:
                filings += fc.recent_filings(page, since)
        filings = list({f[3]: (f, d) for f, d in filings}.values())
        docs = {f[3]: d for f, d in filings}
        todo = []
        for earlier, later in fc.pairs([f for f, _ in filings]):
            key = f"{earlier[3]}|{later[3]}"
            if not await self.raw.sismember(DONE_KEY, key):
                todo.append((earlier, later, key))
        if not todo:
            return 0
        texts: Dict[str, Optional[str]] = {}

        async def text(acc: str) -> Optional[str]:
            if acc not in texts:
                html = await self._get(session, DOC_URL.format(cik=cik, acc=acc.replace("-", ""), doc=docs[acc]),
                                       as_json=False)
                texts[acc] = await asyncio.to_thread(fx.text_of, html) if html else None
            return texts[acc]

        latest_10k = max((f[1] for f, _ in filings if f[0] == "10-K"), default=date.min)
        n = 0
        for earlier, later, key in todo:
            a, b = await text(earlier[3]), await text(later[3])
            if not a or not b:
                continue
            whole, risk = await asyncio.to_thread(fc.change, a, b, later[0] == "10-K")
            if later[0] == "10-K" and later[1] >= latest_10k:
                # The latest 10-K's business vocabulary, for text-based peers (text_peers).
                vocab = await asyncio.to_thread(lambda: tp.terms(tp.business_section(b)))
                if vocab:
                    await self.raw.hset(tp.TERMS_KEY, ticker, json.dumps({"filed": later[1].isoformat(), "terms": vocab}))
            if whole is not None:
                await self.db.execute(_UPSERT_FEATURE, ticker, later[1], fc.FEATURE, whole, SOURCE)
            if risk is not None:
                await self.db.execute(_UPSERT_FEATURE, ticker, later[1], fc.FEATURE_RISK, risk, SOURCE)
            await self.raw.sadd(DONE_KEY, key)
            n += 1
        return n


__all__ = ["FilingChangeJob"]

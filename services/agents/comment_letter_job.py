"""
services/agents/comment_letter_job.py

Every half hour, BATCH stocks: their replies to SEC comment letters (CORRESP)
from the last HISTORY_YEARS, each read once by the extraction model
(shared/utils/comment_letters.py) into the ev_comment_letter trial feature,
dated at the reply's filing. Ranked stocks of $1bn and up; at most
MAX_LLM_PER_HOUR model calls; SEC requests through sec_slot with
SEC_USER_AGENT. A reply whose quote is not in its text is counted and dropped.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

import aiohttp

from shared.utils import comment_letters as cl
from shared.utils import filing_extract as fx
from shared.utils import market_cap as mc
from shared.utils.quiet_failures import dropped, swallowed
from shared.utils.sec_rate import sec_slot
from shared.utils.signal_store import factor_scores_key

logger = logging.getLogger("agent.comment_letters")

MODEL = os.getenv("AGENT_MODEL", "qwen2.5:3b")      # extraction tier (hosted route)
INTERVAL_SEC = 30 * 60
BATCH = 20
HISTORY_YEARS = 3
MAX_LLM_PER_HOUR = 10
RECHECK_SEC = 7 * 86400
SOURCE = "comment_letters"
DONE_KEY = "sentinel:comment_letters:done"
CHECKED_KEY = "sentinel:comment_letters:checked:{ticker}"
LOG_KEY = "sentinel:comment_letters:recent"
SEC_HEADERS = {"User-Agent": os.getenv("SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local")}
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
PAGE_URL = "https://data.sec.gov/submissions/{name}"
DOC_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{doc}"
_CIK_SQL = "SELECT cik FROM issuer_ticker WHERE ticker = $1 LIMIT 1"
_UPSERT_FEATURE = """
    INSERT INTO stock_features_daily (ticker, day, feature, value, source) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (ticker, day, feature) DO UPDATE SET value = GREATEST(stock_features_daily.value, EXCLUDED.value),
        source = EXCLUDED.source
"""


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v or "")


def replies(rec: Dict[str, Any], since: date) -> List[Tuple[date, str, str]]:
    """(filed, accession, document) for CORRESP filings with a text or HTML document since `since`."""
    out = []
    for form, filed, acc, doc in zip(rec.get("form", []), rec.get("filingDate", []), rec.get("accessionNumber", []),
                                     rec.get("primaryDocument", [])):
        if form != "CORRESP" or not doc or not str(doc).lower().endswith((".htm", ".html", ".txt")):
            continue
        try:
            d = date.fromisoformat(filed)
        except ValueError:
            continue
        if d >= since:
            out.append((d, acc, doc))
    return out


class CommentLetterJob:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.redis = redis
        self.raw = getattr(redis, "raw", redis)
        self._calls: List[float] = []

    async def run(self) -> None:
        await asyncio.sleep(1200)
        async with aiohttp.ClientSession() as session:
            from shared.utils.ollama import OllamaClient

            llm = OllamaClient(session, model=MODEL, redis_client=self.redis, service_name="comment_letters")
            while True:
                try:
                    out = await self.run_once(session, llm)
                    if out.get("read"):
                        logger.info("Comment letters: %s", out)
                except Exception as e:
                    swallowed("agents.comment_letters.pass", e, logger)
                await asyncio.sleep(INTERVAL_SEC)

    def _budget(self) -> bool:
        now = time.time()
        self._calls = [t for t in self._calls if now - t < 3600]
        return len(self._calls) < MAX_LLM_PER_HOUR

    async def _get(self, session: aiohttp.ClientSession, url: str, as_json: bool) -> Any:
        await sec_slot()
        async with session.get(url, headers=SEC_HEADERS, timeout=aiohttp.ClientTimeout(total=90)) as resp:
            if resp.status != 200:
                return None
            return await resp.json(content_type=None) if as_json else await resp.text(errors="replace")

    async def run_once(self, session: aiohttp.ClientSession, llm: Any) -> Dict[str, int]:
        try:
            ranked = set(json.loads(_s(await self.raw.get(factor_scores_key("monthly"))) or "{}"))
        except ValueError:
            ranked = set()
        large = await mc.large_caps(self.raw) or set()
        universe = sorted(t.upper() for t in ranked if t.upper() in large)
        out = {"stocks": 0, "read": 0}
        for ticker in universe:
            if out["stocks"] >= BATCH or not self._budget():
                break
            if await self.raw.exists(CHECKED_KEY.format(ticker=ticker)):
                continue
            try:
                out["read"] += await self._stock(session, llm, ticker)
                out["stocks"] += 1
                if self._budget():                  # finished, not cut short by the budget
                    await self.raw.set(CHECKED_KEY.format(ticker=ticker), "1", ex=RECHECK_SEC)
            except Exception as e:
                swallowed("agents.comment_letters.stock", e, logger, detail=ticker)
        return out

    async def _stock(self, session: aiohttp.ClientSession, llm: Any, ticker: str) -> int:
        row = await self.db.query_one(_CIK_SQL, ticker)
        if not row or not row.get("cik"):
            return 0
        cik = int(row["cik"])
        subs = await self._get(session, SUBMISSIONS_URL.format(cik=cik), as_json=True)
        if not subs:
            return 0
        since = date.today() - timedelta(days=365 * HISTORY_YEARS)
        found = replies((subs.get("filings") or {}).get("recent") or {}, since)
        for f in (subs.get("filings") or {}).get("files") or []:
            if str(f.get("filingTo", "")) >= since.isoformat():
                page = await self._get(session, PAGE_URL.format(name=f["name"]), as_json=True)
                if page:
                    found += replies(page, since)
        n = 0
        for filed, acc, doc in sorted(set(found)):
            if await self.raw.sismember(DONE_KEY, acc) or not self._budget():
                continue
            body = await self._get(session, DOC_URL.format(cik=cik, acc=acc.replace("-", ""), doc=doc), as_json=False)
            if not body:
                continue
            text = (await asyncio.to_thread(fx.text_of, body))[:cl.MAX_CHARS]
            self._calls.append(time.time())
            try:
                got = await llm.infer(cl.SYSTEM, text, cl.Reading, temperature=0.0, model=MODEL, num_predict=300)
            except Exception as e:
                swallowed("agents.comment_letters.read", e, logger)
                break                                   # model unavailable: the rest wait for the next pass
            await self.raw.sadd(DONE_KEY, acc)
            if not cl.grounded(got, text):
                dropped("agents.comment_letters.ungrounded", "quote not in the reply", logger, detail=acc)
                continue
            if cl.severity(got) <= 0:
                continue                                # procedural: no signal
            await self.db.execute(_UPSERT_FEATURE, ticker, filed, cl.FEATURE, cl.severity(got), SOURCE)
            rec = {"ticker": ticker, "accession": acc, "filed": filed.isoformat(), "topics": got.topics,
                   "restates": got.restates, "quote": got.quote[:300]}
            await self.raw.lpush(LOG_KEY, json.dumps(rec))
            await self.raw.ltrim(LOG_KEY, 0, 199)
            n += 1
        return n


__all__ = ["CommentLetterJob", "replies"]

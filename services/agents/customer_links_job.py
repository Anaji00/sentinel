"""
services/agents/customer_links_job.py

Reads ranked stocks' 10-Ks (the latest three each, so the links have history)
for the customers they name (shared/utils/customer_links.py), resolves the
names to tickers through the SEC-seeded alias table, stores them in
customer_links, and writes each supplier's customer momentum (cust_mom_21) into
stock_features_daily for the researcher's screen and the model's trials.

Paced: BATCH tickers a pass, every half hour, through the shared SEC limiter;
the extraction model only on passages the pattern could not read, at most
MAX_LLM_PER_HOUR. Each filing is read once; each ticker's filing list is
rechecked monthly.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from shared.kafka import Topics
from shared.utils import customer_links as cl
from shared.utils import filing_extract as fx
from shared.utils.quiet_failures import swallowed
from shared.utils.sec_rate import sec_slot

logger = logging.getLogger("agent.customer_links")

INTERVAL_SEC = 30 * 60
BATCH = 25
FILINGS_PER_TICKER = 3
MAX_LLM_PER_HOUR = 8
RECHECK_SEC = 30 * 86400
DONE_KEY = "sentinel:customer_links:done"              # accessions read
CHECKED_KEY = "sentinel:customer_links:checked:{ticker}"
FEATURE_DAY_KEY = "sentinel:customer_links:feature_day"
SOURCE = "customer_links"
MODEL = os.getenv("AGENT_MODEL", "qwen2.5:3b")      # extraction tier
SEC_HEADERS = {"User-Agent": os.getenv("SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local")}
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
DOC_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{doc}"

_CIK_SQL = "SELECT cik FROM issuer_ticker WHERE ticker = $1 LIMIT 1"
_UNIVERSE_SQL = """
    SELECT ticker FROM factor_scores_daily WHERE horizon = 'monthly'
      AND as_of = (SELECT max(as_of) FROM factor_scores_daily WHERE horizon = 'monthly') ORDER BY 1
"""
_UPSERT_LINK = """
    INSERT INTO customer_links (supplier, customer_name, customer_ticker, pct, filed, accession, method)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (supplier, accession, customer_name) DO UPDATE
      SET customer_ticker = EXCLUDED.customer_ticker, pct = EXCLUDED.pct
"""
_LINKS_SQL = """
    SELECT supplier, filed, customer_ticker, pct FROM customer_links
    WHERE customer_ticker IS NOT NULL AND customer_ticker <> supplier
"""
_CLOSES_SQL = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2 ORDER BY ticker, day"
_SESSIONS_SQL = "SELECT day FROM daily_bars WHERE ticker = 'SPY' AND day >= $1 ORDER BY day"
_LATEST_SQL = """
    SELECT DISTINCT ON (ticker, feature) ticker, feature, value FROM stock_features_daily
    WHERE feature = ANY($1::text[]) AND ticker = ANY($2::text[]) AND day > CURRENT_DATE - 120
    ORDER BY ticker, feature, day DESC
"""
_UPSERT_FEATURE = """
    INSERT INTO stock_features_daily (ticker, day, feature, value, source) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (ticker, day, feature) DO UPDATE SET value = EXCLUDED.value, source = EXCLUDED.source
"""


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v or "")


class CustomerLinksJob:
    def __init__(self, db: Any, redis: Any):
        self.db = db
        self.redis = redis
        self.raw = getattr(redis, "raw", redis)
        self._llm_calls: List[float] = []
        self._producer: Any = None

    async def run(self) -> None:
        await asyncio.sleep(600)
        async with aiohttp.ClientSession() as session:
            from shared.utils.ollama import OllamaClient

            llm = OllamaClient(session, model=MODEL, redis_client=self.redis, service_name="customer_links")
            while True:
                try:
                    out = await self.run_once(session, llm)
                    if out.get("filings") or out.get("feature_rows"):
                        logger.info("Customer links: %s", out)
                except Exception as e:
                    swallowed("agents.customer_links", e, logger)
                await asyncio.sleep(INTERVAL_SEC)

    def _llm_budget(self) -> bool:
        now = time.time()
        self._llm_calls = [t for t in self._llm_calls if now - t < 3600]
        return len(self._llm_calls) < MAX_LLM_PER_HOUR

    async def _universe(self) -> List[str]:
        from shared.utils.signal_store import factor_scores_key

        try:
            live = json.loads(_s(await self.raw.get(factor_scores_key("monthly"))) or "{}")
        except ValueError:
            live = {}
        return sorted(live) or [str(r["ticker"]) for r in await self.db.query(_UNIVERSE_SQL) or []]

    async def _get(self, session: aiohttp.ClientSession, url: str, as_json: bool) -> Any:
        await sec_slot()
        async with session.get(url, headers=SEC_HEADERS, timeout=aiohttp.ClientTimeout(total=90)) as resp:
            if resp.status != 200:
                return None
            return await resp.json(content_type=None) if as_json else await resp.text(errors="replace")

    async def _resolve(self, name: str) -> Optional[str]:
        """A listed company's ticker for the name or, failing that, a shorter
        form of it. A bare ticker-like word ("NET") is not taken as a name."""
        from shared.utils.entity_resolution import ALIAS_KEY, canonical_key, looks_like_ticker

        for v in cl.variants(name):
            if v.isupper() and looks_like_ticker(v):
                continue
            for k in (v.upper(), canonical_key(v)):
                if k:
                    hit = _s(await self.raw.hget(ALIAS_KEY, k)).upper()
                    if hit:
                        # A preferred share or warrant (T-PC, ...) stands for its common stock.
                        base, _, cls = hit.partition("-")
                        return base if cls[:1] in ("P", "W") else hit
        return None

    async def _filing(self, session: aiohttp.ClientSession, llm: Any, ticker: str, cik: int,
                      acc: str, doc: str, filed: date) -> int:
        html = await self._get(session, DOC_URL.format(cik=cik, acc=acc.replace("-", ""), doc=doc), as_json=False)
        if not html:
            return -1
        text = await asyncio.to_thread(fx.text_of, html)
        found: List[Tuple[str, Optional[float], str, Optional[str]]] = []
        unread: List[str] = []
        for p in cl.passages(text):
            hits = [(n, pct, "pattern", await self._resolve(n)) for n, pct in cl.candidates(p)]
            hits = [h for h in hits if h[3] and h[3] != ticker]
            if hits:
                found += hits
            elif "customer" in p.lower():
                unread.append(p)
        # One call for a filing's passages the patterns could not read: they
        # went one call each (130 calls on 29 Oct). Names are still grounded
        # verbatim in the passages they came from.
        if unread and self._llm_budget():
            self._llm_calls.append(time.time())
            batch = "\n\n".join(f"[{i + 1}] {p}" for i, p in enumerate(unread))
            try:
                got = await llm.infer(cl.EXTRACT_SYSTEM, batch, schema=cl.Customers, temperature=0.0, model=MODEL,
                                      num_predict=200 + 100 * len(unread))
                found += [(n, pct, "model", await self._resolve(n))
                          for n, pct in cl.grounded(got.model_dump()["customers"], "\n".join(unread))]
            except Exception as e:
                swallowed("agents.customer_links.extract", e, logger)
        seen = set()
        for name, pct, method, tick in found:
            if (tick or name) in seen:
                continue                                  # "Apple" and "Apple Inc" in one filing: one link
            seen.add(tick or name)
            await self.db.execute(_UPSERT_LINK, ticker, name, tick, pct, filed, acc, method)
            if tick:
                await self._propose(ticker, tick, pct, filed)
        return len(seen)

    async def _propose(self, supplier: str, customer: str, pct: Optional[float], filed: Any) -> None:
        """The link as a graph edge, through the supervisor like every other
        write: the graph had competitor, holder and insider links for 8,300
        companies and no supplier or customer link at all."""
        try:
            if self._producer is None:
                from shared.kafka import SentinelProducer
                self._producer = SentinelProducer(service_name="customer_links")
                await self._producer.start()
            await self._producer.send(Topics.ONTOLOGY_PROPOSALS, {
                "entity_id": supplier,
                "action": "LINK_ENTITY",
                "data": {"target_id": customer, "source_label": "Company", "target_label": "Company",
                         "relation_type": "SUPPLIER_TO", "confidence": 1.0, "method": "10-K Item 101",
                         "revenue_pct": pct, "filed": str(filed)},
            }, key=supplier)
        except Exception as e:
            swallowed("agents.customer_links.propose", e, logger)

    async def _ticker(self, session: aiohttp.ClientSession, llm: Any, ticker: str) -> Tuple[int, int]:
        rows = await self.db.query(_CIK_SQL, ticker) or []
        if not rows:
            return 0, 0
        cik = int(rows[0]["cik"])
        sub = await self._get(session, SUBMISSIONS_URL.format(cik=cik), as_json=True)
        recent = ((sub or {}).get("filings") or {}).get("recent") or {}
        forms = recent.get("form") or []
        picked = [i for i, f in enumerate(forms) if f == "10-K"][:FILINGS_PER_TICKER]
        filings = links = 0
        for i in picked:
            acc = str(recent["accessionNumber"][i])
            if await self.raw.sismember(DONE_KEY, acc):
                continue
            filed = date.fromisoformat(str(recent["filingDate"][i]))
            n = await self._filing(session, llm, ticker, cik, acc, str(recent["primaryDocument"][i]), filed)
            if n < 0:
                continue                                  # fetch failed: try again next month
            await self.raw.sadd(DONE_KEY, acc)
            filings += 1
            links += n
        return filings, links

    async def write_features(self, since: date) -> int:
        links: Dict[str, List[Tuple[date, str, float]]] = {}
        for r in await self.db.query(_LINKS_SQL) or []:
            links.setdefault(str(r["supplier"]), []).append(
                (r["filed"], str(r["customer_ticker"]), float(r["pct"]) if r["pct"] is not None else 0.0))
        if not links:
            return 0
        customers = sorted({c for ls in links.values() for _, c, _ in ls})
        closes: Dict[str, Tuple[List[date], List[float]]] = {}
        for r in await self.db.query(_CLOSES_SQL, customers, since - timedelta(days=45)) or []:
            d, p = closes.setdefault(str(r["ticker"]), ([], []))
            d.append(r["day"])
            p.append(float(r["close"]))
        sessions = [r["day"] for r in await self.db.query(_SESSIONS_SQL, since) or []]
        rows = [(*x, SOURCE) for s, ls in links.items() for x in cl.momentum_rows(s, ls, closes, sessions)]
        # Customers' headline read and filing change carried to suppliers (trial), today's only.
        latest = {(str(r["ticker"]), str(r["feature"])): float(r["value"])
                  for r in await self.db.query(_LATEST_SQL, sorted(cl.PROPAGATED), customers) or []}
        rows += [(*x, SOURCE) for x in cl.propagated_rows(links, latest, date.today())]
        for k in range(0, len(rows), 5000):
            await self.db.execute_many(_UPSERT_FEATURE, rows[k:k + 5000])
        return len(rows)

    async def run_once(self, session: aiohttp.ClientSession, llm: Any) -> Dict[str, Any]:
        filings = links = done = 0
        for t in await self._universe():
            if done >= BATCH:
                break
            key = CHECKED_KEY.format(ticker=t)
            if await self.raw.exists(key):
                continue
            try:
                f, n = await self._ticker(session, llm, t)
                filings, links = filings + f, links + n
            except Exception as e:
                swallowed("agents.customer_links.ticker", e, logger, detail=t)
            await self.raw.set(key, "1", ex=RECHECK_SEC)
            done += 1
        # The feature: from 2016 while links are still arriving, then the last fortnight once a day.
        today = datetime.now(timezone.utc).date()
        rows = 0
        if filings or _s(await self.raw.get(FEATURE_DAY_KEY)) != today.isoformat():
            rows = await self.write_features(date(2016, 1, 1) if filings else today - timedelta(days=14))
            await self.raw.set(FEATURE_DAY_KEY, today.isoformat())
        return {"tickers": done, "filings": filings, "links": links, "feature_rows": rows}


__all__ = ["CustomerLinksJob"]

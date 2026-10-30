"""
services/agents/activist_13d_job.py

Reads Item 4 of new Schedule 13D filings and amendments (shared/utils/
activist_13d.py) with a hosted extraction model, and writes ev_13d_activist into
stock_features_daily for the factor model's trials and the case file.

About ten 13D filings a day arrive (12 new and 61 amendments in the week to 29
Oct 2026); 13Gs are passive by definition and skipped. Each filing is read once;
a failed read is retried at most MAX_ATTEMPTS times, then parked. SEC requests
go through sec_slot(). Hosted only: with no hosted key the job does nothing.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from shared.utils import activist_13d as a13
from shared.utils import filing_extract as fx
from shared.utils import hosted_llm
from shared.utils.form4 import index_json_url
from shared.utils.ollama import InferenceError
from shared.utils.quiet_failures import dropped, swallowed
from shared.utils.sec_rate import sec_slot

logger = logging.getLogger("agent.activist_13d")

INTERVAL_SEC = 30 * 60
MAX_PER_PASS = 20
MAX_ATTEMPTS = 2
DONE_KEY = "sentinel:activist_13d:done"
TRIES_KEY = "sentinel:activist_13d:tries"
LOG_KEY = "sentinel:activist_13d:recent"          # the latest readings, for the dashboard and case file
DONE_TTL_SEC = 30 * 86400
SOURCE = "activist_13d"
MODEL = os.getenv("ACTIVIST_MODEL", "groq/openai/gpt-oss-20b")
SEC_HEADERS = {"User-Agent": os.getenv("SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local")}

_FILINGS_SQL = """
    SELECT created_at, filing_data FROM events
    WHERE source = 'sec_13d' AND created_at > NOW() - INTERVAL '7 days'
      AND upper(filing_data->>'form_type') = ANY($1::text[]) ORDER BY created_at DESC LIMIT 200
"""
_UPSERT_FEATURE = """
    INSERT INTO stock_features_daily (ticker, day, feature, value, source) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (ticker, day, feature) DO UPDATE SET value = GREATEST(stock_features_daily.value, EXCLUDED.value),
        source = EXCLUDED.source
"""


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v or "")


class Activist13DJob:
    def __init__(self, db: Any, redis: Any):
        self.db = db
        self.redis = redis
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        if not hosted_llm.is_hosted(MODEL):
            logger.info("13D reading off: ACTIVIST_MODEL must name a hosted model, got %r", MODEL)
            return
        await asyncio.sleep(420)
        async with aiohttp.ClientSession() as session:
            from shared.utils.ollama import OllamaClient

            llm = OllamaClient(session, model=MODEL, redis_client=self.redis, service_name="activist_13d")
            while True:
                try:
                    out = await self.run_once(session, llm)
                    if out.get("read"):
                        logger.info("13D intent: %s", out)
                except Exception as e:
                    swallowed("agents.activist_13d", e, logger)
                await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self, session: aiohttp.ClientSession, llm: Any) -> Dict[str, int]:
        read = activist = 0
        for r in (await self.db.query(_FILINGS_SQL, [f.upper() for f in a13.FORMS]) or [])[:MAX_PER_PASS * 5]:
            if read >= MAX_PER_PASS:
                break
            fd = r.get("filing_data")
            fd = json.loads(fd) if isinstance(fd, str) else (fd or {})
            acc, ticker = str(fd.get("accession_number") or ""), str(fd.get("ticker") or "").upper()
            if not acc or not ticker or await self.raw.sismember(DONE_KEY, acc):
                continue
            try:
                got = await self._read(session, llm, fd)
            except InferenceError as exc:
                # The model was unavailable (quota, cooldown, outage): not the
                # filing's fault, so it is not counted towards parking it.
                swallowed("agents.activist_13d.model_unavailable", exc, logger)
                break
            except Exception as exc:
                await self._failed(acc, exc)
                continue
            await self.raw.sadd(DONE_KEY, acc)
            await self.raw.expire(DONE_KEY, DONE_TTL_SEC)
            if got is None:
                continue
            read += 1
            day = _day(r.get("created_at"))
            await self.db.execute(_UPSERT_FEATURE, ticker, day, a13.FEATURE, 1.0 if got.activist else 0.0, SOURCE)
            activist += int(got.activist)
            rec = {"ticker": ticker, "accession": acc, "form": fd.get("form_type"), "filer_company": fd.get("company_name"),
                   "activist": got.activist, "intents": a13.clean_intents(got.intents), "hostile": got.hostile,
                   "quote": got.quote[:300], "day": day.isoformat()}
            await self.raw.lpush(LOG_KEY, json.dumps(rec))
            await self.raw.ltrim(LOG_KEY, 0, 199)
        return {"read": read, "activist": activist}

    async def _read(self, session: aiohttp.ClientSession, llm: Any, fd: Dict[str, Any]) -> Optional[a13.Intent]:
        """The filing's intent, or None when it has no Item 4 or the reading
        does not stand on the text (both counted)."""
        body = await self._item4(session, str(fd.get("primary_doc_url") or ""))
        if not body:
            dropped("agents.activist_13d.no_item4", "13D without an Item 4 heading", logger)
            return None
        got = await llm.infer(a13.SYSTEM, body, a13.Intent, temperature=0.0, model=MODEL, max_retries=1, num_predict=600)
        if not a13.grounded(got, body):
            dropped("agents.activist_13d.ungrounded", "activist reading whose quote is not in Item 4", logger)
            return None
        return got

    async def _item4(self, session: aiohttp.ClientSession, index_link: str) -> Optional[str]:
        """Item 4's text: from the structured primary_doc.xml EDGAR has used for
        13Ds since late 2024, else cut from the HTML or text document."""
        url = index_json_url(index_link)
        if not url:
            return None
        await sec_slot()
        async with session.get(url, headers=SEC_HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                return None
            listing = await resp.json(content_type=None)
        base = url.rsplit("/", 1)[0]
        names = [str(i.get("name")) for i in ((listing or {}).get("directory") or {}).get("item") or []]
        xml = [n for n in names if n.lower().endswith(".xml")]
        docs: List[str] = xml + sorted((n for n in names if n.lower().endswith((".htm", ".html", ".txt"))
                                        and "index" not in n.lower()),
                                       key=lambda n: (not n.lower().endswith((".htm", ".html")), n))
        for name in docs[:3]:
            await sec_slot()
            async with session.get(f"{base}/{name}", headers=SEC_HEADERS, timeout=aiohttp.ClientTimeout(total=60)) as r2:
                if r2.status != 200:
                    continue
                raw_doc = await r2.text(errors="replace")
            body = a13.item4_from_xml(raw_doc) if name.lower().endswith(".xml") else a13.item4(fx.text_of(raw_doc))
            if body:
                return body
        return None

    async def _failed(self, acc: str, exc: Exception) -> None:
        tries = int(await self.raw.hincrby(TRIES_KEY, acc, 1))
        await self.raw.expire(TRIES_KEY, DONE_TTL_SEC)
        if tries < MAX_ATTEMPTS:
            swallowed("agents.activist_13d.read", exc, logger)
            return
        await self.raw.sadd(DONE_KEY, acc)
        dropped("agents.activist_13d.parked", "13D failed MAX_ATTEMPTS reads and was marked done", logger,
                detail=f"{acc}: {exc}"[:300])


def _day(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.date()
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).date()
    except ValueError:
        return datetime.now(timezone.utc).date()


__all__ = ["Activist13DJob"]

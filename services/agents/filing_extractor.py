"""
services/agents/filing_extractor.py

Reads new 8-K earnings releases and equity-sale filings for ranked stocks and
writes what they state as events (shared/utils/filing_extract.py): guidance
raised, lowered, maintained or initiated (the local model, only on passages
that mention guidance), and an equity sale's size against market value
(pattern, no model). The events carry the tags the guidance and offering
families read, so the reaction study measures them from here on.

Paced: at most MAX_LLM_PER_HOUR model calls, since this host affords about
twenty an hour across every agent, and each filing is read once.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from shared.utils import filing_extract as fx
from shared.utils.form4 import index_json_url
from shared.utils.quiet_failures import swallowed
from shared.utils.quote_cache import parse_quote, quote_key
from shared.utils.sec_rate import sec_slot
from shared.utils.signal_store import factor_scores_key

logger = logging.getLogger("agent.filing_extractor")

INTERVAL_SEC = 20 * 60
FIRST_RUN_DELAY_SEC = 300
MAX_LLM_PER_HOUR = 6
DONE_KEY = "sentinel:filing_extract:done"
DONE_TTL_SEC = 14 * 86400
SEC_HEADERS = {"User-Agent": os.getenv("SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local")}
MARKETCAP_KEY = "sentinel:marketcap:usd:{ticker}"

_FILINGS_SQL = """
    SELECT event_id::text AS event_id, created_at, financial_data->>'ticker' AS fd_ticker, filing_data
    FROM events
    WHERE type = 'filing' AND created_at > NOW() - INTERVAL '2 days' AND summary LIKE 'Material 8-K Event%'
      AND (summary LIKE '%2.02:%' OR summary LIKE '%3.02:%') AND headline NOT LIKE '%8-K/A%'
    ORDER BY created_at
"""
_EVENTS_SQL = """
    INSERT INTO events (event_id, type, source, occurred_at, collected_at, created_at, headline, summary, tags,
                        primary_entity_id, financial_data, anomaly_score, region)
    VALUES ($1::uuid, 'filing', $2, $3, NOW(), $3, $4, $5, $6, $7, $8::jsonb, $9, 'US')
    ON CONFLICT DO NOTHING
"""


class FilingExtractor:
    def __init__(self, db: Any, redis: Any):
        self.db = db
        self.raw = getattr(redis, "raw", redis)
        self._llm_calls: List[float] = []

    async def run(self) -> None:
        await asyncio.sleep(FIRST_RUN_DELAY_SEC)
        async with aiohttp.ClientSession() as session:
            from shared.utils.ollama import OllamaClient
            llm = OllamaClient(session, redis_client=None, service_name="filing_extractor")
            while True:
                try:
                    out = await self.run_once(session, llm)
                    if out.get("written"):
                        logger.info("Filing extraction: %s", out)
                except Exception as exc:
                    logger.error("Filing extraction failed: %s", exc, exc_info=True)
                await asyncio.sleep(INTERVAL_SEC)

    def _llm_budget(self) -> bool:
        now = time.time()
        self._llm_calls = [t for t in self._llm_calls if now - t < 3600]
        return len(self._llm_calls) < MAX_LLM_PER_HOUR

    async def _ranked(self) -> set:
        blob = await self.raw.get(factor_scores_key("monthly"))
        try:
            return set(json.loads(blob)) if blob else set()
        except (ValueError, TypeError):
            return set()

    async def _documents(self, session: aiohttp.ClientSession, index_link: str) -> List[Dict[str, str]]:
        url = index_json_url(index_link)
        if not url:
            return []
        await sec_slot()
        async with session.get(url, headers=SEC_HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                return []
            listing = await resp.json(content_type=None)
        base = url.rsplit("/", 1)[0]
        items = ((listing or {}).get("directory") or {}).get("item") or []
        return [{"name": str(i.get("name")), "url": f"{base}/{i.get('name')}"} for i in items
                if str(i.get("name", "")).lower().endswith((".htm", ".html", ".txt"))]

    async def _text(self, session: aiohttp.ClientSession, url: str) -> str:
        await sec_slot()
        async with session.get(url, headers=SEC_HEADERS, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            return fx.text_of(await resp.text(errors="replace")) if resp.status == 200 else ""

    async def run_once(self, session: aiohttp.ClientSession, llm: Any) -> Dict[str, int]:
        ranked = await self._ranked()
        written = seen = 0
        for r in await self.db.query(_FILINGS_SQL) or []:
            fd = r.get("filing_data")
            fd = json.loads(fd) if isinstance(fd, str) else (fd or {})
            ticker = str(fd.get("ticker") or r.get("fd_ticker") or "").upper()
            acc = str(fd.get("accession_number") or "")
            if not ticker or not acc or (ranked and ticker not in ranked):
                continue
            if await self.raw.sismember(DONE_KEY, acc):
                continue
            seen += 1
            items = set(fd.get("items") or [])
            docs = await self._documents(session, str(fd.get("primary_doc_url") or ""))
            if not docs:
                continue
            out: Optional[Dict[str, Any]] = None
            urls = {d["name"]: d["url"] for d in docs}
            picked = fx.pick_documents(list(urls), acc)
            if "2.02" in items:
                exhibit = urls.get(picked["release"] or "")
                passage = fx.guidance_passages(await self._text(session, exhibit)) if exhibit else ""
                if not passage:
                    out = {"kind": "guidance", "direction": "none", "evidence": ""}
                elif self._llm_budget():
                    self._llm_calls.append(time.time())
                    got = await llm.infer(fx.GUIDANCE_SYSTEM, passage, fx.Guidance, temperature=0.0)
                    out = {"kind": "guidance", "direction": got.direction, "evidence": got.evidence[:300]}
                else:
                    continue                          # over budget: read next pass
            elif "3.02" in items:
                main = urls.get(picked["body"] or "")
                shares = fx.offered_shares(await self._text(session, main)) if main else None
                price = parse_quote(await self.raw.get(quote_key(ticker)))
                cap_raw = await self.raw.get(MARKETCAP_KEY.format(ticker=ticker))
                cap = float(cap_raw) if cap_raw else None
                out = {"kind": "offering", "shares": shares, "tags": fx.offering_tags(shares, price, cap)}
            await self.raw.sadd(DONE_KEY, acc)
            await self.raw.expire(DONE_KEY, DONE_TTL_SEC)
            if out is None:
                continue
            await self._write(r, ticker, acc, out)
            written += 1
        return {"filings": seen, "written": written}

    async def _write(self, r: Dict[str, Any], ticker: str, acc: str, out: Dict[str, Any]) -> None:
        if out["kind"] == "guidance":
            tags = ["filing_extract", f"guidance_{out['direction']}", f"exposes:{ticker}"]
            headline = f"{ticker} earnings release: guidance {out['direction']}"
            summary = out["evidence"] or "No forward guidance in the release."
        else:
            tags = ["filing_extract", "equity_offering", *out["tags"], f"exposes:{ticker}"]
            headline = f"{ticker} unregistered equity sale: {out['shares'] or 'unstated'} shares"
            summary = "Size against market value: " + (out["tags"][0].replace("offering_", "") if out["tags"] else "unknown")
        # Stamped when read, not when the filing arrived: the label did not
        # exist before this moment, and the study measures from when it did.
        created = datetime.now(timezone.utc)
        eid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"filing-extract:{acc}:{out['kind']}"))
        try:
            await self.db.execute(_EVENTS_SQL, eid, fx.EXTRACT_SOURCE, created,
                                  headline, summary, tags, ticker,
                                  {"ticker": ticker, "accession": acc, **{k: v for k, v in out.items() if k != "tags"}},
                                  0.6 if out.get("direction") in ("raised", "lowered") or "offering_large" in tags else 0.3)
        except Exception as exc:
            swallowed("agents.filing_extractor.write", exc, logger)


__all__ = ["FilingExtractor"]

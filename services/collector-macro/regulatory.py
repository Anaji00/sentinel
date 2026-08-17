"""
services/collector-macro/regulatory.py

REGULATORY & LEGISLATIVE POLICY TRACKER (§2.4)
==============================================
Polls the Federal Register API and legislative actions for policy decisions
affecting tracked equities, semiconductor supply chains, and export controls:
- BIS / Commerce: Export controls, Entity List additions, AI chip restrictions
- FTC / DOJ: Antitrust actions, merger challenges
- USTR: Section 301 tariffs, trade policy
- DOE / FCC / FDA: Energy, spectrum, and biotech regulatory actions

Publishes RawEvent(source="federal_register", trade_type="REGULATORY_ACTION")
to Kafka `Topics.RAW_NEWS` / `Topics.RAW_TRADFI` with `REGULATORY_EXPOSURE` graph links.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import aiohttp

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent

logger = logging.getLogger("collector.macro.regulatory")

FEDERAL_REGISTER_API = "https://www.federalregister.gov/api/v1/documents.json"

TARGET_KEYWORDS = [
    "export controls",
    "semiconductor",
    "advanced computing",
    "antitrust",
    "Section 301",
    "foreign investment",
    "critical minerals",
    "artificial intelligence",
    "entity list",
]

TARGET_AGENCIES = [
    "industry-and-security-bureau",
    "federal-trade-commission",
    "justice-department",
    "international-trade-commission",
    "energy-department",
    "food-and-drug-administration",
]

# Tickers exposed by sector keywords
KEYWORD_TO_TICKERS = {
    "semiconductor": ["NVDA", "AMD", "INTC", "TSM", "ASML", "AVGO", "QCOM"],
    "export controls": ["NVDA", "AMD", "ASML", "KLAC", "LRCX", "AMAT"],
    "advanced computing": ["NVDA", "MSFT", "GOOGL", "AMZN", "META"],
    "antitrust": ["GOOGL", "AAPL", "AMZN", "META", "MSFT"],
    "Section 301": ["AAPL", "TSLA", "NKE", "BABA"],
}


class RegulatoryDeduplicator:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.key = "sentinel:seen:regulatory"

    async def is_seen(self, doc_num: str) -> bool:
        if not self.redis:
            return False
        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            score = await raw_redis.zscore(self.key, doc_num)
            return score is not None
        except Exception:
            return False

    async def mark_seen(self, doc_num: str):
        if not self.redis:
            return
        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            now = time.time()
            pipe = raw_redis.pipeline()
            pipe.zadd(self.key, {doc_num: now})
            cutoff = now - (60 * 86400)
            pipe.zremrangebyscore(self.key, 0, cutoff)
            await pipe.execute()
        except Exception as e:
            logger.debug(f"Regulatory dedup notice: {e}")


async def poll_federal_register(
    session: aiohttp.ClientSession,
    producer: SentinelProducer,
    dedup: RegulatoryDeduplicator,
) -> int:
    """
    Queries Federal Register API for recent rules and notices in target areas.
    """
    new_count = 0
    headers = {"User-Agent": "Sentinel-Intelligence-Platform/1.0 research@sentinel.local"}

    for kw in TARGET_KEYWORDS[:4]:  # Query top 4 search terms per cycle
        params = {
            "conditions[term]": kw,
            "conditions[type][]": ["RULE", "PRORULE", "NOTICE"],
            "order": "newest",
            "per_page": 5,
        }

        try:
            async with session.get(FEDERAL_REGISTER_API, params=params, headers=headers, timeout=10) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()

            results = data.get("results", [])
            for doc in results:
                doc_num = doc.get("document_number")
                if not doc_num or await dedup.is_seen(doc_num):
                    continue

                await dedup.mark_seen(doc_num)

                title = doc.get("title", "Regulatory Policy Action")
                abstract = doc.get("abstract") or title
                doc_type = doc.get("type", "RULE")
                pub_date = doc.get("publication_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
                doc_url = doc.get("html_url", "")
                agencies = [a.get("name", "") for a in doc.get("agencies", [])]
                primary_agency = agencies[0] if agencies else "Federal Regulatory Body"

                # Match affected equities
                affected = []
                for k, tickers in KEYWORD_TO_TICKERS.items():
                    if k.lower() in (title + " " + abstract).lower():
                        for t in tickers:
                            if t not in affected:
                                affected.append(t)

                event = RawEvent(
                    source="federal_register",
                    occurred_at=datetime.now(timezone.utc),
                    raw_payload={
                        "document_number": doc_num,
                        "title": title,
                        "summary": f"{primary_agency} {doc_type}: {abstract[:400]}",
                        "agency": primary_agency,
                        "agencies": agencies,
                        "action_type": doc_type,
                        "publication_date": pub_date,
                        "regulation_url": doc_url,
                        "query_keyword": kw,
                        "affected_tickers": affected,
                        "trade_type": "REGULATORY_ACTION",
                        "source_type": "primary_filing",
                        "reliability": 0.95,
                        "tags": ["regulatory", "policy", f"agency:{primary_agency.lower().replace(' ', '_')}", f"keyword:{kw}"],
                    }
                )

                await producer.send(Topics.RAW_NEWS, event.model_dump(), key=doc_num)
                new_count += 1

        except Exception as e:
            logger.debug(f"Federal register poll notice for '{kw}': {e}")

    return new_count

"""
services/collector-filings/beneficial.py

Schedule 13D and 13G filings as they arrive, into events and the ownership graph.

EDGAR's current feed lists each filing twice -- once under the subject company
"(Subject)" and once under the filer "(Filed by)" -- so entries are grouped by
accession: the subject's CIK names the company, the filer's names the holder,
and the structured primary_doc.xml gives the stake. Each holder's last reported
percentage per company is kept, so an amendment can say how far it moved.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import aiohttp
import feedparser

_PARENTS = Path(__file__).resolve().parents
ROOT = _PARENTS[2] if len(_PARENTS) > 2 else _PARENTS[-1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.kafka import Topics  # noqa: E402
from shared.models import Entity, EntityType, EventType, NormalizedEvent, RawEvent  # noqa: E402
from shared.models.events import FilingData  # noqa: E402
from shared.utils.beneficial import headline, is_common_equity, parse_schedule, score_schedule  # noqa: E402
from shared.utils.quiet_failures import dropped, swallowed  # noqa: E402

logger = logging.getLogger("collector.filings.beneficial")

SEC_HEADERS = {"User-Agent": os.getenv("SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local")}
FEED = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form}&owner=include&count=80&output=atom"
FORMS = ("SCHEDULE%2013D", "SCHEDULE%2013G")
POLL_SEC = 600
SEEN_KEY = "sentinel:beneficial:seen"
LAST_PCT_KEY = "sentinel:beneficial:last_pct"     # hash: "{filer_cik}:{issuer_cik}" -> pct
_ENTRY = re.compile(r"^(?P<form>.+?) - (?P<name>.+) \((?P<cik>\d{6,10})\) \((?P<role>Subject|Filed by)\)$")


def group_entries(entries) -> Dict[str, Dict[str, Any]]:
    """{accession: {form, link, subject: (name, cik), filer: (name, cik)}} from the feed."""
    out: Dict[str, Dict[str, Any]] = {}
    for e in entries:
        m = _ENTRY.match((e.get("title") or "").strip())
        acc = re.search(r"/(\d{18})/", e.get("link") or "")
        if not m or not acc:
            continue
        g = out.setdefault(acc.group(1), {"form": m.group("form"), "link": e.get("link")})
        key = "subject" if m.group("role") == "Subject" else "filer"
        g[key] = (m.group("name").strip(), str(int(m.group("cik"))))
    return {a: g for a, g in out.items() if "subject" in g and "filer" in g}


def schedule_event(acc: str, group: Dict[str, Any], parsed: Dict[str, Any], ticker: str,
                   previous_pct: Optional[float]) -> NormalizedEvent:
    filer_name = group["filer"][0]
    score = score_schedule(parsed, previous_pct)
    tags = ["beneficial_ownership", "schedule_13d" if parsed["is_13d"] else "schedule_13g",
            f"exposes:{ticker}", f"holder:{filer_name.lower()[:60]}"]
    if parsed["is_13d"]:
        tags.append("activist")
    summary = (f"{group['form']} filed for {group['subject'][0]} ({ticker}) by {filer_name}. "
               f"Reported stake: {parsed['pct'] if parsed['pct'] is not None else 'n/a'}% of the class"
               + (f", previously {previous_pct}%" if previous_pct is not None else "") + ". "
               + (f"Purpose: {parsed['purpose'][:300]}" if parsed.get("purpose") else ""))
    return NormalizedEvent(
        # A UUID, derived from the accession so a re-poll is still one event.
        # "sched13_{acc}" could not be stored -- events.event_id is uuid -- so
        # no live 13D/13G ever reached the events table, the study or the
        # correlations that cite it.
        event_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"sec-schedule-13:{acc}")),
        type=EventType.FILING,
        occurred_at=datetime.now(timezone.utc),
        collected_at=datetime.now(timezone.utc),
        source="sec_13d",
        source_reliability=0.99,
        primary_entity=Entity(id=ticker, name=group["subject"][0], type=EntityType.INSTRUMENT),
        headline=headline(parsed, ticker, filer_name, previous_pct),
        summary=summary,
        anomaly_score=score,
        filing_data=FilingData(ticker=ticker, cik=group["subject"][1], company_name=group["subject"][0],
                               form_type=group["form"], accession_number=acc,
                               primary_doc_url=group.get("link"), description=parsed.get("purpose") or None),
        tags=tags,
        named_entities=[ticker, filer_name],
        region="US",
    )


async def poll_once(session: aiohttp.ClientSession, producer: Any, raw: Any, graph: Any,
                    cik_ticker: Callable[[str], Optional[str]]) -> int:
    emitted = 0
    for form in FORMS:
        async with session.get(FEED.format(form=form), timeout=aiohttp.ClientTimeout(total=60)) as r:
            feed = feedparser.parse(await r.read())
        await asyncio.sleep(0.25)
        for acc, group in group_entries(feed.entries).items():
            if await raw.sismember(SEEN_KEY, acc):
                continue
            ticker = cik_ticker(group["subject"][1])
            if not ticker:
                await raw.sadd(SEEN_KEY, acc)
                dropped("collector_filings.beneficial_unlisted", "subject company has no listed ticker",
                        logger, detail=group["subject"][0][:60])
                continue
            base = group["link"].rsplit("/", 1)[0]
            try:
                async with session.get(f"{base}/primary_doc.xml", timeout=aiohttp.ClientTimeout(total=60)) as r:
                    parsed = parse_schedule(await r.text()) if r.status == 200 else None
            except Exception as e:
                swallowed("collector_filings.beneficial.doc", e, logger)
                continue
            await asyncio.sleep(0.25)
            await raw.sadd(SEEN_KEY, acc)
            if not parsed:
                dropped("collector_filings.beneficial_unparsed", "no structured primary_doc.xml", logger)
                continue
            if not is_common_equity(parsed.get("class_title", ""), ticker):
                dropped("collector_filings.beneficial_not_common", "stake in a preferred or note class",
                        logger, detail=f"{ticker} {parsed.get('class_title', '')[:40]}")
                continue
            pair = f"{group['filer'][1]}:{group['subject'][1]}"
            prev_raw = await raw.hget(LAST_PCT_KEY, pair)
            previous = float(prev_raw) if prev_raw not in (None, b"", "") else None
            event = schedule_event(acc, group, parsed, ticker, previous)
            await producer.send(Topics.RAW_TRADFI, RawEvent(
                source=event.source, occurred_at=event.occurred_at,
                raw_payload={"normalized_event": event.model_dump(mode="json")},
            ).model_dump(), key=ticker)
            emitted += 1
            if parsed.get("pct") is not None:
                await raw.hset(LAST_PCT_KEY, pair, str(parsed["pct"]))
                if graph is not None:
                    await graph.link_entities(
                        group["filer"][0].upper()[:80], "HOLDS_STAKE_IN", ticker,
                        properties={"pct_outstanding": parsed["pct"], "period": parsed.get("event_date"),
                                    "source": "13D" if parsed["is_13d"] else "13G",
                                    "activist": parsed["is_13d"], "confidence": 0.99},
                        source_label="InstitutionalFiler", target_label="Company",
                    )
            logger.info("%s", event.headline)
    await raw.expire(SEEN_KEY, 90 * 86400)
    return emitted


async def run_forever(producer: Any, redis_client: Any, cik_ticker: Callable[[str], Optional[str]]) -> None:
    raw = getattr(redis_client, "raw", redis_client)
    graph = None
    try:
        from services.enrichment.graph_writer import GraphWriter
        graph = GraphWriter(producer)
    except Exception as e:
        swallowed("collector_filings.beneficial.graph_writer", e, logger)
    while True:
        try:
            async with aiohttp.ClientSession(headers=SEC_HEADERS) as session:
                await poll_once(session, producer, raw, graph, cik_ticker)
        except Exception as e:
            swallowed("collector_filings.beneficial.poll", e, logger)
        await asyncio.sleep(POLL_SEC)

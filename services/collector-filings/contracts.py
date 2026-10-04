"""
services/collector-filings/contracts.py

Federal contract obligations to listed companies, from USAspending.gov's
transaction search, as one event per company per day and as graph edges from
the awarding agency to the company.

USAspending publishes contract actions with a lag of days, so each poll looks
back over the last LOOKBACK_DAYS and remembers every transaction it has seen.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import aiohttp

_PARENTS = Path(__file__).resolve().parents
ROOT = _PARENTS[2] if len(_PARENTS) > 2 else _PARENTS[-1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.kafka import Topics  # noqa: E402
from shared.models import Entity, EntityType, EventType, NormalizedEvent, RawEvent  # noqa: E402
from shared.models.events import RegulatoryData  # noqa: E402
from shared.utils.contracts import aggregate_awards, award_score, parent_ticker  # noqa: E402
from shared.utils.ownership import MCAP_KEY  # noqa: E402
from shared.utils.quiet_failures import swallowed  # noqa: E402

logger = logging.getLogger("collector.filings.contracts")

URL = "https://api.usaspending.gov/api/v2/search/spending_by_transaction/"
MIN_OBLIGATION_USD = 25_000_000
# A company-day below this is not news. The search's size filter applies to the
# award, not the transaction, so Lockheed, Boeing and Northrop arrived as "$0M"
# days made of small modifications to large awards.
MIN_EVENT_TOTAL_USD = 10_000_000
LOOKBACK_DAYS = 5
PAGES = 5
POLL_SEC = 6 * 3600
SEEN_KEY = "sentinel:contracts:seen"
FIELDS = ["Award ID", "Recipient Name", "Transaction Amount", "Awarding Agency", "Action Date",
          "Transaction Description", "Mod", "internal_id"]


async def fetch_transactions(session: aiohttp.ClientSession, start: date, end: date) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for page in range(1, PAGES + 1):
        body = {
            "filters": {
                "time_period": [{"start_date": start.isoformat(), "end_date": end.isoformat()}],
                "award_type_codes": ["A", "B", "C", "D"],
                "award_amounts": [{"lower_bound": MIN_OBLIGATION_USD}],
            },
            "fields": FIELDS, "limit": 100, "page": page, "sort": "Transaction Amount", "order": "desc",
        }
        async with session.post(URL, json=body, timeout=aiohttp.ClientTimeout(total=120)) as r:
            if r.status != 200:
                break
            data = await r.json()
        batch = data.get("results") or []
        rows.extend(batch)
        if not (data.get("page_metadata") or {}).get("hasNext") or len(batch) < 100:
            break
        await asyncio.sleep(1.0)
    return rows


def _key(row: Dict[str, Any]) -> str:
    return f"{row.get('Award ID')}|{row.get('Mod')}|{row.get('Action Date')}|{row.get('Transaction Amount')}"


def contract_event(ticker: str, agg: Dict[str, Any], day: str, score: float) -> NormalizedEvent:
    total = float(agg["total"])
    agencies = sorted(a for a in agg["agencies"] if a)
    largest = agg["largest"] or {}
    return NormalizedEvent(
        # The total is part of the id: a later poll the same day adds actions.
        event_id=f"contracts_{ticker}_{day}_{int(total)}",
        type=EventType.REGULATORY_EVENT,
        occurred_at=datetime.now(timezone.utc),
        collected_at=datetime.now(timezone.utc),
        source="usaspending",
        source_reliability=0.98,
        primary_entity=Entity(id=ticker, name=ticker, type=EntityType.INSTRUMENT),
        headline=(f"Federal contracts: {ticker} obligated ${total / 1e6:,.0f}M across "
                  f"{agg['actions']} action(s) ({', '.join(agencies[:3])})"),
        summary=(f"Largest: ${float(largest.get('amount', 0)) / 1e6:,.0f}M from {largest.get('agency', '')} -- "
                 f"{largest.get('description', '')}. Recipients: {', '.join(sorted(agg['recipients']))[:200]}."),
        anomaly_score=score,
        regulatory_data=RegulatoryData(
            agency=", ".join(agencies[:3]) or "US Government", action_type="contract_award",
            title=f"{ticker} federal contract obligations", document_number=str(largest.get("award_id") or ""),
            publication_date=day, affected_tickers=[ticker],
        ),
        tags=["government_contract", "follow_the_money", f"exposes:{ticker}"],
        named_entities=[ticker],
        region="US",
    )


async def poll_once(session: aiohttp.ClientSession, producer: Any, raw: Any, graph: Any,
                    resolve: Callable[[str], Optional[str]]) -> int:
    end = datetime.now(timezone.utc).date()
    rows = await fetch_transactions(session, end - timedelta(days=LOOKBACK_DAYS), end)
    fresh = []
    for r in rows:
        k = _key(r)
        if await raw.sismember(SEEN_KEY, k):
            continue
        await raw.sadd(SEEN_KEY, k)
        fresh.append(r)
    await raw.expire(SEEN_KEY, 60 * 86400)
    by_ticker = aggregate_awards(fresh, lambda name: parent_ticker(name, resolve))
    emitted = 0
    for ticker, agg in by_ticker.items():
        if float(agg["total"]) < MIN_EVENT_TOTAL_USD:
            continue
        cap_raw = await raw.get(MCAP_KEY.format(ticker=ticker))
        cap = float(cap_raw) if cap_raw else None
        event = contract_event(ticker, agg, end.isoformat(), award_score(float(agg["total"]), cap))
        await producer.send(Topics.RAW_TRADFI, RawEvent(
            source=event.source, occurred_at=event.occurred_at,
            raw_payload={"normalized_event": event.model_dump(mode="json")},
        ).model_dump(), key=ticker)
        emitted += 1
        logger.info("%s", event.headline)
        if graph is not None:
            for agency in agg["agencies"]:
                if agency:
                    await graph.link_entities(
                        ticker, "CONTRACTED_BY", agency.upper()[:80],
                        properties={"amount_usd": float(agg["total"]), "period": end.isoformat(),
                                    "source": "usaspending", "confidence": 0.98},
                        source_label="Company", target_label="Entity",
                    )
    return emitted


async def run_forever(producer: Any, redis_client: Any, resolve: Callable[[str], Optional[str]]) -> None:
    raw = getattr(redis_client, "raw", redis_client)
    graph = None
    try:
        from services.enrichment.graph_writer import GraphWriter
        graph = GraphWriter(producer)
    except Exception as e:
        swallowed("collector_filings.contracts.graph_writer", e, logger)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                await poll_once(session, producer, raw, graph, resolve)
        except Exception as e:
            swallowed("collector_filings.contracts.poll", e, logger)
        await asyncio.sleep(POLL_SEC)

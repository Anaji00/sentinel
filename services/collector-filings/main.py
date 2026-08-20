"""
services/collector-filings/main.py

SEC EDGAR CORPORATE FILINGS & 13F COLLECTOR
===========================================
Polls real-time SEC EDGAR disclosures for equities on the active watchlist
and prominent institutional asset managers:
- 8-K: Material Corporate Disclosures (M&A, Executive departures, Contract entries)
- 13F-HR: Institutional Portfolio Holdings & QoQ Position Changes
- S-1 / 424B: Offerings, Dilution, and Prospectuses
- 10-K / 10-Q: Annual and Quarterly Financial Statements

Publishes structured RawEvents to `Topics.RAW_FILINGS` (`events.raw.filings`).
Maintains a 15-second universal heartbeat for system health monitoring.
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

import aiohttp
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.utils.logging import setup_sentinel_logging
logger = setup_sentinel_logging("collector.filings", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis
from shared.utils.heartbeat import start_heartbeat_task

try:
    from thirteen_f import (
        PROMINENT_FILERS,
        fetch_live_13f_from_edgar,
        generate_curated_seed_13f,
        ThirteenFPortfolioReport,
    )
except (ImportError, ModuleNotFoundError):
    import importlib.util
    tf_path = Path(__file__).resolve().parent / "thirteen_f.py"
    spec_tf = importlib.util.spec_from_file_location("thirteen_f", tf_path)
    thirteen_f_mod = importlib.util.module_from_spec(spec_tf)
    spec_tf.loader.exec_module(thirteen_f_mod)
    PROMINENT_FILERS = thirteen_f_mod.PROMINENT_FILERS
    fetch_live_13f_from_edgar = thirteen_f_mod.fetch_live_13f_from_edgar
    generate_curated_seed_13f = thirteen_f_mod.generate_curated_seed_13f
    ThirteenFPortfolioReport = thirteen_f_mod.ThirteenFPortfolioReport

SEC_HEADERS = {
    "User-Agent": "Sentinel-Intelligence-Platform/1.0 research@sentinel.local",
    "Accept-Encoding": "gzip, deflate",
}

POLL_INTERVAL_SEC = 90

# Standard fallback mapping for top watchable equities
BASE_CIK_MAP = {
    "AAPL": "0000320193",
    "MSFT": "0000789019",
    "NVDA": "0001045810",
    "AMZN": "0001018724",
    "GOOGL": "0001652044",
    "META": "0001326801",
    "TSLA": "0001318605",
    "AVGO": "0001730168",
    "AMD": "0000002488",
    "INTC": "0000050863",
    "QCOM": "0000804328",
    "TSM": "0001046179",
    "ASML": "0000937966",
    "XOM": "0000034088",
    "CVX": "0000093410",
    "JPM": "0000019617",
    "BAC": "0000070858",
    "GS": "0000886982",
    "MS": "0000895421",
    "PLTR": "0001321655",
}

# 8-K Form Item Code Descriptions
ITEM_DESCRIPTIONS = {
    "1.01": "Entry into a Material Definitive Agreement",
    "1.02": "Termination of a Material Definitive Agreement",
    "1.03": "Bankruptcy or Receivership",
    "2.01": "Completion of Acquisition or Disposition of Assets",
    "2.02": "Results of Operations and Financial Condition (Earnings)",
    "2.03": "Creation of a Direct Financial Obligation",
    "3.01": "Notice of Delisting or Failure to Satisfy Listing Rule",
    "3.02": "Unregistered Sales of Equity Securities (Dilution)",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "5.01": "Changes in Control of Registrant",
    "5.02": "Departure of Directors or Principal Officers; Election of Directors",
    "5.03": "Amendments to Articles of Incorporation or Bylaws",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events (Material Corporate Disclosure)",
}


class FilingDeduplicator:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.key = "sentinel:seen:filings"

    async def is_seen(self, accession_number: str) -> bool:
        if not self.redis:
            return False
        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            score = await raw_redis.zscore(self.key, accession_number)
            return score is not None
        except Exception:
            return False

    async def mark_seen(self, accession_number: str):
        if not self.redis:
            return
        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            now = time.time()
            pipe = raw_redis.pipeline()
            pipe.zadd(self.key, {accession_number: now})
            cutoff = now - (30 * 86400)
            pipe.zremrangebyscore(self.key, 0, cutoff)
            await pipe.execute()
        except Exception as e:
            logger.debug(f"Filing dedup error: {e}")


async def get_cik_for_ticker(ticker: str, redis_client) -> Optional[str]:
    """Resolves ticker to SEC CIK with Redis caching."""
    t_clean = ticker.upper().strip()
    if t_clean in BASE_CIK_MAP:
        return BASE_CIK_MAP[t_clean]

    if redis_client:
        try:
            raw_redis = getattr(redis_client, "raw", redis_client)
            cached_cik = await raw_redis.hget("sentinel:sec:cik_map", t_clean)
            if cached_cik:
                return cached_cik.decode("utf-8") if isinstance(cached_cik, bytes) else str(cached_cik)
        except Exception:
            pass

    return None


async def poll_company_filings(
    session: aiohttp.ClientSession,
    producer: SentinelProducer,
    dedup: FilingDeduplicator,
    ticker: str,
    cik: str,
) -> int:
    """Polls recent SEC EDGAR submissions for a specific CIK."""
    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    new_count = 0

    try:
        async with session.get(url, headers=SEC_HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return 0
            data = await resp.json()

        company_name = data.get("name", ticker)
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        report_dates = recent.get("reportDate", [])
        primary_docs = recent.get("primaryDocument", [])
        items_list = recent.get("items", [])

        # Evaluate last 15 filings
        count = min(15, len(forms))
        for i in range(count):
            form = forms[i]
            acc_num = accessions[i]
            if not acc_num or await dedup.is_seen(acc_num):
                continue

            # We focus on 8-K, 10-K, 10-Q, S-1, S-3, 424B, 13F
            if not (form.startswith("8-K") or form.startswith("10-") or form.startswith("S-") or "424B" in form or "13F" in form):
                continue

            await dedup.mark_seen(acc_num)

            f_date = filing_dates[i] if i < len(filing_dates) else datetime.now(timezone.utc).strftime("%Y-%m-%d")
            r_date = report_dates[i] if i < len(report_dates) else f_date
            p_doc = primary_docs[i] if i < len(primary_docs) else ""
            raw_items = items_list[i] if i < len(items_list) else ""
            
            # Format primary document link
            acc_no_hyphen = acc_num.replace("-", "")
            doc_url = f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{int(cik)}/{acc_no_hyphen}/{p_doc}" if p_doc else f"https://www.sec.gov/edgar/browse/?CIK={cik}"

            is_8k = form.startswith("8-K")
            parsed_items = [item.strip() for item in str(raw_items).split(",") if item.strip()]
            item_labels = [f"{it}: {ITEM_DESCRIPTIONS.get(it, 'Corporate Disclosure')}" for it in parsed_items]

            title = f"SEC Filing: {company_name} ({ticker}) filed Form {form}"
            if is_8k and item_labels:
                summary = f"Material 8-K Event for {ticker}: {'; '.join(item_labels)}. Filed on {f_date}."
            else:
                summary = f"Periodic SEC filing Form {form} for {company_name} ({ticker}) on {f_date}."

            event = RawEvent(
                source="sec_edgar",
                occurred_at=datetime.now(timezone.utc),
                raw_payload={
                    "ticker": ticker,
                    "cik": cik,
                    "company_name": company_name,
                    "form_type": form,
                    "filing_date": f_date,
                    "report_date": r_date,
                    "accession_number": acc_num,
                    "items": parsed_items,
                    "item_descriptions": item_labels,
                    "primary_doc_url": doc_url,
                    "is_material_8k": is_8k,
                    "source_type": "primary_filing",
                    "reliability": 0.99,
                    "title": title,
                    "summary": summary,
                    "tags": ["filing", "sec_edgar", f"form:{form}", f"ticker:{ticker}", f"cik:{cik}"],
                }
            )

            await producer.send(Topics.RAW_FILINGS, event.model_dump(), key=ticker)
            new_count += 1

    except Exception as e:
        logger.debug(f"EDGAR polling notice for {ticker}: {e}")

    return new_count


async def seed_prominent_13f_reports(session: aiohttp.ClientSession, producer: SentinelProducer, redis_client: Any):
    """
    Dynamically fetches and parses official SEC EDGAR 13F-HR filings for prominent institutional managers.
    Stores verified live holdings in Redis and emits events to Topics.RAW_FILINGS.
    """
    if not redis_client:
        return

    raw_redis = getattr(redis_client, "raw", redis_client)
    for cik, meta in PROMINENT_FILERS.items():
        try:
            report = None
            if session:
                report = await fetch_live_13f_from_edgar(session, cik, meta)

            if not report:
                # Disclose that live EDGAR is currently unreachable, do NOT publish fabricated live data
                logger.info(f"Live SEC EDGAR 13F data currently unavailable for {meta['name']} (CIK {cik}).")
                continue

            report_json = report.model_dump_json()

            # Store in Redis: latest report and list of prominent filers
            await raw_redis.set(f"sentinel:13f:{cik}:latest", report_json, ex=86400 * 30)
            await raw_redis.sadd("sentinel:13f:prominent_ciks", cik)

            # Update consensus accumulator for top holdings
            for h in report.top_holdings:
                if h.ticker:
                    await raw_redis.sadd(f"sentinel:13f:consensus:{h.ticker}:buyers", meta["manager"])

            # Publish verified live event to RAW_FILINGS
            event = RawEvent(
                source="sec_edgar_13f",
                occurred_at=datetime.now(timezone.utc),
                raw_payload={
                    "filer_id": report.filer_id,
                    "filer_name": report.filer_name,
                    "manager_name": report.manager_name,
                    "cik": report.cik,
                    "report_period": report.report_period,
                    "total_value_usd": report.total_portfolio_value_usd,
                    "positions_count": report.total_positions_count,
                    "top_holdings": [p.model_dump() for p in report.top_holdings],
                    "form_type": "13F-HR",
                    "source_type": report.source_type,
                    "is_synthetic": report.is_synthetic,
                    "reliability": 0.99 if not report.is_synthetic else 0.0,
                    "tags": ["filing", "13f", f"filer:{report.filer_id}", "institutional_flow"],
                }
            )
            await producer.send(Topics.RAW_FILINGS, event.model_dump(), key=report.filer_id)
        except Exception as e:
            logger.warning(f"Error fetching live 13F for {meta['name']}: {e}")


async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL SEC EDGAR CORPORATE FILINGS & 13F COLLECTOR")
    logger.info(f"Prominent Institutional Filers: {len(PROMINENT_FILERS)}")
    logger.info("=" * 60)

    producer = SentinelProducer()
    await producer.start()

    redis_client = await get_redis()
    dedup = FilingDeduplicator(redis_client)

    # Universal 15s heartbeat
    hb_task = asyncio.create_task(start_heartbeat_task(redis_client, "collector-filings"))

    connector = aiohttp.TCPConnector(limit=10)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            # Fetch live 13F filings from SEC EDGAR on startup
            await seed_prominent_13f_reports(session, producer, redis_client)

            cycle = 0
            while True:
                cycle += 1
                t0 = time.time()

                # 1. Fetch watched equities dynamically from Redis
                tickers = list(BASE_CIK_MAP.keys())
                if redis_client:
                    try:
                        raw_redis = getattr(redis_client, "raw", redis_client)
                        dynamic_tickers = await raw_redis.zrange("sentinel:watched:equities", 0, 50)
                        for t in dynamic_tickers:
                            sym = t.decode("utf-8") if isinstance(t, bytes) else str(t)
                            if sym not in tickers:
                                tickers.append(sym)
                    except Exception:
                        pass

                tasks = []
                for ticker in tickers:
                    cik = await get_cik_for_ticker(ticker, redis_client)
                    if cik:
                        tasks.append(poll_company_filings(session, producer, dedup, ticker, cik))

                results = await asyncio.gather(*tasks, return_exceptions=True)
                new_filings = sum(r for r in results if isinstance(r, int))
                elapsed = time.time() - t0
                logger.info(f"EDGAR Poll Cycle #{cycle}: Ingested {new_filings} filings across {len(tasks)} companies in {elapsed:.1f}s")

                await asyncio.sleep(POLL_INTERVAL_SEC)
    finally:
        hb_task.cancel()
        await producer.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

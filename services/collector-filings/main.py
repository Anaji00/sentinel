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
from shared.utils.collector_metrics import CollectorMetrics
from shared.utils.tasks import safe_create_task

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

# SEC fair-access requires a User-Agent that identifies the requester with a
# contact address they actually read. research@sentinel.local is not
# deliverable, and EDGAR blocks by IP for repeated anonymous traffic -- so this
# is configurable, and says so when it is still the placeholder.
SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local"
)
SEC_HEADERS = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
}

POLL_INTERVAL_SEC = 90

# Beyond this, a 13F cannot be the current one.
#
# Form 13F is due 45 days after the quarter it covers, so at any moment the
# newest filing in existence is at most a quarter plus that filing window old.
# Two quarters of slack absorbs late filers and amended reports; past it, the
# filing is not late, the filer has stopped.
MAX_13F_AGE_DAYS = 225


def _report_age_days(report_period: str):
    """Days since the quarter a filing covers, or None if the period is unparseable.

    Unparseable returns None rather than a large number: an unreadable period
    is a reason to look, not a reason to silently drop a live filing.
    """
    if not report_period:
        return None
    text = str(report_period).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m"):
        try:
            when = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - when).days
        except ValueError:
            continue
    return None



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


# CIK -> ticker. The forward map exists because this collector starts from a
# ticker; the Form 4 enricher starts from an EDGAR title, gets a CIK out of it,
# and had nothing to resolve it with -- which is why it fell through to matching
# the parenthesised role label and filed 497 events under CHUCK, FILER, ISSUER,
# REPORTING and SUBJECT.
CIK_TO_TICKER_KEY = "sentinel:sec:ticker_by_cik"


async def _remember_cik(redis_client, ticker: str, cik: str) -> None:
    """Record both directions of a resolution, so either end can start."""
    if not (redis_client and ticker and cik):
        return
    try:
        raw_redis = getattr(redis_client, "raw", redis_client)
        t = ticker.upper().strip()
        c = str(cik).strip()
        await raw_redis.hset("sentinel:sec:cik_map", t, c)
        # Both the padded and unpadded forms, because EDGAR prints the padded
        # one in titles and the API takes either.
        await raw_redis.hset(CIK_TO_TICKER_KEY, c.lstrip("0") or "0", t)
        await raw_redis.hset(CIK_TO_TICKER_KEY, c.zfill(10), t)
    except Exception as e:
        logger.debug(f"CIK map write skipped for {ticker}: {e}")


async def get_cik_for_ticker(ticker: str, redis_client) -> Optional[str]:
    """Resolves ticker to SEC CIK with Redis caching."""
    t_clean = ticker.upper().strip()
    if t_clean in BASE_CIK_MAP:
        # The hardcoded table is the seed for the reverse map: it is the only
        # resolution available before the first poll completes.
        await _remember_cik(redis_client, t_clean, BASE_CIK_MAP[t_clean])
        return BASE_CIK_MAP[t_clean]

    if redis_client:
        try:
            raw_redis = getattr(redis_client, "raw", redis_client)
            cached_cik = await raw_redis.hget("sentinel:sec:cik_map", t_clean)
            if cached_cik:
                cik = cached_cik.decode("utf-8") if isinstance(cached_cik, bytes) else str(cached_cik)
                await _remember_cik(redis_client, t_clean, cik)
                return cik
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

            # A filing this old is not news, and may mean the filer is gone.
            #
            # Live on 4 September 2026, Appaloosa was publishing a portfolio
            # dated 2015-12-31 -- ten years stale, emitted as a current 13F
            # event, indistinguishable in the stream from Berkshire's
            # 2026-06-30. A 13F is due 45 days after quarter end, so the newest
            # available period is never more than about five months old; a
            # decade-old one means EDGAR returned that filer's LAST filing and
            # there have been none since.
            #
            # That is what a dissolved or deregistered manager looks like from
            # here: not an error, not an empty response, but the final filing
            # served forever as though it were current. Scion Asset Management
            # is the case that prompted this check -- a fund that stopped
            # filing does not announce it, and a collector that only ever asks
            # for "the latest" cannot tell the difference between a manager who
            # has not filed yet this quarter and one who will never file again.
            staleness_days = _report_age_days(report.report_period)
            if staleness_days is not None and staleness_days > MAX_13F_AGE_DAYS:
                logger.warning(
                    "%s (CIK %s) last filed for %s, %d days ago -- beyond the "
                    "%d-day window in which a 13F can still be the current one. "
                    "Not published: this filer appears to have stopped filing. "
                    "Its last known portfolio remains in Redis for reference.",
                    meta["name"], cik, report.report_period,
                    staleness_days, MAX_13F_AGE_DAYS,
                )
                await raw_redis.set(
                    f"sentinel:13f:{cik}:dormant",
                    json.dumps({"last_period": report.report_period,
                                "age_days": staleness_days,
                                "observed_at": datetime.now(timezone.utc).isoformat()}),
                    ex=86400 * 30,
                )
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
    if "sentinel.local" in SEC_USER_AGENT:
        # Said once at startup rather than buried. EDGAR throttles and then
        # blocks by IP, and the block arrives as empty responses rather than
        # an error, which would look exactly like a quiet filing day.
        logger.warning(
            "SEC_USER_AGENT is still the placeholder (%s). SEC fair-access "
            "expects a contact address that is read; set SEC_USER_AGENT in .env "
            "to avoid being throttled or blocked.", SEC_USER_AGENT,
        )
    logger.info("=" * 60)

    producer = SentinelProducer(service_name="collector-filings")
    await producer.start()

    redis_client = await get_redis()
    dedup = FilingDeduplicator(redis_client)

    # Universal 15s heartbeat
    # Throughput counters. The heartbeat proves this process is alive;
    # these prove it is still producing.
    metrics = CollectorMetrics("collector-filings")
    await metrics.start(redis_client)
    hb_task = safe_create_task(start_heartbeat_task(redis_client, "collector-filings"))

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

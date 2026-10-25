"""
services/collector-filings/ownership_history.py

Five years of institutional flows, so the reaction study can judge them.

The live loop (ownership.py) compares each tracked manager's latest 13F with
the one before, and publishes an accumulation or distribution when three or
more of the six active managers in FLOW_FILERS move the same way
(shared/utils/ownership.py). It has only seen this autumn's filings, so both
families sat at "insufficient data". Here every original 13F-HR those six
filed in the last HISTORY_YEARS is read once (holdings cached in Redis, so a
restart resumes), consecutive quarters are compared the same way, and each
flow goes into the study's archive stamped at the end of the day the last of
the six filed for that quarter: the earliest moment every vote was public.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import tempfile
import uuid
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp

_PARENTS = Path(__file__).resolve().parents
ROOT = _PARENTS[2] if len(_PARENTS) > 2 else _PARENTS[-1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils.ownership import (  # noqa: E402
    FLOW_FILERS, OWNERSHIP_FILERS, aggregate_by_cusip, aggregate_changes, holding_changes, iter_infotable,
)
from shared.utils.quiet_failures import swallowed  # noqa: E402
from shared.utils.sec_rate import sec_slot  # noqa: E402

logger = logging.getLogger("collector.filings.ownership_history")

SEC_HEADERS = {"User-Agent": os.getenv("SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local")}
HISTORY_YEARS = 5
HOLDINGS_KEY = "sentinel:ownership:history:{cik}:{period}"     # one filing's {ticker: shares}
DONE_KEY = "sentinel:ownership:history:done"
NY = ZoneInfo("America/New_York")
_ARCHIVE_SQL = """
    INSERT INTO signal_event_archive (event_id, type, source, created_at, occurred_at, headline, summary, tags,
                                      sentiment, primary_entity_id, financial_data, region)
    VALUES ($1, 'thirteen_f', 'ownership_changes', $2, $2, $3, $4, $5, NULL, $6, $7::jsonb, NULL)
    ON CONFLICT DO NOTHING
"""


async def _filings(sec: aiohttp.ClientSession, cik: str, since: str) -> List[Dict[str, str]]:
    """Every original 13F-HR with a period on or after `since`, from the
    submissions index and its older pages."""
    out: List[Dict[str, str]] = []
    await sec_slot()
    async with sec.get(f"https://data.sec.gov/submissions/CIK{cik}.json", timeout=aiohttp.ClientTimeout(total=60)) as r:
        r.raise_for_status()
        data = await r.json()
    pages = [data.get("filings", {}).get("recent", {})]
    for f in data.get("filings", {}).get("files", []) or []:
        if str(f.get("filingTo", "9999")) < since:
            continue
        await sec_slot()
        async with sec.get(f"https://data.sec.gov/submissions/{f['name']}", timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status == 200:
                pages.append(await r.json())
    for rec in pages:
        for i, form in enumerate(rec.get("form", [])):
            period = rec["reportDate"][i]
            if form == "13F-HR" and period and period >= since:
                out.append({"accession": rec["accessionNumber"][i], "period": period, "filed": rec["filingDate"][i]})
    latest: Dict[str, Dict[str, str]] = {}
    for f in sorted(out, key=lambda f: f["filed"]):
        latest.setdefault(f["period"], f)          # the first original filed for each period
    return sorted(latest.values(), key=lambda f: f["period"])


async def _holdings(session: aiohttp.ClientSession, sec: aiohttp.ClientSession, raw: Any, cik: str,
                    filing: Dict[str, str], resolve: Callable[[str, Optional[str]], Optional[str]],
                    cusip_cache: Dict[str, Optional[str]]) -> Optional[Dict[str, float]]:
    """{ticker: shares} for one filing, cached for good once read."""
    import ownership as live

    key = HOLDINGS_KEY.format(cik=cik, period=filing["period"])
    cached = await raw.get(key)
    if cached:
        return json.loads(cached)
    url = await live._infotable_url(sec, cik, filing["accession"])
    await sec_slot()
    if not url:
        return None
    fd, path = tempfile.mkstemp(suffix=".xml")
    os.close(fd)
    try:
        await sec_slot()
        await live._stream_to_file(sec, url, path)
        with open(path, "rb") as fh:
            positions = aggregate_by_cusip(iter_infotable(fh))
    finally:
        os.unlink(path)
    await live.resolve_cusips(session, raw, positions, resolve, cusip_cache)
    shares: Dict[str, float] = {}
    for cusip, pos in positions.items():
        t = (cusip_cache.get(cusip) or "").upper()
        if t:
            shares[t] = shares.get(t, 0.0) + float(pos["shares"] or 0)
    await raw.set(key, json.dumps(shares))
    return shares


def flows(per_period: Dict[str, Dict[str, Tuple[str, Dict[str, float]]]]) -> List[Dict[str, Any]]:
    """Flows per period from {period: {filer name: (filed, {ticker: shares})}}:
    each period compared with the filer's previous one, aggregated across
    filers exactly as the live loop does, stamped when the last filed."""
    periods = sorted(per_period)
    out: List[Dict[str, Any]] = []
    for prev_p, p in zip(periods, periods[1:]):
        changes, filed = {}, []
        for name, (fdate, curr) in per_period[p].items():
            prev = per_period[prev_p].get(name)
            if prev is None:
                continue
            changes[name] = holding_changes([{"ticker": t, "shares": s} for t, s in prev[1].items()],
                                            [{"ticker": t, "shares": s} for t, s in curr.items()])
            filed.append(fdate)
        if not filed:
            continue
        last = max(filed)
        for ticker, row in aggregate_changes(changes).items():
            if row["flow"]:
                out.append({"period": p, "ticker": ticker, "flow": row["flow"], "known": last,
                            "names": row["buyers"] if row["flow"] > 0 else row["sellers"]})
    return out


def _known_at(filed: str) -> datetime:
    d = date.fromisoformat(filed) + timedelta(days=1)
    return datetime.combine(d, time(0, 0), tzinfo=NY).astimezone(timezone.utc)


async def run_once(resolve: Callable[[str, Optional[str]], Optional[str]]) -> int:
    raw = getattr(await get_redis(), "raw", None)
    if await raw.get(DONE_KEY):
        return 0
    since = (date.today() - timedelta(days=365 * HISTORY_YEARS + 120)).isoformat()
    per_period: Dict[str, Dict[str, Tuple[str, Dict[str, float]]]] = {}
    cusip_cache: Dict[str, Optional[str]] = {}
    async with aiohttp.ClientSession() as session, aiohttp.ClientSession(headers=SEC_HEADERS) as sec:
        for cik, filer in OWNERSHIP_FILERS.items():
            if filer.name not in FLOW_FILERS:
                continue
            for f in await _filings(sec, cik, since):
                try:
                    h = await _holdings(session, sec, raw, cik, f, resolve, cusip_cache)
                except Exception as e:
                    swallowed("collector_filings.ownership_history.filing", e, logger, variant=filer.filer_id)
                    continue
                if h is not None:
                    per_period.setdefault(f["period"], {})[filer.name] = (f["filed"], h)
            logger.info("Ownership history: %s, %s quarters", filer.name,
                        sum(1 for v in per_period.values() if filer.name in v))
    rows = []
    for fl in flows(per_period):
        kind = "institutional_accumulation" if fl["flow"] > 0 else "institutional_distribution"
        verb = "added to or opened" if fl["flow"] > 0 else "cut or exited"
        rows.append((str(uuid.uuid5(uuid.NAMESPACE_URL, f"13f-flow:{fl['period']}:{fl['ticker']}")), _known_at(fl["known"]),
                     f"{len(fl['names'])} tracked institutions {verb} {fl['ticker']} in the quarter to {fl['period']}",
                     ", ".join(fl["names"]) + ". 13F long positions as filed.",
                     [kind, f"exposes:{fl['ticker']}", f"period:{fl['period']}"], fl["ticker"], {"ticker": fl["ticker"]}))
    db = await get_timescale()
    for i in range(0, len(rows), 1000):
        await db.execute_many(_ARCHIVE_SQL, rows[i:i + 1000])
    await raw.set(DONE_KEY, datetime.now(timezone.utc).isoformat())
    logger.info("Ownership history: %s institutional flows archived over %s quarters", len(rows), len(per_period))
    return len(rows)


async def run_forever(resolve: Callable[[str, Optional[str]], Optional[str]]) -> None:
    await asyncio.sleep(15 * 60)
    try:
        await run_once(resolve)
    except Exception as e:
        swallowed("collector_filings.ownership_history", e, logger)


__all__ = ["flows", "run_forever", "run_once"]

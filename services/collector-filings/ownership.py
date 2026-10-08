"""
services/collector-filings/ownership.py

The ownership graph from 13F: which institution holds what share of which
company, and which institutions hold each other.

For each filer in shared.utils.ownership.OWNERSHIP_FILERS, the latest 13F-HR's
information table is streamed to disk and parsed incrementally (BlackRock's is
23 MB). Positions are kept when the issuer resolves to a company the platform
quotes, or to one of the listed filers themselves -- that second set is what
makes cross-ownership visible. Each kept position is sized as a share of the
company's shares outstanding, stored for the API and the reasoning tier, and
proposed to the graph as InstitutionalFiler -[HOLDS_STAKE_IN]-> Company.

A filer whose latest accession has already been processed is skipped, so the
daily loop costs one submissions request per filer between quarters.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import Counter
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import aiohttp

_PARENTS = Path(__file__).resolve().parents
ROOT = _PARENTS[2] if len(_PARENTS) > 2 else _PARENTS[-1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.ownership import (  # noqa: E402
    FILER_KEY, HOLDERS_KEY, MCAP_KEY, OWNERSHIP_FILERS, aggregate_by_cusip, iter_infotable,
    DEFENSE_PRIMES, OSINT_SECTORS, stake_pct, value_scale,
    CHANGES_KEY, aggregate_changes, holding_changes,
)
from shared.kafka import Topics  # noqa: E402
from shared.models import NormalizedEvent, RawEvent  # noqa: E402
from shared.utils.quiet_failures import dropped, swallowed  # noqa: E402
from shared.utils.quote_cache import quote_key  # noqa: E402

logger = logging.getLogger("collector.filings.ownership")

SEC_HEADERS = {"User-Agent": os.getenv("SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local")}
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
REFRESH_SEC = 86400
SEC_PAUSE_SEC = 0.25          # SEC asks for no more than ten requests a second
FINNHUB_PAUSE_SEC = 1.1       # free tier: sixty a minute
SHARES_OUT_TTL_SEC = 7 * 86400

ACCESSION_KEY = "sentinel:ownership:filer:{cik}:accession"
SHARES_OUT_KEY = "sentinel:shares_out:{ticker}"
CUSIP_KEY = "sentinel:cusip:ticker"       # hash: cusip -> ticker, or "-" for none

# OpenFIGI without a key: 25 requests a minute, 10 identifiers a request.
FIGI_URL = "https://api.openfigi.com/v3/mapping"
FIGI_BATCH = 10
FIGI_PAUSE_SEC = 2.6
FIGI_TOP_N = 600   # largest unresolved positions per filer sent for mapping
_PLAIN_TICKER = re.compile(r"^[A-Z]{1,5}(-[A-Z])?$")


async def _latest_13f(session: aiohttp.ClientSession, cik: str) -> Optional[Dict[str, str]]:
    async with session.get(f"https://data.sec.gov/submissions/CIK{cik}.json",
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
        if r.status != 200:
            return None
        data = await r.json()
    rec = data.get("filings", {}).get("recent", {})
    for i, form in enumerate(rec.get("form", [])):
        if form in ("13F-HR", "13F-HR/A"):
            return {"accession": rec["accessionNumber"][i], "period": rec["reportDate"][i],
                    "filed": rec["filingDate"][i]}
    return None


async def _previous_13f(session: aiohttp.ClientSession, cik: str, before_period: str) -> Optional[Dict[str, str]]:
    """The newest original 13F-HR for a period earlier than `before_period`."""
    async with session.get(f"https://data.sec.gov/submissions/CIK{cik}.json",
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
        if r.status != 200:
            return None
        data = await r.json()
    rec = data.get("filings", {}).get("recent", {})
    for i, form in enumerate(rec.get("form", [])):
        if form == "13F-HR" and rec["reportDate"][i] < before_period:
            return {"accession": rec["accessionNumber"][i], "period": rec["reportDate"][i],
                    "filed": rec["filingDate"][i]}
    return None


async def _infotable_url(session: aiohttp.ClientSession, cik: str, accession: str) -> Optional[str]:
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}"
    async with session.get(f"{base}/index.json", timeout=aiohttp.ClientTimeout(total=60)) as r:
        if r.status != 200:
            return None
        items = (await r.json()).get("directory", {}).get("item", [])
    xmls = [it for it in items if it.get("name", "").endswith(".xml") and it["name"] != "primary_doc.xml"]
    if not xmls:
        return None
    biggest = max(xmls, key=lambda it: int(it.get("size") or 0))
    return f"{base}/{biggest['name']}"


async def _stream_to_file(session: aiohttp.ClientSession, url: str, path: str) -> int:
    size = 0
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=600)) as r:
        r.raise_for_status()
        with open(path, "wb") as fh:
            async for chunk in r.content.iter_chunked(1 << 16):
                fh.write(chunk)
                size += len(chunk)
    return size


async def _company_size(session: aiohttp.ClientSession, raw: Any, ticker: str):
    """(shares outstanding, market cap in USD) from Finnhub's profile, cached a week."""
    s_key, m_key = SHARES_OUT_KEY.format(ticker=ticker), MCAP_KEY.format(ticker=ticker)
    s_cached, m_cached = await raw.get(s_key), await raw.get(m_key)
    if s_cached is not None and m_cached is not None:
        so, mc = float(s_cached), float(m_cached)
        return (so if so > 0 else None), (mc if mc > 0 else None)
    if not FINNHUB_API_KEY:
        return None, None
    await asyncio.sleep(FINNHUB_PAUSE_SEC)
    try:
        async with session.get("https://finnhub.io/api/v1/stock/profile2",
                               params={"symbol": ticker.replace("-", "."), "token": FINNHUB_API_KEY},
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status != 200:
                return None, None
            prof = await r.json()
    except Exception as e:
        swallowed("collector_filings.ownership.company_size", e, logger, variant=ticker)
        return None, None
    # Finnhub reports market cap in the listing's currency; a non-USD listing's
    # cap is not comparable with a 13F's dollar value.
    usd = str(prof.get("currency") or "USD").upper() == "USD"
    so = float(prof.get("shareOutstanding") or 0.0) * 1_000_000
    mc = float(prof.get("marketCapitalization") or 0.0) * 1_000_000 if usd else 0.0
    await raw.set(s_key, str(so), ex=SHARES_OUT_TTL_SEC)
    await raw.set(m_key, str(mc), ex=SHARES_OUT_TTL_SEC)
    return (so if so > 0 else None), (mc if mc > 0 else None)


def _figi_pick(data: List[Dict[str, Any]]) -> str:
    """The US common-stock ticker among OpenFIGI's results, or "-".

    Unfiltered results list every venue (XOM, XOM_KZ, XOMCHF, EXMOC for Exxon),
    so the most frequent plain ticker wins. An ETF or fund is "-": a fund's
    position is not a stake in a company.
    """
    tickers = [str(d.get("ticker") or "").upper() for d in data
               if d.get("securityType") in ("Common Stock", "ADR", "REIT")]
    plain = [t.replace(".", "-").replace("/", "-") for t in tickers]
    plain = [t for t in plain if _PLAIN_TICKER.match(t)]
    return Counter(plain).most_common(1)[0][0] if plain else "-"


async def _figi_tickers(session: aiohttp.ClientSession, raw: Any, cusips: List[str]) -> Dict[str, str]:
    """CUSIP -> ticker through OpenFIGI, cached forever in Redis.

    13F names are abbreviated -- "BANK OF AMER CORP", "DISNEY WALT CO" -- and the
    name resolver missed 100 of BlackRock's 400 largest positions, Exxon and
    Bank of America among them. A CUSIP is exact.
    """
    out: Dict[str, str] = {}
    todo = []
    for c in cusips:
        hit = await raw.hget(CUSIP_KEY, c)
        if hit is not None:
            out[c] = hit.decode() if isinstance(hit, bytes) else hit
        else:
            todo.append(c)
    async def _map(cusips_: List[str], exch: Optional[str]) -> Dict[str, str]:
        jobs = [{"idType": "ID_CUSIP", "idValue": c, **({"exchCode": exch} if exch else {})} for c in cusips_]
        for _ in range(2):
            try:
                async with session.post(FIGI_URL, json=jobs, timeout=aiohttp.ClientTimeout(total=60)) as r:
                    if r.status == 429:
                        await asyncio.sleep(60)
                        continue
                    results = await r.json() if r.status == 200 else []
            except Exception as e:
                swallowed("collector_filings.ownership.openfigi", e, logger)
                results = []
            await asyncio.sleep(FIGI_PAUSE_SEC)
            return {c: (_figi_pick(res.get("data") or []) if isinstance(res, dict) else "-")
                    for c, res in zip(cusips_, results or [])}
        return {}

    for i in range(0, len(todo), FIGI_BATCH):
        batch = todo[i:i + FIGI_BATCH]
        out.update({c: t for c, t in (await _map(batch, "US")).items() if t != "-"})
        rest = [c for c in batch if c not in out]
        if rest:
            out.update(await _map(rest, None))
        # Only answers are cached. A request that failed says nothing about the
        # CUSIP, and caching "-" for it would hide it until the cache is cleared.
        for c in batch:
            if c in out:
                await raw.hset(CUSIP_KEY, c, out[c])
    return out


async def _universe(raw: Any) -> set:
    """Tickers worth an ownership record.

    The ones the platform quotes, every listed filer, and every company an OSINT
    bridge can reach -- the airlines, the crypto-linked equities, the semis, and
    anything with a commodity, region or operator edge in the graph. Following
    the money from a tanker in Hormuz ends at the holders of the companies
    exposed to crude; Lockheed and Northrop were absent on the first run
    because nothing quoted them.
    """
    tickers = {f.listed_as for f in OWNERSHIP_FILERS.values() if f.listed_as}
    tickers.update(DEFENSE_PRIMES)
    async for k in raw.scan_iter(match=quote_key("*"), count=1000):
        key = k.decode() if isinstance(k, bytes) else k
        tickers.add(key.rsplit(":", 1)[-1].upper())
    try:
        from shared.utils.airlines import AIRLINE_TICKERS
        from shared.utils.osint_bridges import CRYPTO_EQUITIES, SEMIS
        tickers.update(t for t in AIRLINE_TICKERS.values() if t)
        tickers.update(t for ts in CRYPTO_EQUITIES.values() for t in ts)
        tickers.update(t for t in SEMIS if "=" not in t)
    except Exception as e:
        swallowed("collector_filings.ownership.bridge_universe", e, logger)
    try:
        from shared.db import get_neo4j
        neo = await get_neo4j()
        rows = await neo.query(
            "MATCH (c:Company) WHERE c.sector IN $sectors "
            "OR (c)-[:POSITIVE_EXPOSURE_TO|INVERSE_EXPOSURE_TO|HAS_EXPOSURE_IN|OPERATED_BY]-() "
            "RETURN DISTINCT c.id AS t",
            {"sectors": list(OSINT_SECTORS)},
        )
        tickers.update(str(r["t"]).upper() for r in rows or [] if r.get("t"))
    except Exception as e:
        swallowed("collector_filings.ownership.graph_universe", e, logger)
    return {t for t in tickers if t}


async def _load_holdings(
    session: aiohttp.ClientSession, sec: aiohttp.ClientSession, raw: Any, cik: str, accession: str,
    resolve: Callable[[str, Optional[str]], Optional[str]], universe: set,
    cusip_cache: Dict[str, Optional[str]],
):
    """One 13F's information table: (positions, kept holdings, total value, bytes), or None."""
    filer = OWNERSHIP_FILERS[cik]
    url = await _infotable_url(sec, cik, accession)
    await asyncio.sleep(SEC_PAUSE_SEC)
    if not url:
        dropped("collector_filings.ownership.no_infotable", "13F with no information table", logger, variant=filer.filer_id)
        return None

    fd, path = tempfile.mkstemp(suffix=".xml")
    os.close(fd)
    try:
        size = await _stream_to_file(sec, url, path)
        with open(path, "rb") as fh:
            positions = aggregate_by_cusip(iter_infotable(fh))
    finally:
        os.unlink(path)
    scale = value_scale(positions)
    if scale != 1.0:
        for p in positions.values():
            p["value"] = float(p["value"]) * scale

    # Exact identifiers first, names second, and OpenFIGI for the largest
    # positions neither could place.
    for cusip, pos in positions.items():
        if cusip not in cusip_cache:
            known = await raw.hget(CUSIP_KEY, cusip)
            known = known.decode() if isinstance(known, bytes) else known
            cusip_cache[cusip] = None if known in (None, "-") else known
            if cusip_cache[cusip] is None and known is None:
                cusip_cache[cusip] = resolve(str(pos["issuer"]), cusip)
    unresolved = sorted(
        (c for c, p in positions.items() if not cusip_cache.get(c)),
        key=lambda c: -float(positions[c]["value"]),
    )[:FIGI_TOP_N]
    if unresolved:
        # The plain session: the SEC one carries the contact address SEC
        # requires in its User-Agent, which has no business going to OpenFIGI.
        for c, t in (await _figi_tickers(session, raw, unresolved)).items():
            cusip_cache[c] = None if t == "-" else t

    kept: List[Dict[str, Any]] = []
    for cusip, pos in positions.items():
        ticker = (cusip_cache.get(cusip) or "").upper()
        if ticker not in universe:
            continue
        shares_out, mcap = await _company_size(session, raw, ticker)
        kept.append({
            "ticker": ticker, "issuer": pos["issuer"], "cusip": cusip,
            "shares": pos["shares"], "value_usd": pos["value"],
            "pct_outstanding": stake_pct(float(pos["shares"]), float(pos["value"]), shares_out, mcap),
        })

    total_value = sum(float(p["value"]) for p in positions.values()) or 1.0
    for k in kept:
        k["pct_of_portfolio"] = round(100.0 * float(k["value_usd"]) / total_value, 4)
    return positions, kept, total_value, size


async def _record_changes(raw: Any, filer_name: str, period: str, previous: list, current: list) -> None:
    """One filer's quarter-on-quarter changes, filed under the new period."""
    changes = holding_changes(previous, current)
    await raw.hset(CHANGES_KEY.format(period=period), filer_name, json.dumps(changes))
    await raw.expire(CHANGES_KEY.format(period=period), 200 * 86400)


async def ensure_baseline(
    session: aiohttp.ClientSession, sec: aiohttp.ClientSession, raw: Any, cik: str,
    resolve: Callable[[str, Optional[str]], Optional[str]], universe: set,
    cusip_cache: Dict[str, Optional[str]],
) -> bool:
    """For a filer already on its latest period with no comparison yet, fetch the
    previous quarter once and record the changes. True when it did."""
    filer = OWNERSHIP_FILERS[cik]
    blob = await raw.get(FILER_KEY.format(filer_id=filer.filer_id))
    if not blob:
        return False
    current = json.loads(blob)
    period = current.get("period")
    if not period or await raw.hexists(CHANGES_KEY.format(period=period), filer.name):
        return False
    prev = await _previous_13f(sec, cik, period)
    await asyncio.sleep(SEC_PAUSE_SEC)
    if not prev:
        return False
    loaded = await _load_holdings(session, sec, raw, cik, prev["accession"], resolve, universe, cusip_cache)
    if loaded is None:
        return False
    await _record_changes(raw, filer.name, period, loaded[1], current.get("holdings") or [])
    logger.info("Ownership: %s compared %s against %s", filer.name, period, prev["period"])
    return True


async def publish_flows(producer: Any, raw: Any) -> int:
    """Accumulation and distribution across filers, once per ticker per period."""
    published = 0
    async for key in raw.scan_iter(CHANGES_KEY.format(period="*")):
        key = key.decode() if isinstance(key, bytes) else key
        period = key.rsplit(":", 1)[1]
        per_filer = {
            (f.decode() if isinstance(f, bytes) else f): json.loads(v)
            for f, v in (await raw.hgetall(key) or {}).items()
        }
        for ticker, row in aggregate_changes(per_filer).items():
            if not row["flow"]:
                continue
            marker = f"sentinel:ownership:flow_published:{period}:{ticker}"
            if not await raw.set(marker, "1", ex=200 * 86400, nx=True):
                continue
            kind = "institutional_accumulation" if row["flow"] > 0 else "institutional_distribution"
            names = row["buyers"] if row["flow"] > 0 else row["sellers"]
            verb = "added to or opened" if row["flow"] > 0 else "cut or exited"
            event = NormalizedEvent.model_validate({
                "type": "thirteen_f",
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "source": "ownership_changes",
                "primary_entity": {"id": ticker, "type": "company", "name": ticker},
                "headline": f"{len(names)} tracked institutions {verb} {ticker} in the quarter to {period}",
                "summary": ", ".join(names) + ". 13F long positions as filed, 45 days after quarter end.",
                "financial_data": {"ticker": ticker},
                "tags": [kind, f"exposes:{ticker}", f"period:{period}"],
                "anomaly_score": min(0.9, 0.4 + 0.1 * len(names)),
            })
            await producer.send(
                Topics.RAW_TRADFI,
                RawEvent(source="ownership_changes", occurred_at=event.occurred_at,
                         raw_payload={"normalized_event": event.model_dump(mode="json")}).model_dump(mode="json"),
                key=ticker,
            )
            published += 1
    return published


async def refresh_filer(
    session: aiohttp.ClientSession, sec: aiohttp.ClientSession, raw: Any, graph: Any,
    cik: str, resolve: Callable[[str, Optional[str]], Optional[str]],
    universe: set, cusip_cache: Dict[str, Optional[str]],
) -> int:
    filer = OWNERSHIP_FILERS[cik]
    latest = await _latest_13f(sec, cik)
    await asyncio.sleep(SEC_PAUSE_SEC)
    if not latest:
        return 0
    seen = await raw.get(ACCESSION_KEY.format(cik=cik))
    seen = seen.decode() if isinstance(seen, bytes) else seen
    if seen == latest["accession"]:
        return 0
    loaded = await _load_holdings(session, sec, raw, cik, latest["accession"], resolve, universe, cusip_cache)
    if loaded is None:
        return 0
    positions, kept, total_value, size = loaded
    for k in kept:
        k["period"] = latest["period"]

    # The previous quarter's holdings, before they are overwritten, for the
    # quarter-on-quarter comparison.
    prior_blob = await raw.get(FILER_KEY.format(filer_id=filer.filer_id))
    prior = json.loads(prior_blob) if prior_blob else None
    if prior and prior.get("period") and prior["period"] < latest["period"]:
        await _record_changes(raw, filer.name, latest["period"], prior.get("holdings") or [], kept)

    await raw.set(FILER_KEY.format(filer_id=filer.filer_id), json.dumps({
        "filer_id": filer.filer_id, "name": filer.name, "cik": cik,
        "listed_as": filer.listed_as, "listed_via": filer.listed_via,
        "period": latest["period"], "filed": latest["filed"], "accession": latest["accession"],
        "positions_total": len(positions), "positions_kept": len(kept),
        "portfolio_value_usd": total_value, "infotable_bytes": size, "holdings": kept,
    }), ex=120 * 86400)
    for k in kept:
        await raw.hset(HOLDERS_KEY.format(ticker=k["ticker"]), filer.filer_id, json.dumps({
            "filer": filer.name, "shares": k["shares"], "value_usd": k["value_usd"],
            "pct_outstanding": k["pct_outstanding"], "period": k["period"],
        }))
        await raw.expire(HOLDERS_KEY.format(ticker=k["ticker"]), 120 * 86400)
        if graph is not None:
            await graph.link_entities(
                filer.filer_id, "HOLDS_STAKE_IN", k["ticker"],
                properties={"shares": k["shares"], "value_usd": k["value_usd"],
                            "pct_outstanding": k["pct_outstanding"], "period": k["period"],
                            "source": "13F", "confidence": 0.99},
                source_label="InstitutionalFiler", target_label="Company",
            )
    if graph is not None and filer.listed_as:
        await graph.link_entities(
            filer.filer_id, "LISTED_AS", filer.listed_as,
            properties={"via": filer.listed_via, "source": "edgar_submissions", "confidence": 0.99},
            source_label="InstitutionalFiler", target_label="Company",
        )
    await raw.set(ACCESSION_KEY.format(cik=cik), latest["accession"], ex=200 * 86400)
    logger.info("Ownership: %s %s -- %d positions, %d in universe (%.1f MB table)",
                filer.name, latest["period"], len(positions), len(kept), size / 1e6)
    return len(kept)


async def run_forever(producer: Any, redis_client: Any,
                      resolve: Callable[[str, Optional[str]], Optional[str]]) -> None:
    raw = getattr(redis_client, "raw", redis_client)
    graph = None
    try:
        from services.enrichment.graph_writer import GraphWriter
        graph = GraphWriter(producer)
    except Exception as e:
        swallowed("collector_filings.ownership.graph_writer", e, logger)
    while True:
        cusip_cache: Dict[str, Optional[str]] = {}
        try:
            universe = await _universe(raw)
            async with aiohttp.ClientSession() as session, \
                    aiohttp.ClientSession(headers=SEC_HEADERS) as sec:
                for cik in OWNERSHIP_FILERS:
                    try:
                        await refresh_filer(session, sec, raw, graph, cik, resolve, universe, cusip_cache)
                        await ensure_baseline(session, sec, raw, cik, resolve, universe, cusip_cache)
                    except Exception as e:
                        swallowed("collector_filings.ownership.refresh", e, logger,
                                  variant=OWNERSHIP_FILERS[cik].filer_id)
            try:
                n = await publish_flows(producer, raw)
                if n:
                    logger.info("Ownership: published %d institutional flow event(s)", n)
            except Exception as e:
                swallowed("collector_filings.ownership.flows", e, logger)
        except Exception as e:
            swallowed("collector_filings.ownership.cycle", e, logger)
        await asyncio.sleep(REFRESH_SEC)

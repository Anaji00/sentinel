"""
services/collector-filings/company_facts.py

Every financial fact a company filed, with the date it was filed, into
`fundamental_facts`; and ticker -> issuer into `issuer_ticker`.

The XBRL frames in fundamentals.py carry one value per period from the latest
filing and no filing date, so restatements leaked backwards and every figure
had to be assumed known 90 days after its period. Company facts carry each
filing separately (`filed`, `form`, `accn`), which is what point-in-time means.

One request per company (data.sec.gov/api/xbrl/companyfacts/CIK##########.json,
4-8 MB for the largest filers), one at a time: this collector's memory limit is
384 MiB and the largest file parses in about 120. The first pass over the price
universe's ~5,000 issuers takes a couple of hours; after it a company is fetched
again when a new 10-Q or 10-K is due, and monthly for amendments.

Tickers are resolved ticker -> CIK from the SEC registry, which is unambiguous;
the reverse (CIK -> ticker) is not, and filed JPMorgan under JPM-PM.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from shared.utils.fundamentals_pit import ALL_TAGS, FORMS
from shared.utils.quiet_failures import swallowed
from shared.utils.sec_rate import sec_slot

logger = logging.getLogger("collector.filings.company_facts")

REGISTRY_URL = "https://www.sec.gov/files/company_tickers.json"
FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
CONCEPT_URL = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/{taxonomy}/{tag}.json"
# A company-facts file above this is read line item by line item instead. The
# largest filers' files run to tens of MB, and parsing one inside this
# collector's 384 MiB limit killed the process (8 Oct, 19:33 UTC).
MAX_FACTS_BYTES = 12_000_000
DEI_TAGS = frozenset({"EntityCommonStockSharesOutstanding"})
FIRST_FILED = date(2015, 1, 1)
REQUEST_SPACING_SEC = 0.35   # ~3 a second: SEC's fair-access limit is 10, shared with this collector's other loops
DUE_AFTER_DAYS = 75          # a quarter's 10-Q is due 40-45 days after it ends
REFRESH_DAYS = 30            # amendments and late corrections
PASS_INTERVAL_SEC = 6 * 3600
FIRST_PASS_DELAY_SEC = 300
_TAGS = frozenset(ALL_TAGS)
_FORMS = frozenset(FORMS)
_UNITS = frozenset({"USD", "shares"})

_UNIVERSE_SQL = "SELECT DISTINCT ticker FROM daily_bars"
_ISSUER_SQL = """
    INSERT INTO issuer_ticker (ticker, cik, source) VALUES ($1, $2, $3)
    ON CONFLICT (ticker, cik) DO NOTHING
"""
_DUE_SQL = f"""
    SELECT cik FROM issuer_ticker GROUP BY cik
    HAVING max(fetched_at) IS NULL
        OR max(fetched_at) < NOW() - INTERVAL '{REFRESH_DAYS} days'
        OR (max(last_filed) + {DUE_AFTER_DAYS} <= CURRENT_DATE AND max(fetched_at) < NOW() - INTERVAL '1 day')
    ORDER BY max(fetched_at) NULLS FIRST
"""
_FACTS_SQL = """
    INSERT INTO fundamental_facts (cik, concept, unit, period_start, period_end, value, fy, fp, form, filed, accn)
    VALUES ($1, $2, $3, $4::date, $5::date, $6, $7, $8, $9, $10::date, $11)
    ON CONFLICT DO NOTHING
"""
_FETCHED_SQL = "UPDATE issuer_ticker SET fetched_at = NOW(), last_filed = COALESCE($2::date, last_filed) WHERE cik = $1"


def registry_pairs(payload: Dict[str, Any]) -> List[Tuple[str, str]]:
    """company_tickers.json -> [(ticker, cik)], every ticker of every issuer."""
    out = []
    for item in (payload or {}).values():
        try:
            out.append((str(item["ticker"]).upper(), str(int(item["cik_str"]))))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def universe_pairs(pairs: List[Tuple[str, str]], universe: set) -> List[Tuple[str, str, str]]:
    """Registry pairs for tickers the price universe carries. The SEC writes a
    share class with a dash (BRK-B), the price feed with a dot (BRK.B)."""
    out = []
    for t, c in pairs:
        for form in (t, t.replace("-", ".")):
            if form in universe:
                out.append((form, c, "sec_registry"))
                break
    return out


def _d(x: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(x)[:10]) if x else None
    except ValueError:
        return None


def extract(payload: Dict[str, Any], first_filed: date = FIRST_FILED) -> List[Tuple]:
    """companyfacts JSON -> fact rows for the tags the model reads."""
    cik = str(int(payload.get("cik") or 0))
    rows = []
    for taxonomy in ("us-gaap", "dei"):
        for concept, body in ((payload.get("facts") or {}).get(taxonomy) or {}).items():
            if concept not in _TAGS:
                continue
            for unit, facts in (body.get("units") or {}).items():
                if unit not in _UNITS:
                    continue
                for f in facts:
                    filed, end = _d(f.get("filed")), _d(f.get("end"))
                    form = str(f.get("form") or "")
                    if not filed or not end or filed < first_filed or form not in _FORMS:
                        continue
                    try:
                        val = float(f["val"])
                    except (KeyError, TypeError, ValueError):
                        continue
                    rows.append((cik, concept, unit, _d(f.get("start")), end, val, f.get("fy"),
                                 f.get("fp"), form, filed, str(f.get("accn") or "")))
    return rows


def _parse(body: bytes) -> Tuple[List[Tuple], Optional[date]]:
    rows = extract(json.loads(body))
    return rows, max((r[9] for r in rows), default=None)


async def _read_capped(resp: Any, cap: int) -> Optional[bytes]:
    """The body, or None once it passes `cap` bytes."""
    buf = bytearray()
    async for chunk in resp.content.iter_chunked(1 << 16):
        buf.extend(chunk)
        if len(buf) > cap:
            return None
    return bytes(buf)


async def _concept_rows(session: Any, headers: Dict[str, str], cik: str) -> List[Tuple]:
    """The model's line items one request each, for an issuer whose full file is too large."""
    rows: List[Tuple] = []
    for tag in sorted(_TAGS):
        taxonomy = "dei" if tag in DEI_TAGS else "us-gaap"
        url = CONCEPT_URL.format(cik=int(cik), taxonomy=taxonomy, tag=tag)
        await sec_slot()
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                body = await resp.json(content_type=None)
                rows.extend(extract({"cik": cik, "facts": {taxonomy: {tag: {"units": body.get("units") or {}}}}}))
        await asyncio.sleep(REQUEST_SPACING_SEC)
    return rows


async def sync(session: Any, headers: Dict[str, str], db: Any) -> Dict[str, int]:
    stats = {"issuers": 0, "facts": 0, "errors": 0, "missing": 0}
    async with session.get(REGISTRY_URL, headers=headers) as resp:
        if resp.status != 200:
            logger.warning("SEC company registry returned HTTP %s; pass skipped.", resp.status)
            return stats
        pairs = registry_pairs(await resp.json(content_type=None))
    universe = {str(r["ticker"]).upper() for r in await db.query(_UNIVERSE_SQL) or []}
    await db.execute_many(_ISSUER_SQL, universe_pairs(pairs, universe))
    for r in await db.query(_DUE_SQL) or []:
        cik = str(r["cik"])
        try:
            await sec_slot()
            async with session.get(FACTS_URL.format(cik=int(cik)), headers=headers) as resp:
                if resp.status == 404:
                    stats["missing"] += 1             # registered, never filed XBRL
                    await db.execute(_FETCHED_SQL, cik, None)
                    continue
                if resp.status == 429:
                    logger.warning("SEC throttled the company-facts pass; pausing a minute.")
                    await asyncio.sleep(60)
                    continue
                if resp.status != 200:
                    stats["errors"] += 1
                    continue
                body = await _read_capped(resp, MAX_FACTS_BYTES)
            if body is None:
                rows = await _concept_rows(session, headers, cik)
                last = max((r[9] for r in rows), default=None)
                stats["by_concept"] = stats.get("by_concept", 0) + 1
            else:
                rows, last = await asyncio.to_thread(_parse, body)
                del body
            if rows:
                await db.execute_many(_FACTS_SQL, rows)
            await db.execute(_FETCHED_SQL, cik, last)
            stats["issuers"] += 1
            stats["facts"] += len(rows)
            if stats["issuers"] % 250 == 0:
                logger.info("SEC company facts: %s issuers, %s facts so far.", stats["issuers"], stats["facts"])
        except Exception as e:
            stats["errors"] += 1
            swallowed("collector_filings.company_facts.issuer", e, logger, detail=cik)
        await asyncio.sleep(REQUEST_SPACING_SEC)
    return stats


async def run_forever(headers: Dict[str, str]) -> None:
    """Its own HTTP session, for the reason fundamentals.run_forever has one."""
    import aiohttp
    from shared.db import get_timescale

    await asyncio.sleep(FIRST_PASS_DELAY_SEC)
    while True:
        started = datetime.now(timezone.utc)
        try:
            # Addresses kept five minutes: host DNS failed in bursts on 8 Oct and
            # one burst failed 1,318 issuers of a pass with a ten-second cache.
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120),
                                             connector=aiohttp.TCPConnector(ttl_dns_cache=300)) as session:
                stats = await sync(session, headers, await get_timescale())
            logger.info("SEC company facts pass (%s): %s", started.strftime("%H:%M"), stats)
        except Exception as e:
            swallowed("collector_filings.company_facts", e, logger)
        await asyncio.sleep(PASS_INTERVAL_SEC)


__all__ = ["extract", "registry_pairs", "run_forever", "sync"]

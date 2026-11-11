"""Years of 8-K history, rebuilt point-in-time from EDGAR, into the permanent archive.

The live feed only knows filings from the day it started, and the reaction
study needs twenty independent episodes per family before it will judge one.
A family like Item 4.02 (non-reliance) fires a few times a month across the
whole market; on months of live data it could never be measured.

EDGAR's submissions API returns, per company, every recent filing with its
form, its 8-K item codes and its acceptance timestamp -- the moment it became
public. Each 8-K in the window becomes a row in signal_event_archive stamped at
that acceptance time, so the study measures from when the market could have
known, never earlier. The summary is written in the live firehose's format, so
the 8-K families read history and live filings identically.

One request per company, paced under SEC's fair-access limit, and each company
is done once (Redis set), so a restart resumes rather than refetches.

The main submissions file lists only a company's most recent ~1,000 filings;
the rest sit in the older pages named under filings.files, same columns at the
top level. The window opens at HISTORY_START (the first daily price bar), and a
company's older pages are fetched only when they reach back into the window.
"""
from __future__ import annotations

import asyncio
import re
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from shared.utils.quiet_failures import swallowed
from shared.utils.sec_rate import sec_slot

logger = logging.getLogger("collector_filings.history")

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:0>10}.json"
SUBMISSIONS_PAGE_URL = "https://data.sec.gov/submissions/{name}"
# Daily prices begin on 4 January 2016, so history before it could not be measured.
HISTORY_START = datetime(2016, 1, 1, tzinfo=timezone.utc)
# Tickers finished for this window. The earlier set (five years, recent filings
# only) is left alone: its companies are fetched again for the older pages, and
# the rows they already archived are skipped on conflict.
DEEP_DONE_SUFFIX = ":from2016"
# A company whose older pages keep failing is parked after this many passes.
MAX_ATTEMPTS = 3
# Dealer-bank filers run to a hundred pages; past this a company is cut short and counted.
MAX_PAGES_PER_COMPANY = 400
# SEC asks for no more than ten requests a second.
REQUEST_SPACING_SEC = 0.15

_INSERT_SQL = """
    INSERT INTO signal_event_archive
        (event_id, type, source, created_at, occurred_at, headline, summary, tags,
         sentiment, primary_entity_id, financial_data, region)
    VALUES ($1, 'filing', 'sec_edgar_history', $2, $2, $3, $4, $5, NULL, $6, $7::jsonb, NULL)
    ON CONFLICT DO NOTHING
"""


def _accepted(raw: str) -> Optional[datetime]:
    """EDGAR acceptance time ("2024-03-01T16:05:12.000Z") as an aware datetime."""
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def older_page_names(submissions: Dict[str, Any], since: datetime) -> List[str]:
    """Names of the older submissions pages that reach into the window. Pure."""
    cutoff = since.date().isoformat()
    names = []
    for f in ((submissions or {}).get("filings") or {}).get("files") or []:
        name = str(f.get("name") or "")
        if name and str(f.get("filingTo") or "9999-12-31") >= cutoff:
            names.append(name)
    return names


def eight_k_rows(
    ticker: str,
    company: str,
    submissions: Dict[str, Any],
    descriptions: Dict[str, str],
    since: datetime,
    older_pages: Iterable[Dict[str, Any]] = (),
) -> List[tuple]:
    """Archive rows for every 8-K accepted since `since`, from the recent
    filings and any older pages (same columns, at the top level). Pure."""
    recent = ((submissions or {}).get("filings") or {}).get("recent") or {}
    rows: List[tuple] = []
    for page in [recent, *older_pages]:
        rows.extend(_page_rows(ticker, company, page or {}, descriptions, since))
    return rows


def _page_rows(ticker: str, company: str, page: Dict[str, Any], descriptions: Dict[str, str],
               since: datetime) -> List[tuple]:
    forms = page.get("form") or []
    rows: List[tuple] = []
    for i, form in enumerate(forms):
        if form != "8-K":
            continue
        try:
            accepted = _accepted(page["acceptanceDateTime"][i])
            accession = page["accessionNumber"][i]
            items_raw = page.get("items", [""] * len(forms))[i] or ""
            filed = page.get("filingDate", [""] * len(forms))[i] or ""
        except (KeyError, IndexError):
            continue
        if accepted is None or accepted < since:
            continue
        codes = [c.strip() for c in items_raw.split(",") if c.strip()]
        if not codes:
            continue
        labels = [f"{c}: {descriptions.get(c, 'Corporate Disclosure')}" for c in codes]
        summary = f"Material 8-K Event for {ticker}: {'; '.join(labels)}. Filed on {filed}."
        rows.append((
            f"edgar8k:{accession}",
            accepted,
            f"SEC Filing: {company} ({ticker}) filed Form 8-K",
            summary,
            ["filing", "form:8-K", f"ticker:{ticker}", "historical_backfill", f"exposes:{ticker}"],
            ticker,
            # A dict, not json.dumps: the pool's jsonb codec encodes it (0052).
            {"ticker": ticker, "items": codes, "accession_number": accession},
        ))
    return rows


async def backfill_8k_history(
    session: Any,
    headers: Dict[str, str],
    db: Any,
    redis_client: Any,
    tickers_to_cik: Dict[str, str],
    descriptions: Dict[str, str],
    done_key: str,
    since: datetime = HISTORY_START,
    extra_spacing_sec: float = 0.0,
) -> Dict[str, int]:
    """8-K history since `since` for every ticker not yet done. Returns counts."""
    raw = getattr(redis_client, "raw", redis_client)
    done_key = done_key + DEEP_DONE_SUFFIX
    attempts_key = done_key + ":attempts"
    done = {(m.decode() if isinstance(m, bytes) else str(m)) for m in (await raw.smembers(done_key) or [])}
    stats = {"companies": 0, "filings": 0, "pages": 0, "errors": 0, "truncated": 0}
    for ticker, cik in sorted(tickers_to_cik.items()):
        if ticker in done or not cik:
            continue
        try:
            await sec_slot()
            async with session.get(SUBMISSIONS_URL.format(cik=str(cik).lstrip("0") or "0"), headers=headers) as resp:
                if resp.status == 429:
                    logger.warning("SEC throttled the history backfill; pausing a minute.")
                    await asyncio.sleep(60)
                    continue
                if resp.status != 200:
                    stats["errors"] += 1
                    await raw.sadd(done_key, ticker)   # nothing to fetch; do not retry forever
                    continue
                payload = await resp.json(content_type=None)
            names = older_page_names(payload, since)
            if len(names) > MAX_PAGES_PER_COMPANY:
                stats["truncated"] += 1
                names = names[:MAX_PAGES_PER_COMPANY]
            pages: List[Dict[str, Any]] = []
            complete = True
            for name in names:
                await sec_slot()
                async with session.get(SUBMISSIONS_PAGE_URL.format(name=name), headers=headers) as presp:
                    if presp.status != 200:
                        if presp.status == 429:
                            logger.warning("SEC throttled the history backfill; pausing a minute.")
                            await asyncio.sleep(60)
                        stats["errors"] += 1
                        complete = False
                        break
                    pages.append(await presp.json(content_type=None))
                stats["pages"] += 1
                await asyncio.sleep(REQUEST_SPACING_SEC + extra_spacing_sec)
            rows = eight_k_rows(ticker, payload.get("name") or ticker, payload, descriptions, since, pages)
            for i in range(0, len(rows), 500):
                await db.execute_many(_INSERT_SQL, rows[i:i + 500])
            stats["companies"] += 1
            stats["filings"] += len(rows)
            if complete:
                await raw.sadd(done_key, ticker)
            elif int(await raw.hincrby(attempts_key, ticker, 1)) >= MAX_ATTEMPTS:
                logger.warning("8-K history for %s parked after %s incomplete passes.", ticker, MAX_ATTEMPTS)
                await raw.sadd(done_key, ticker)
        except Exception as e:
            stats["errors"] += 1
            swallowed("collector_filings.history.company", e, logger, detail=ticker)
        await asyncio.sleep(REQUEST_SPACING_SEC + extra_spacing_sec)
    if stats["companies"]:
        logger.info("EDGAR 8-K history: %s companies, %s filings archived from %s pages, %s errors, %s truncated.",
                    stats["companies"], stats["filings"], stats["pages"], stats["errors"], stats["truncated"])
    return stats


_PROFILE_SQL = """
    INSERT INTO company_profile (ticker, cik, sic, sic_description, issuer_kind, state_of_incorporation, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, NOW())
    ON CONFLICT (ticker) DO UPDATE SET cik = EXCLUDED.cik, sic = EXCLUDED.sic,
        sic_description = EXCLUDED.sic_description, issuer_kind = EXCLUDED.issuer_kind,
        state_of_incorporation = EXCLUDED.state_of_incorporation, updated_at = NOW()
"""
# Annual reports that mark a foreign private issuer, whose shares trade here as
# ADRs or foreign ordinary shares rather than a domestic company's common stock.
FOREIGN_ANNUAL_FORMS = frozenset({"20-F", "20-F/A", "40-F", "40-F/A"})
DOMESTIC_ANNUAL_FORMS = frozenset({"10-K", "10-K/A", "10-KT"})
_PARTNERSHIP = re.compile(r"(\bL\.?\s?P\.?|\bLimited Partnership)$", re.IGNORECASE)


def issuer_kind(payload: Dict[str, Any]) -> str:
    """"domestic", "foreign" or "partnership" from an EDGAR submissions payload:
    a 20-F or 40-F filer with no 10-K is foreign; a name ending in L.P. a
    partnership (units, a K-1, not common stock)."""
    if _PARTNERSHIP.search(str(payload.get("name") or "").strip().rstrip(",")):
        return "partnership"
    forms = set(((payload.get("filings") or {}).get("recent") or {}).get("form") or [])
    if forms & FOREIGN_ANNUAL_FORMS and not forms & DOMESTIC_ANNUAL_FORMS:
        return "foreign"
    return "domestic"


def profile_row(ticker: str, cik: str, payload: Dict[str, Any]
                ) -> Optional[Tuple[str, str, Optional[int], Optional[str], str, Optional[str]]]:
    """The company's SIC industry code and issuer kind from an EDGAR submissions payload."""
    raw_sic = str(payload.get("sic") or "").strip()
    sic = int(raw_sic) if raw_sic.isdigit() else None
    if sic is None:
        return None
    return (ticker.upper(), str(cik), sic, (payload.get("sicDescription") or None), issuer_kind(payload),
            (str(payload.get("stateOfIncorporation") or "") or None))


async def backfill_profiles(session: Any, headers: Dict[str, str], db: Any,
                            tickers_to_cik: Dict[str, str]) -> Dict[str, int]:
    """SIC codes for every ticker without one, for industry features in the
    cross-sectional model. One submissions request per company, paced as the
    8-K backfill is."""
    # Profiles stored before issuer kinds were kept are fetched again once.
    have = {str(r["ticker"]).upper() for r in
            await db.query("SELECT ticker FROM company_profile WHERE issuer_kind IS NOT NULL") or []}
    stats = {"profiles": 0, "errors": 0}
    for ticker, cik in sorted(tickers_to_cik.items()):
        if ticker in have or not cik:
            continue
        try:
            await sec_slot()
            async with session.get(SUBMISSIONS_URL.format(cik=str(cik).lstrip("0") or "0"), headers=headers) as resp:
                if resp.status == 429:
                    logger.warning("SEC throttled the profile backfill; pausing a minute.")
                    await asyncio.sleep(60)
                    continue
                if resp.status != 200:
                    stats["errors"] += 1
                    continue
                payload = await resp.json(content_type=None)
            row = profile_row(ticker, cik, payload)
            if row:
                await db.execute(_PROFILE_SQL, *row)
                stats["profiles"] += 1
        except Exception as e:
            stats["errors"] += 1
            swallowed("collector_filings.history.profile", e, logger, detail=ticker)
        await asyncio.sleep(REQUEST_SPACING_SEC)
    if stats["profiles"]:
        logger.info("EDGAR company profiles: %s SIC codes stored, %s errors.", stats["profiles"], stats["errors"])
    return stats


def universe_ciks(cik_to_ticker: Dict[str, str], tickers: Iterable[str]) -> Dict[str, str]:
    """{ticker: cik} for the tickers the registry knows."""
    inverse: Dict[str, str] = {}
    for cik, t in (cik_to_ticker or {}).items():
        t = str(t).upper()
        inverse.setdefault(t, str(cik))
    return {t.upper(): inverse[t.upper()] for t in tickers if t and t.upper() in inverse}


__all__ = ["HISTORY_START", "backfill_8k_history", "backfill_profiles", "eight_k_rows", "older_page_names",
           "profile_row", "universe_ciks"]

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
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector_filings.history")

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:0>10}.json"
HISTORY_YEARS = 5
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


def eight_k_rows(
    ticker: str,
    company: str,
    submissions: Dict[str, Any],
    descriptions: Dict[str, str],
    since: datetime,
) -> List[tuple]:
    """Archive rows for every 8-K accepted since `since`. Pure."""
    recent = ((submissions or {}).get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    rows: List[tuple] = []
    for i, form in enumerate(forms):
        if form != "8-K":
            continue
        try:
            accepted = _accepted(recent["acceptanceDateTime"][i])
            accession = recent["accessionNumber"][i]
            items_raw = recent.get("items", [""] * len(forms))[i] or ""
            filed = recent.get("filingDate", [""] * len(forms))[i] or ""
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
            json.dumps({"ticker": ticker, "items": codes, "accession_number": accession}),
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
    years: int = HISTORY_YEARS,
) -> Dict[str, int]:
    """8-K history for every ticker not yet done. Returns counts."""
    raw = getattr(redis_client, "raw", redis_client)
    done = {(m.decode() if isinstance(m, bytes) else str(m)) for m in (await raw.smembers(done_key) or [])}
    since = datetime.now(timezone.utc) - timedelta(days=365 * years)
    stats = {"companies": 0, "filings": 0, "errors": 0}
    for ticker, cik in sorted(tickers_to_cik.items()):
        if ticker in done or not cik:
            continue
        try:
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
            rows = eight_k_rows(ticker, payload.get("name") or ticker, payload, descriptions, since)
            for row in rows:
                await db.execute(_INSERT_SQL, *row)
            stats["companies"] += 1
            stats["filings"] += len(rows)
            await raw.sadd(done_key, ticker)
        except Exception as e:
            stats["errors"] += 1
            swallowed("collector_filings.history.company", e, logger, detail=ticker)
        await asyncio.sleep(REQUEST_SPACING_SEC)
    if stats["companies"]:
        logger.info("EDGAR 8-K history: %s companies, %s filings archived, %s errors.",
                    stats["companies"], stats["filings"], stats["errors"])
    return stats


_PROFILE_SQL = """
    INSERT INTO company_profile (ticker, cik, sic, sic_description, updated_at)
    VALUES ($1, $2, $3, $4, NOW())
    ON CONFLICT (ticker) DO UPDATE SET cik = EXCLUDED.cik, sic = EXCLUDED.sic,
        sic_description = EXCLUDED.sic_description, updated_at = NOW()
"""


def profile_row(ticker: str, cik: str, payload: Dict[str, Any]) -> Optional[Tuple[str, str, Optional[int], Optional[str]]]:
    """The company's SIC industry code from an EDGAR submissions payload."""
    raw_sic = str(payload.get("sic") or "").strip()
    sic = int(raw_sic) if raw_sic.isdigit() else None
    if sic is None:
        return None
    return (ticker.upper(), str(cik), sic, (payload.get("sicDescription") or None))


async def backfill_profiles(session: Any, headers: Dict[str, str], db: Any,
                            tickers_to_cik: Dict[str, str]) -> Dict[str, int]:
    """SIC codes for every ticker without one, for industry features in the
    cross-sectional model. One submissions request per company, paced as the
    8-K backfill is."""
    have = {str(r["ticker"]).upper() for r in await db.query("SELECT ticker FROM company_profile") or []}
    stats = {"profiles": 0, "errors": 0}
    for ticker, cik in sorted(tickers_to_cik.items()):
        if ticker in have or not cik:
            continue
        try:
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


__all__ = ["backfill_8k_history", "backfill_profiles", "eight_k_rows", "profile_row", "universe_ciks"]

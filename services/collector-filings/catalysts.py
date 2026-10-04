"""
services/collector-filings/catalysts.py

Two free catalyst feeds for listed equities: FINRA short volume and FDA approvals.

FINRA publishes, each trading day, every symbol's short-sale volume against its
total volume across the reporting venues. A name whose short share jumps well
above its own recent norm is being leaned on, and nothing else on the platform
measures that. The FDA's approvals are the catalyst a biotech is priced for.
Both arrive as finished events: the collector measures them against their own
history and sends them through enrichment unchanged, like the FRED releases.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from shared.kafka import SentinelProducer, Topics
from shared.models import NormalizedEvent, EventType, Entity, EntityType, FinancialData, RawEvent
from shared.models.events import RegulatoryData
from shared.utils.quiet_failures import dropped, swallowed
from shared.utils.quote_cache import quote_key

logger = logging.getLogger("collector.catalysts")

CATALYST_INTERVAL_SEC = 1800

# ── FINRA short volume ────────────────────────────────────────────────────────

FINRA_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{day}.txt"
FINRA_STATE_KEY = "sentinel:catalysts:finra:last_day"
SHORT_HISTORY_KEY = "sentinel:shortvol:{symbol}"
SHORT_HISTORY_DAYS = 20
SHORT_MIN_HISTORY = 10
# Thin names swing on a handful of prints; below this a ratio is noise.
SHORT_MIN_TOTAL_VOLUME = 500_000
# A spike: at least this share short, and this far above the name's own mean.
SHORT_SPIKE_FLOOR = 0.55
SHORT_SPIKE_JUMP = 0.15


def parse_short_volume(text: str) -> Dict[str, Tuple[float, float]]:
    """{symbol: (short_volume, total_volume)} from FINRA's pipe-delimited file."""
    out: Dict[str, Tuple[float, float]] = {}
    for line in (text or "").splitlines()[1:]:
        parts = line.split("|")
        if len(parts) < 5:
            continue  # the trailer is a bare row count
        try:
            short, total = float(parts[2]), float(parts[4])
        except ValueError:
            continue
        sym = parts[1].strip().upper()
        if sym and total > 0:
            out[sym] = (short, total)
    return out


def short_spike(ratio: float, history: List[float]) -> bool:
    """Whether today's short share is a spike against the name's own history."""
    if len(history) < SHORT_MIN_HISTORY:
        return False
    return ratio >= SHORT_SPIKE_FLOOR and ratio >= mean(history) + SHORT_SPIKE_JUMP


def short_spike_event(symbol: str, day: str, ratio: float, history: List[float],
                      total: float) -> NormalizedEvent:
    base = mean(history)
    jump = ratio - base
    score = round(min(0.95, 0.55 + jump * 1.5), 3)
    occurred = datetime.strptime(day, "%Y%m%d").replace(hour=22, tzinfo=timezone.utc)
    return NormalizedEvent(
        event_id=f"shortvol_{symbol}_{day}_{uuid.uuid4().hex[:6]}",
        type=EventType.MARKET_ANOMALY,
        occurred_at=occurred,
        collected_at=datetime.now(timezone.utc),
        source="finra_short_volume",
        source_reliability=0.95,
        primary_entity=Entity(id=symbol, name=symbol, type=EntityType.INSTRUMENT),
        headline=f"Short volume spike: {symbol} {ratio:.0%} of volume short vs {base:.0%} 20-day norm",
        summary=(
            f"FINRA reported {ratio:.1%} of {symbol}'s {total:,.0f} shares traded on "
            f"{occurred.date()} as short sales, against a {len(history)}-day mean of "
            f"{base:.1%} (+{jump:.1%})."
        ),
        anomaly_score=score,
        financial_data=FinancialData(ticker=symbol, volume=total, trade_type="SHORT_VOLUME"),
        tags=["short_volume_spike", "tradfi", f"exposes:{symbol}"],
        region="US",
    )


async def poll_short_volume(session, producer: Optional[SentinelProducer], redis_client: Any) -> int:
    """Process the newest FINRA file not yet seen. Returns spikes emitted."""
    raw = getattr(redis_client, "raw", redis_client)
    last = await raw.get(FINRA_STATE_KEY)
    last = last.decode() if isinstance(last, bytes) else last
    today = datetime.now(timezone.utc).date()
    for back in range(0, 5):
        day = (today - timedelta(days=back)).strftime("%Y%m%d")
        if last and day <= last:
            return 0
        async with session.get(FINRA_URL.format(day=day), headers={"User-Agent": "Mozilla/5.0"}) as resp:
            if resp.status != 200:
                continue
            text = await resp.text()
        rows = parse_short_volume(text)
        if not rows:
            continue
        emitted = await _process_short_day(day, rows, producer, raw)
        await raw.set(FINRA_STATE_KEY, day)
        logger.info("FINRA short volume %s: %s symbols, %s spike(s).", day, len(rows), emitted)
        return emitted
    return 0


async def _process_short_day(day, rows, producer, raw) -> int:
    # Only names the platform prices: a spike in something it cannot quote is
    # nothing an agent can act on.
    symbols = [s for s, (_, total) in rows.items() if total >= SHORT_MIN_TOTAL_VOLUME]
    pipe = raw.pipeline()
    for s in symbols:
        pipe.exists(quote_key(s))
    quoted = [s for s, ok in zip(symbols, await pipe.execute()) if ok]
    emitted = 0
    for s in quoted:
        short, total = rows[s]
        ratio = short / total
        key = SHORT_HISTORY_KEY.format(symbol=s)
        history = [float(v) for v in await raw.lrange(key, 0, SHORT_HISTORY_DAYS - 1)]
        if short_spike(ratio, history):
            await _publish(producer, short_spike_event(s, day, ratio, history, total))
            emitted += 1
        await raw.lpush(key, f"{ratio:.4f}")
        await raw.ltrim(key, 0, SHORT_HISTORY_DAYS - 1)
        await raw.expire(key, 45 * 86400)
    return emitted


# ── FDA approvals ─────────────────────────────────────────────────────────────

OPENFDA_URL = "https://api.fda.gov/drug/drugsfda.json"
FDA_SEEN_KEY = "sentinel:catalysts:fda:seen"
FDA_LOOKBACK_DAYS = 7

# FDA sponsor names that never match an SEC registrant title: subsidiaries
# and house abbreviations of the listed parents.
SPONSOR_TICKERS: Dict[str, str] = {
    "ELI LILLY": "LLY", "LILLY": "LLY",
    "MERCK SHARP DOHME": "MRK", "MERCK SHARP": "MRK",
    "JANSSEN": "JNJ", "JOHNSON AND JOHNSON": "JNJ",
    "GENENTECH": "RHHBY", "HOFFMANN LA ROCHE": "RHHBY",
    "NOVARTIS": "NVS", "SANDOZ": "SDZNY",
    "ASTRAZENECA": "AZN", "GLAXOSMITHKLINE": "GSK", "GSK": "GSK",
    "SANOFI": "SNY", "SANOFI AVENTIS": "SNY", "GENZYME": "SNY",
    "NOVO NORDISK": "NVO", "TAKEDA": "TAK", "BAYER": "BAYRY",
    "BRISTOL MYERS SQUIBB": "BMY", "ABBVIE": "ABBV", "PFIZER": "PFE",
    "TEVA": "TEVA", "VERTEX": "VRTX", "REGENERON": "REGN",
}

_ABBREVIATIONS = (
    (r"\bPHARMS\b", "PHARMACEUTICALS"), (r"\bPHARMA\b", "PHARMACEUTICALS"),
    (r"\bHLTHCARE\b", "HEALTHCARE"), (r"\bLABS\b", "LABORATORIES"),
    (r"\bTHERAP\b", "THERAPEUTICS"), (r"\bINTL\b", "INTERNATIONAL"),
)
_NOISE = re.compile(r"\b(USA|US|INC|LLC|LTD|CORP|CO|AND|&|AG|SA|PLC|NV|GMBH)\b")


def sponsor_ticker(sponsor: str, resolver: Callable[[str], Optional[str]]) -> Optional[str]:
    """The listed ticker of an FDA sponsor, or None when it has no listing."""
    name = re.sub(r"[^A-Z0-9 ]", " ", (sponsor or "").upper())
    name = " ".join(name.split())
    if not name:
        return None
    expanded = name
    for pat, rep in _ABBREVIATIONS:
        expanded = re.sub(pat, rep, expanded)
    stripped = " ".join(_NOISE.sub(" ", expanded).split())
    for candidate in (name, expanded, stripped):
        for known, ticker in SPONSOR_TICKERS.items():
            if candidate == known or candidate.startswith(known + " "):
                return ticker
    for candidate in (expanded, stripped, name):
        try:
            hit = resolver(candidate)
        except Exception:
            hit = None
        if hit:
            return hit
    return None


def parse_approvals(results: Iterable[Dict[str, Any]], since: date) -> List[Dict[str, Any]]:
    """Original NDA/BLA approvals dated on or after `since`.

    openFDA's date and type filters each match any submission on the
    application, not the same one, so the pairing is checked here.
    """
    out = []
    for app in results or []:
        number = str(app.get("application_number") or "")
        if not number.startswith(("NDA", "BLA")):
            continue  # generics (ANDA) rarely move a listed sponsor
        for sub in app.get("submissions") or []:
            if sub.get("submission_type") != "ORIG" or sub.get("submission_status") != "AP":
                continue
            try:
                when = datetime.strptime(str(sub.get("submission_status_date")), "%Y%m%d").date()
            except ValueError:
                continue
            if when < since:
                continue
            brands = sorted({p.get("brand_name") for p in app.get("products") or [] if p.get("brand_name")})
            out.append({
                "application": number, "sponsor": app.get("sponsor_name") or "",
                "date": when, "brands": brands,
                "class": str(sub.get("submission_class_code") or ""),
            })
    return out


def approval_event(approval: Dict[str, Any], ticker: str) -> NormalizedEvent:
    novel = approval["class"] in ("TYPE 1", "TYPE 1/4") or approval["application"].startswith("BLA")
    score = 0.85 if novel else 0.6
    brands = ", ".join(approval["brands"][:3]) or approval["application"]
    occurred = datetime.combine(approval["date"], datetime.min.time(), tzinfo=timezone.utc)
    kind = "new molecular entity / biologic" if novel else "new application"
    return NormalizedEvent(
        event_id=f"fda_{approval['application']}_{approval['date']:%Y%m%d}",
        type=EventType.REGULATORY_EVENT,
        occurred_at=occurred,
        collected_at=datetime.now(timezone.utc),
        source="openfda",
        source_reliability=0.99,
        primary_entity=Entity(id=ticker, name=approval["sponsor"], type=EntityType.INSTRUMENT),
        headline=f"FDA approval: {brands} ({approval['sponsor']}, {ticker})",
        summary=f"FDA approved {approval['application']} ({kind}) for {approval['sponsor']} on {approval['date']}.",
        anomaly_score=score,
        regulatory_data=RegulatoryData(
            agency="FDA", action_type="approval", title=f"{brands} approval",
            document_number=approval["application"], publication_date=approval["date"].isoformat(),
            affected_tickers=[ticker],
        ),
        tags=["fda_approval", "regulatory", f"exposes:{ticker}"] + (["novel_approval"] if novel else []),
        region="US",
    )


async def poll_fda(session, producer, redis_client, resolver) -> int:
    raw = getattr(redis_client, "raw", redis_client)
    since = datetime.now(timezone.utc).date() - timedelta(days=FDA_LOOKBACK_DAYS)
    params = {
        "search": f"submissions.submission_status_date:[{since:%Y%m%d}+TO+{datetime.now(timezone.utc):%Y%m%d}]",
        "limit": "100",
    }
    url = OPENFDA_URL + "?search=" + params["search"] + "&limit=" + params["limit"]
    async with session.get(url) as resp:
        if resp.status == 404:
            return 0  # openFDA answers 404 for "no matches"
        resp.raise_for_status()
        results = (await resp.json()).get("results") or []
    emitted = 0
    for approval in parse_approvals(results, since):
        marker = f"{approval['application']}:{approval['date']:%Y%m%d}"
        if await raw.sismember(FDA_SEEN_KEY, marker):
            continue
        await raw.sadd(FDA_SEEN_KEY, marker)
        await raw.expire(FDA_SEEN_KEY, 60 * 86400)
        ticker = sponsor_ticker(approval["sponsor"], resolver)
        if not ticker:
            dropped("collector_filings.fda_unlisted_sponsor", "sponsor has no listed ticker",
                    logger, detail=approval["sponsor"][:60])
            continue
        await _publish(producer, approval_event(approval, ticker))
        emitted += 1
    return emitted


# ── shared ────────────────────────────────────────────────────────────────────

async def _publish(producer, event: NormalizedEvent) -> None:
    """Through enrichment unchanged, so it is stored as well as read."""
    if producer is None:
        return
    raw_event = RawEvent(source=event.source, occurred_at=event.occurred_at,
                         raw_payload={"normalized_event": event.model_dump(mode="json")})
    await producer.send(Topics.RAW_TRADFI, raw_event.model_dump(), key=event.primary_entity.id)


async def run_forever(producer, redis_client, resolver) -> None:
    import aiohttp
    while True:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
                for name, call in (
                    ("short_volume", poll_short_volume(session, producer, redis_client)),
                    ("fda", poll_fda(session, producer, redis_client, resolver)),
                ):
                    try:
                        await call
                    except Exception as e:
                        swallowed(f"collector_filings.catalysts.{name}", e, logger)
        except Exception as e:
            swallowed("collector_filings.catalysts.session", e, logger)
        await asyncio.sleep(CATALYST_INTERVAL_SEC)

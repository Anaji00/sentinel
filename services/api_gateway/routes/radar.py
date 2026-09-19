import json
import logging
import os
from fastapi import APIRouter, Depends, Query
from services.api_gateway.dependencies import get_db, get_db_optional, get_redis_client, get_redis_optional
from shared.utils.serialization import score_dto, to_dto
from shared.models.provenance import ProvenanceSourceType

from shared.utils.quote_cache import quote_key
from shared.utils.candles import (
    CANDLE_KEY_PREFIX,
    candle_cache_key,
    normalize_candle,
    normalize_timeframe,
    timeframe_aliases,
)

from shared.utils.watchlists import WATCHED_EQUITIES_KEY
from shared.utils.ticker_stats import read_ticker_stats
from shared.utils.radar_keys import (
    MOVERS_DAY_ZSET,
    RADAR_BASELINE_PREFIX,
    RADAR_SWEEP_STATE_KEY,
    movers_snapshot_key,
)
logger = logging.getLogger("api-gateway.radar")

# Anomaly scores are unit-normalised [0,1]. The enrichers get there with
# `1 - exp(-z/5)`, so the way back is `-5 * ln(1 - score)` -- not a
# multiplication, and not by 4.5.
#
# What was here read: "maps them onto a z-like scale for display. Named rather
# than inlined so the relationship is auditable." The relationship was
# auditable and it was wrong: `score * 4.5` is bounded above by 4.5, so five,
# ten and twenty sigma displayed as 2.84, 3.89 and 4.42 and converged on the
# ceiling -- in the field named for the quantity that tells them apart.
from shared.utils.anomaly_scale import score_to_z
# One decoder for the movers hash, defined beside the watchlist reader
# that also needs it.
from services.api_gateway.routes.watchlists import read_movers_snapshot
from shared.utils.market_cap import DEFAULT_MIN_MARKET_CAP_USD, cached_market_cap
from shared.utils.equities import TICKER_SHAPED_STOPWORDS

# How far down the ranking a gated request will look before giving up.
# Bounds the request path against a session where thousands of microcaps
# move and the qualifying names sit a long way down.
MOVERS_WALK_CAP = int(os.getenv('MOVERS_WALK_CAP', '3000'))

router = APIRouter(prefix="/api/v1/radar", tags=["Quantitative Radar"])

@router.get("/anomalies")
async def get_radar_anomalies(
    limit: int = Query(20, ge=1, le=100),
    db = Depends(get_db),
    redis = Depends(get_redis_client)
):
    """Retrieve quantitative volume anomalies detected by collector.radar."""
    anomalies = []
    if db:
        try:
            rows = await db.query(
                """
                SELECT event_id, type, occurred_at, primary_entity_id as ticker,
                       primary_entity_name as entity_name, region, anomaly_score,
                       financial_data as domain_data
                FROM events
                WHERE source = 'alpaca_quant_radar' OR type = 'volume_anomaly'
                ORDER BY occurred_at DESC
                LIMIT $1;
                """,
                limit
            )
            for r in rows:
                t = r["ticker"] or "UNKNOWN"
                e_name = r["entity_name"] or t
                score = float(r["anomaly_score"] or 0.0)
                # Rounded here rather than at render time: `score * 4.5` yields
                # values like 2.6999999999999997, which reach the browser
                # verbatim and are displayed as-is by any consumer that forgets.
                anomalies.append(score_dto({
                    "event_id": r["event_id"],
                    "ticker": t,
                    "primary_entity_name": e_name,
                    "entity_name": e_name,
                    "anomaly_score": score,
                    "occurred_at": r["occurred_at"],
                    # A z-equivalent, not a measured sigma: this inverts the
                    # curve the tradfi enricher applies, and not every score
                    # reaching this route came through it.
                    "z_score": score_to_z(score),
                    "region": r["region"] or "US Equities",
                    "details": r["domain_data"] or {}
                }))
        except Exception as e:
            logger.warning(f"Error querying radar anomalies from DB: {e}")

    # What each of those names has actually been doing.
    #
    # A volume spike on a stock up 14% and the same spike on one that has not
    # moved are different events, and this endpoint returned a z-score for both
    # and no way to tell them apart. The move comes from the radar's own sweep
    # and the standing from daily bars; neither costs a query here.
    if redis and anomalies:
        for anomaly in anomalies:
            ticker = anomaly.get("ticker")
            if not ticker:
                continue
            try:
                detail = await redis.raw.hgetall(movers_snapshot_key(ticker))
                for k, v in (detail or {}).items():
                    key = k.decode() if isinstance(k, bytes) else k
                    value = v.decode() if isinstance(v, bytes) else v
                    try:
                        anomaly[key] = float(value)
                    except (TypeError, ValueError):
                        anomaly[key] = value
            except Exception as _exc:
                swallowed("api_gateway.routes.radar.anomaly_move", _exc, logger)
            # None means the store did not answer; {} means no standing.
            stats = await read_ticker_stats(redis, ticker) or {}
            for field in ("dist_sma_50_pct", "dist_sma_200_pct", "ma_alignment",
                          "change_pct_week"):
                if field in stats:
                    anomaly[field] = stats[field]

    # Fallback to current dynamic watchlist if cold start
    watchlist = []
    if redis:
        try:
            raw_items = await redis.raw.zrange(WATCHED_EQUITIES_KEY, 0, -1, withscores=True)
            for item in raw_items:
                t = item[0].decode('utf-8') if isinstance(item[0], bytes) else str(item[0])
                score = float(item[1])
                # A watched ticker used to arrive with nothing but the moment it
                # was promoted. The zset score stays the promotion time, because
                # that is the radar's eviction order and should not be
                # overloaded; the move joins from the movers store instead.
                row = {"ticker": t, "added_timestamp": score, "day_pct": None}
                try:
                    detail = await redis.raw.hgetall(movers_snapshot_key(t))
                    for k, v in (detail or {}).items():
                        key = k.decode() if isinstance(k, bytes) else k
                        value = v.decode() if isinstance(v, bytes) else v
                        try:
                            row[key] = float(value)
                        except (TypeError, ValueError):
                            row[key] = value
                except Exception as _exc:
                    swallowed("api_gateway.routes.radar.watchlist_move", _exc, logger)
                # Where it stands, as well as what it did today. A row used to
                # carry the moment it was promoted and nothing else.
                row.update(await read_ticker_stats(redis, t) or {})
                watchlist.append(row)
        except Exception as e:
            logger.warning(f"Error reading sentinel:watched:equities from Redis: {e}")

    return {
        "service": "collector.radar",
        "anomalies_count": len(anomalies),
        "anomalies": anomalies,
        "watchlist_count": len(watchlist),
        "watchlist": watchlist
    }

from typing import Optional, Dict, Any
from datetime import datetime, timezone
import math
import time

@router.get("/movers")
async def get_movers(
    direction: str = Query("gainers", pattern="^(gainers|losers)$"),
    limit: int = Query(20, ge=1, le=200),
    window_hours: int = Query(24, ge=1, le=168, description="Window for the news join"),
    min_market_cap_usd: float = Query(
        DEFAULT_MIN_MARKET_CAP_USD,
        ge=0,
        description=(
            "Exclude anything smaller than this, in USD. 0 disables the gate "
            "and returns the raw board, warrants included."
        ),
    ),
    redis = Depends(get_redis_client),
    db = Depends(get_db_optional),
):
    """The day's biggest moves, from bars the radar was already downloading.

    Alpaca's snapshot carries today's bar and yesterday's bar for every symbol
    in the sweep. The collector bound both and used them only as a price
    fallback, so the platform could say what traded unusually much and had no
    way to say what went up -- there was no gainers-or-losers concept anywhere
    in the tree.

    One sorted set scored by the day's move answers both directions, so neither
    can fall out of date relative to the other.
    """
    if not redis:
        raise HTTPException(status_code=503, detail="Movers store is unavailable.")

    raw = redis.raw

    # Walk further than `limit` when a gate is on.
    #
    # The board is ranked by percentage move over the radar's whole sweep, and
    # the top of that ranking is warrants and sub-dollar tickers whose previous
    # close was a rounding error -- MSAIW moved 18,800% from $0.0001. Taking
    # the first `limit` rows and filtering them would return an almost empty
    # board; the qualifying companies are further down, so this pages until it
    # has `limit` of them or runs out of patience.
    #
    # WALK_CAP bounds the work: each page is a zset read plus a cached lookup
    # per ticker, and an unbounded walk over 11,579 symbols would put the
    # request path at the mercy of how many microcaps happened to move today.
    page = max(limit, 100)
    walked = 0
    rows = []
    try:
        while len(rows) < limit and walked < MOVERS_WALK_CAP:
            if direction == "gainers":
                chunk = await raw.zrevrange(
                    MOVERS_DAY_ZSET, walked, walked + page - 1, withscores=True,
                )
            else:
                chunk = await raw.zrange(
                    MOVERS_DAY_ZSET, walked, walked + page - 1, withscores=True,
                )
            if not chunk:
                break
            walked += len(chunk)
            if min_market_cap_usd <= 0:
                rows.extend(chunk)
                break
            for member, score in chunk:
                if len(rows) >= limit:
                    break
                ticker = member.decode() if isinstance(member, bytes) else str(member)
                cap = await cached_market_cap(redis, ticker)
                # Unknown is excluded, not admitted. A gated board that quietly
                # contains unmeasured names cannot make the claim it exists to
                # make; the cost is that it is sparse until the radar's
                # backfill has resolved enough of the candidates.
                if cap is not None and cap >= min_market_cap_usd:
                    rows.append((member, score))
    except Exception as _exc:
        swallowed("api_gateway.routes.radar.movers", _exc, logger)
        raise HTTPException(status_code=503, detail="Movers store could not be read.")

    if not rows:
        # Empty is a real answer -- before the market opens there are no moves
        # -- and it is not the same as the store being unreachable, which is a
        # 503 above.
        #
        # The same shape as a populated board. This returned four keys against
        # the seven below, so a client reading `sector_concentration` found it
        # undefined for most of the day, which is when the board is empty.
        return {
            "direction": direction,
            "count": 0,
            "movers": [],
            "as_of": None,
            "news_window_hours": window_hours,
            "movers_without_news_in_window": [] if db else None,
            "sector_concentration": {},
            "min_market_cap_usd": min_market_cap_usd,
            "candidates_walked": walked,
        }

    movers = []
    as_of = None
    for member, score in rows:
        ticker = member.decode() if isinstance(member, bytes) else str(member)
        # day_pct is the percentage move and is what the board is ranked by;
        # market_cap_usd rides along so a reader can see what passed the gate
        # rather than having to trust that one was applied.
        entry = {"ticker": ticker, "day_pct": round(float(score), 4)}
        entry["market_cap_usd"] = await cached_market_cap(redis, ticker)
        # The decode loop this used to inline is `read_movers_snapshot`, whose
        # own docstring said it replaced two hand-written copies. It replaced
        # one; this was the other.
        detail = await read_movers_snapshot(redis, ticker)
        detail.pop("day_pct", None)
        entry.update(detail)
        movers.append(entry)

    try:
        state = await raw.hget(RADAR_SWEEP_STATE_KEY, "at")
        as_of = state.decode() if isinstance(state, bytes) else state
    except Exception as _exc:
        swallowed("api_gateway.routes.radar.movers_as_of", _exc, logger)

    # The two joins that make this something other than a stock screener.
    #
    # A ranked list of percentages is a commodity; anyone can buy one. What is
    # not a commodity is that this platform holds the other side of both
    # questions and has never joined them.
    tickers = [m["ticker"] for m in movers]
    # Whether the join ran, not whether a database object exists.
    #
    # This asked `if db`, so a present database whose query raised left every
    # `news_events` at None, `== 0` false for all of them, and the field
    # returned `[]` -- which reads as "every mover on this board has news". The
    # one distinction the code took care to draw was the one it got wrong.
    news_joined = await _attach_news_counts(db, movers, tickers, window_hours)
    sectors = await _attach_sectors(redis, movers, tickers)

    unexplained = [m["ticker"] for m in movers if m.get("news_events") == 0]
    return {
        "direction": direction,
        "count": len(movers),
        "movers": movers,
        "as_of": as_of,
        "news_window_hours": window_hours,
        # Named for what it is. A mover with no news in the window is not
        # "unexplained" as a fact about the world -- it is unexplained *by what
        # this platform collected*, which is a narrower and checkable claim,
        # and `news_events: null` says the join could not run at all.
        "movers_without_news_in_window": unexplained if news_joined else None,
        # Eight of twenty sharing a sector is a rotation, not eight stories.
        "sector_concentration": sectors,
    }


# News event types, as the correlation rules define them, minus the ones
# nothing emits.
#
# This was ("headline", "narrative_cluster", "social_signal"), taken from the
# news rule's trigger list so the join and the rules could not disagree about
# what counts as news. They agreed, and were both wrong about the same third of
# it: `narrative_cluster` has no producer -- first-story detection scores
# novelty and never emits a cluster event -- and the platform records that in
# shared/models/event_producers.py.
#
# The rules list it deliberately, beside the live types, so a clause keeps
# matching if a producer ever appears. A COUNT(*) has no such reason: an extra
# member of `= ANY(...)` that can never match is a column of zeros with a name
# that suggests otherwise. So the rule list is the source and the producer
# table is the filter.
_RULE_NEWS_TYPES = ("headline", "narrative_cluster", "social_signal")


def _news_types() -> tuple:
    from shared.models.event_producers import unproduced_values

    dead = unproduced_values()
    return tuple(t for t in _RULE_NEWS_TYPES if t not in dead)


_NEWS_TYPES = _news_types()

# Ticker-shaped words the news counter will not ask about. See the note in
# `_attach_news_counts`. US is deliberately kept -- it is a real subject.
_NON_SUBJECT_TICKERS = TICKER_SHAPED_STOPWORDS - {"US"}


async def _attach_news_counts(db, movers, tickers, window_hours: int) -> bool:
    """How many news events named each mover in the window.

    A count rather than a verdict. `named_entities` is the resolved ticker list
    the news enricher writes -- `affected_tickers` from the feed plus what the
    extractor found -- and it carries a GIN index, so this is one overlap query
    for the whole page rather than one per row.

    Null when there is no database: "the join did not run" and "no news" are
    different answers and the caller has to be able to tell them apart.
    """
    for mover in movers:
        mover["news_events"] = None
    if db is None or not tickers:
        return False

    # A ticker that is also an English word is not asked about.
    #
    # `named_entities` is matched against the board's own tickers, and both are
    # stored as bare uppercase tokens -- so a mover called ON, IT, A, ALL, SO or
    # ARE is indistinguishable from the preposition. The historical corpus
    # carries 23,173 rows of exactly that: the ticker extractor accepted any
    # short alphabetic word until it was corrected on 1 September, leaving THE
    # (10,200), ON (4,673) and A (7,169) among the most frequent "named
    # entities" the platform has ever stored. ON Semiconductor would have
    # counted every headline containing the word "on".
    #
    # It does not bite today: the contamination stops on 1 September and this
    # window caps at 168 hours, so the two no longer overlap. That is a fact
    # about today's date, not about the code, and it stops being true if the
    # window is ever widened or a writer regresses.
    #
    # TICKER_SHAPED_STOPWORDS is the writer's own list of ticker-shaped words it
    # refuses to emit, so excluding them here costs nothing that is currently
    # written and protects the count from everything that was. US is kept: the
    # correlation engine records it as a real subject, spaCy tagging "United
    # States".
    askable = [t for t in tickers if t and t.upper() not in _NON_SUBJECT_TICKERS]
    if not askable:
        for mover in movers:
            mover["news_events"] = 0
        return True

    try:
        rows = await db.query(
            """
            SELECT ticker, COUNT(*) AS n
            FROM (
                SELECT UNNEST(named_entities) AS ticker
                FROM events
                WHERE occurred_at >= NOW() - INTERVAL '1 hour' * $2
                  AND type = ANY($3)
                  AND named_entities && $1::text[]
            ) named
            WHERE ticker = ANY($1::text[])
            GROUP BY ticker
            """,
            askable, window_hours, list(_NEWS_TYPES),
        )
        counts = {r["ticker"]: int(r["n"] or 0) for r in (rows or [])}
        for mover in movers:
            mover["news_events"] = counts.get(mover["ticker"], 0)
        return True
    except Exception as _exc:
        swallowed("api_gateway.routes.radar.movers_news", _exc, logger)
        return False


async def _attach_sectors(redis, movers, tickers) -> dict:
    """Each mover's sector, and how concentrated the board is.

    From the reference data the enrichment service already caches per symbol.
    A sector nobody has resolved is "UNKNOWN" rather than omitted, so the
    concentration figures add up to the page.
    """
    concentration: dict = {}
    if redis is None:
        return concentration
    import json as _json

    from services.enrichment.ref_data import REFDATA_PREFIX

    raw = getattr(redis, "raw", redis)
    # One try per mover, not one around the loop.
    #
    # A single malformed cached blob at position three used to leave movers
    # four through twenty with no `sector` key and a concentration that no
    # longer added up to the page it describes -- which is the one property
    # this function was written to have.
    for mover in movers:
        sector = "UNKNOWN"
        try:
            cached = await raw.get(f"{REFDATA_PREFIX}{mover['ticker']}")
            if cached:
                payload = _json.loads(
                    cached.decode() if isinstance(cached, bytes) else cached
                )
                sector = (payload.get("sector") or "").strip() or "UNKNOWN"
        except Exception as _exc:
            swallowed("api_gateway.routes.radar.movers_sectors", _exc, logger)
        mover["sector"] = sector
        concentration[sector] = concentration.get(sector, 0) + 1
    return dict(sorted(concentration.items(), key=lambda kv: -kv[1]))


@router.get("/sweeps")
async def get_radar_sweeps_status(redis = Depends(get_redis_client)):
    """What the last sweep actually covered.

    Two numbers here were literals. `total_universe_scanned` was 4500 while the
    collector's own comments put the figure at 11,631, and `tracked_baselines`
    was `mean_count or 1840` -- where `mean_count` came from scanning
    `sentinel:radar:mean:*`, a prefix nothing writes. The collector writes
    `sentinel:radar:1m_mean:{ticker}`, so the scan matched nothing on every
    request and the constant was the answer, always.
    """
    mean_count = None
    sweep = {}
    if redis:
        try:
            cursor = 0
            mean_count = 0
            while True:
                cursor, keys = await redis.raw.scan(
                    cursor=cursor, match=f"{RADAR_BASELINE_PREFIX}*", count=500
                )
                mean_count += len(keys)
                if cursor == 0:
                    break
        except Exception as _exc:
            swallowed("api_gateway.routes.radar.baseline_scan", _exc, logger)
        try:
            raw_state = await redis.raw.hgetall(RADAR_SWEEP_STATE_KEY)
            sweep = {
                (k.decode() if isinstance(k, bytes) else k):
                (v.decode() if isinstance(v, bytes) else v)
                for k, v in (raw_state or {}).items()
            }
        except Exception as _exc:
            swallowed("api_gateway.routes.radar.sweep_state", _exc, logger)

    return {
        "status": "sweeping",
        "scanner": "Alpaca US Equities Snapshot API",
        # Null rather than a plausible number: the collector publishes what it
        # evaluated, and if it has not run there is nothing to report.
        "total_universe_scanned": int(sweep["evaluated"]) if sweep.get("evaluated") else None,
        "symbols_priced": int(sweep["priced"]) if sweep.get("priced") else None,
        "last_sweep_at": sweep.get("at"),
        "tracked_baselines": mean_count,
        "z_score_threshold": 3.0,
        "ewma_alpha": 0.05,
        "intraday_vwap_normalization": True,
    }


import aiohttp
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

SYMBOL_YAF_MAP = {
    "SPY": "SPY",
    "QQQ": "QQQ",
    "DJI": "^DJI",
    "VIX": "^VIX",
    "WTI": "CL=F",
    "BRENT": "BZ=F",
    "GLD": "GLD",
    "US30": "^TYX",
    "US30Y": "^TYX",
    "30YR": "^TYX",
    "30Y": "^TYX",
    "US10Y": "^TNX",
    "10YR": "^TNX",
    "10Y": "^TNX",
    "TLT": "TLT",
    "SHY": "SHY",
}

# How long an on-the-spot series stays good enough to reuse.
#
# These are intraday series behind a chart, and the alternative is a live call
# to the US Treasury or Yahoo on every page load: /market-series was measured at
# 15-60 seconds per request because it fetched up to six symbols from external
# APIs, uncached, every single time. A minute-old series is indistinguishable
# from a fresh one on a chart; a minute-long page load is not.
ON_THE_SPOT_CACHE_TTL_SEC = int(os.getenv("MARKET_SERIES_CACHE_TTL_SEC", "60"))


def _as_float(value, default=None):
    """A number, or the default. Never a fabricated stand-in."""
    try:
        if value is None or value == "":
            return default
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed == parsed else default   # NaN is not a measurement


def _first_price(financial: dict, crypto: dict):
    """The first genuinely quoted price across both payloads, or None.

    `a or b or c or 100.0` had two faults: a legitimate 0.0 fell through to the
    next candidate, and an event with no price at all plotted $100.
    """
    for source, key in (
        (financial, "current_price"), (financial, "close"),
        (crypto, "price"), (crypto, "mark_price"),
    ):
        value = _as_float((source or {}).get(key))
        if value is not None and value > 0:
            return value
    return None


# The canonical provenance envelope, as a plain dict so it can sit inside a
# series point without a per-point model construction.
#
# `source_type` on these points was an ad-hoc vocabulary --
# LIVE_US_TREASURY_2Y, SECONDARY_MARKET_YIELD, CACHED_QUOTE,
# PARAMETRIC_FALLBACK -- that no consumer understood and that shares neither
# spelling nor semantics with `ProvenanceSourceType`, the enum the frontend's
# ProvenanceBadge switches on. Two provenance vocabularies, and the reader saw
# neither. The specific label stays, because it says which of three feeds
# answered; the envelope is what the badge can actually render.
_PROV_LIVE_MEASUREMENT = {
    "source_type": ProvenanceSourceType.LIVE_MEASUREMENT.value,
    "methodology": "Direct read from the publishing venue or its cached quote",
}


async def fetch_on_the_spot_historical(symbol: str, limit: int = 60, redis = None):
    """Cached wrapper around the live fetch.

    /market-series calls this for every symbol it lacks data for, and the
    underlying function reaches out to the US Treasury and Yahoo on the request
    path. Uncached, the endpoint measured 15-60 seconds per request -- on the
    route behind every chart in the product. A minute-old intraday series is
    indistinguishable from a fresh one on a chart; a minute-long page load is
    not.

    Cache failures are never request failures: a miss, a Redis outage or a
    malformed entry all fall through to the live fetch.
    """
    symbol_upper = symbol.upper().strip()
    cache_key = f"sentinel:market_series:spot:{symbol_upper}:{limit}"

    if redis is not None:
        try:
            cached = await redis.raw.get(cache_key)
            if cached:
                parsed = json.loads(cached if isinstance(cached, str) else cached.decode("utf-8"))
                if isinstance(parsed, list):
                    return parsed
        except Exception as _exc:
            swallowed("api_gateway.routes.radar.fetch_on_the_spot_historical", _exc, logger)

    series = await _fetch_on_the_spot_uncached(symbol, limit, redis)

    if redis is not None and series:
        try:
            await redis.raw.set(cache_key, json.dumps(series, default=str), ex=ON_THE_SPOT_CACHE_TTL_SEC)
        except Exception as _exc:
            swallowed("api_gateway.routes.radar.fetch_on_the_spot_historical", _exc, logger)
    return series

async def _fetch_on_the_spot_uncached(symbol: str, limit: int = 60, redis = None):
    """
    Fetches real authentic historical price series on the spot from public APIs
    if no events currently persist in TimescaleDB for the requested symbol.
    Queries live Redis cache for latest collector quotes if external APIs are rate-limited.

    Results are cached briefly: without it every chart render re-fetched every
    symbol from a third-party API on the request path.
    """
    symbol_upper = symbol.upper().strip()

    # 1. Check 2-Year Treasury Yields via authentic live feeds (US Treasury Par-Yield API / FRED DGS2 / CBOE 2YY)
    if symbol_upper in ("US02Y", "US2Y", "2Y", "2YR", "DGS2"):
        # Tier 1: US Department of the Treasury Daily Par-Yield Curve API (primary official source)
        try:
            now_year = datetime.now(timezone.utc).year
            url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value={now_year}"
            headers = {"User-Agent": "Mozilla/5.0"}
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        xml_text = await resp.text()
                        tree = ET.fromstring(xml_text)
                        ns = {
                            'atom': 'http://www.w3.org/2005/Atom',
                            'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
                            'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices'
                        }
                        entries = tree.findall('.//atom:entry', ns)
                        pts = []
                        for entry in entries:
                            content = entry.find('atom:content', ns)
                            if content is not None:
                                props = content.find('m:properties', ns)
                                if props is not None:
                                    d_elem = props.find('d:NEW_DATE', ns)
                                    y_elem = props.find('d:BC_2YEAR', ns)
                                    if d_elem is not None and y_elem is not None and y_elem.text:
                                        try:
                                            val = float(y_elem.text)
                                            pts.append({
                                                "timestamp": d_elem.text,
                                                "price": round(val, 3),
                                                # A par yield is a published rate,
                                                # not a traded instrument. It has no
                                                # volume, and 1000.0 drew a real bar
                                                # on the chart for a quantity that
                                                # does not exist -- the same invention
                                                # already removed from the tier below.
                                                "volume": None,
                                                "anomaly_score": 0.0,
                                                "provider": "US Department of the Treasury (Par Yield)",
                                                "source_type": "LIVE_US_TREASURY_2Y",
                                                "provenance": _PROV_LIVE_MEASUREMENT
                                            })
                                        except ValueError as _exc:
                                            swallowed("api_gateway.routes.radar._fetch_on_the_spot_uncached", _exc, logger)
                        if pts:
                            return pts[-limit:]
        except Exception as e:
            logger.debug(f"US Treasury 2Y yield live fetch failed: {e}")

        # Tier 2: Secondary Labeled Market Source: CBOE 2-Year Treasury Note Yield (2YY=F)
        try:
            url = "https://query1.finance.yahoo.com/v8/finance/chart/2YY=F?range=5d&interval=5m"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        chart = data.get("chart", {}).get("result", [])[0]
                        timestamps = chart.get("timestamp", [])
                        indicators = chart.get("indicators", {}).get("quote", [])[0]
                        closes = indicators.get("close", [])
                        volumes = indicators.get("volume", [])
                        pts = []
                        for t, c, v in zip(timestamps, closes, volumes):
                            if c is not None:
                                ts_str = datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
                                raw_p = float(c)
                                if raw_p > 0:
                                    pts.append({
                                        "timestamp": ts_str,
                                        "price": round(raw_p, 3),
                                        # Absent volume is absent. `v or 1000` invented a round number
                                        # that renders as a real bar on the chart.
                                        "volume": _as_float(v),
                                        "anomaly_score": 0.0,
                                        "provider": "CBOE 2-Year Treasury Note Yield (2YY=F)",
                                        "source_type": "SECONDARY_MARKET_YIELD",
                                        "provenance": _PROV_LIVE_MEASUREMENT
                                    })
                        if pts:
                            return pts[-limit:]
        except Exception as e:
            logger.debug(f"Secondary 2Y yield fetch failed: {e}")

        # Tier 3: Live Redis Cache
        if redis:
            try:
                for rk in (symbol_upper, "US02Y", "US2Y", "2YR", "2Y"):
                    cached_p = await redis.raw.get(quote_key(rk))
                    if cached_p:
                        val = float(cached_p)
                        now_str = datetime.now(timezone.utc).isoformat()
                        return [{
                            "timestamp": now_str,
                            "price": val,
                            "volume": None,
                            "anomaly_score": 0.0,
                            "provider": "Sentinel Redis Cache",
                            "source_type": "CACHED_QUOTE",
                            "provenance": _PROV_LIVE_MEASUREMENT
                        }]
            except Exception as e:
                logger.debug(f"Redis latest quote fetch failed for {symbol}: {e}")

        # There is no Tier 4.
        #
        # This returned a hardcoded 4.15 under the label "Parametric Baseline
        # Yield". The label was honest and reached nobody: the chart component
        # plots `price`, and a constant invented by the gateway rendered
        # indistinguishably from a Treasury print. Three live sources failing
        # means the yield is unknown, and every other symbol on this endpoint
        # already says so by returning nothing.
        return []
    
    # 2. Check Crypto symbols via Coinbase Public Exchange Candles API (US-compliant, zero auth)
    if any(c in symbol_upper for c in ("BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK")) or symbol_upper.endswith("USDT") or symbol_upper.endswith("USD"):
        clean_base = symbol_upper.replace("USDT", "").replace("USD", "").strip() or "BTC"
        pair = f"{clean_base}-USD"
        url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity=60"
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        raw_candles = await resp.json()
                        pts = []
                        # Coinbase returns [time, low, high, open, close, volume] ordered newest to oldest
                        for k in reversed(raw_candles[:limit]):
                            ts_str = datetime.fromtimestamp(k[0], tz=timezone.utc).isoformat()
                            close_p = float(k[4])
                            vol = float(k[5])
                            pts.append({
                                "timestamp": ts_str,
                                "price": round(close_p, 2),
                                "volume": round(vol, 2),
                                "anomaly_score": 0.0
                            })
                        if pts:
                            return pts
        except Exception as e:
            logger.debug(f"Coinbase historical candle fetch failed for {symbol}: {e}")

    # 3. Check Equities, Commodities, Yields via Yahoo Finance v8 Chart API
    yf_symbol = SYMBOL_YAF_MAP.get(symbol_upper, symbol_upper)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_symbol}?range=1d&interval=5m"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    chart = data.get("chart", {}).get("result", [])[0]
                    timestamps = chart.get("timestamp", [])
                    indicators = chart.get("indicators", {}).get("quote", [])[0]
                    closes = indicators.get("close", [])
                    volumes = indicators.get("volume", [])

                    pts = []
                    for t, c, v in zip(timestamps, closes, volumes):
                        if c is not None:
                            ts_str = datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
                            raw_p = float(c)
                            pts.append({
                                "timestamp": ts_str,
                                "price": round(raw_p, 2),
                                # Absent volume is absent. `v or 1000` invented a round number
                                        # that renders as a real bar on the chart.
                                        "volume": _as_float(v),
                                "anomaly_score": 0.0
                            })
                    if pts:
                        return pts[-limit:]
    except Exception as e:
        logger.debug(f"Yahoo Finance historical fetch failed for {symbol}: {e}")

    # 4. Check Live Redis Collector Cache for authentic price
    if redis:
        try:
            cached_p = await redis.raw.get(quote_key(symbol_upper))
            if cached_p:
                val = float(cached_p)
                now_str = datetime.now(timezone.utc).isoformat()
                return [{
                    "timestamp": now_str,
                    "price": val,
                    "volume": None,
                    "anomaly_score": 0.0,
                    "provider": "Sentinel Redis Cache",
                    "source_type": "CACHED_QUOTE",
                    "provenance": _PROV_LIVE_MEASUREMENT
                }]
        except Exception as e:
            logger.debug(f"Redis latest quote fetch failed for {symbol}: {e}")

    return []


@router.get("/market-series")
async def get_market_series(
    symbols: Optional[str] = Query(None, description="Comma-separated symbols, e.g. TLT,IEF,SHY,BTCUSD,SPY,QQQ"),
    limit: int = Query(60, ge=10, le=300),
    db = Depends(get_db),
    redis = Depends(get_redis_client)
):
    """Retrieve intraday price series & financial telemetry for Bond Yields, BTC, SPY, QQQ."""
    target_symbols = [s.strip().upper() for s in (symbols.split(",") if symbols else ["TLT", "IEF", "SHY", "BTCUSD", "SPY", "QQQ"])]
    
    series_data: Dict[str, Any] = {}
    if db:
        try:
            rows = await db.query(
                """
                -- Matched on upper(column), served by
                -- events_entity_id_upper_time_idx and
                -- events_entity_name_upper_time_idx (migration 0009).
                --
                -- Neither column holds a consistent casing -- each collector
                -- spells primary_entity_id its own way, which is why the row
                -- handler below has to .upper() what it reads back. A plain `=`
                -- against the caller's upper-cased symbols missed every row a
                -- collector wrote in lower case and returned a blank chart.
                --
                -- MATERIALIZED is load-bearing, not decoration. Measured on
                -- this deployment: with the entity filter and
                -- `ORDER BY occurred_at DESC LIMIT` in one query, the planner
                -- takes the LIMIT as licence to walk events_occurred_at_idx
                -- backwards and filter as it goes -- expecting to hit 60
                -- matching rows quickly. The entity predicate is far too
                -- selective for that: only 196 rows in the whole 7-day window
                -- match, so it walked most of the hypertable and the query took
                -- 11.8 seconds with both new indexes sitting unused. Fencing
                -- the selection into a materialised CTE makes it choose the
                -- BitmapOr over the two expression indexes and sort 196 rows
                -- afterwards: 107ms, and the fence is what holds the plan.
                WITH matched AS MATERIALIZED (
                    SELECT primary_entity_id, primary_entity_name, occurred_at,
                           anomaly_score, financial_data, crypto_data
                    FROM events
                    WHERE (upper(primary_entity_id) = ANY($1::text[])
                           OR upper(primary_entity_name) = ANY($1::text[]))
                      AND occurred_at > NOW() - INTERVAL '7 days'
                )
                SELECT primary_entity_id, primary_entity_name, occurred_at, anomaly_score,
                       financial_data, crypto_data
                FROM matched
                ORDER BY occurred_at DESC
                LIMIT $2;
                """,
                target_symbols,
                limit * len(target_symbols)
            )
            for r in rows:
                sym = (r["primary_entity_id"] or r["primary_entity_name"] or "UNKNOWN").upper()
                if sym not in series_data:
                    series_data[sym] = []
                
                fin = r.get("financial_data") or {}
                cryp = r.get("crypto_data") or {}
                # No invented price. This ended `or 100.0`, so any event without
                # a usable price plotted a flat $100 line on the chart -- a
                # fabricated quote presented as market data, and one that looks
                # entirely plausible next to a real series. A point with no price
                # is skipped instead: a gap is honest, a made-up level is not.
                price = _first_price(fin, cryp)
                if price is None:
                    continue
                
                series_data[sym].append({
                    "timestamp": r["occurred_at"].isoformat() if hasattr(r["occurred_at"], "isoformat") else str(r["occurred_at"]),
                    "price": float(price),
                    # Volume genuinely absent is reported as absent, not as the
                    # invented 1000 that used to stand in for it.
                    "volume": _as_float(fin.get("volume"), _as_float(cryp.get("volume"))),
                    "anomaly_score": float(r["anomaly_score"] or 0.0)
                })
        except Exception as e:
            logger.warning(f"Error fetching market series from DB: {e}")

    # Fetch on-the-spot historical ticks for any target symbol with missing or insufficient DB events
    fetch_tasks = []
    missing_symbols = []
    for sym in target_symbols:
        if sym not in series_data or len(series_data[sym]) < 5:
            missing_symbols.append(sym)
            fetch_tasks.append(fetch_on_the_spot_historical(sym, limit, redis))

    if fetch_tasks:
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        for sym, res in zip(missing_symbols, results):
            if isinstance(res, list) and res:
                series_data[sym] = res
    # Mirror canonical key aliases for seamless frontend component matching
    alias_map = {
        "BTC": ["BTCUSD", "BTCUSDT"],
        "BTCUSD": ["BTC", "BTCUSDT"],
        "ETH": ["ETHUSD", "ETHUSDT"],
        "ETHUSD": ["ETH", "ETHUSDT"],
        "30YR": ["US30Y", "US30", "30Y"],
        "US30Y": ["30YR", "US30", "30Y"],
        "US30": ["US30Y", "30YR", "30Y"],
        "2YR": ["US02Y", "US2Y", "2Y"],
        "US02Y": ["2YR", "US2Y", "2Y"],
        "US2Y": ["2YR", "US02Y", "2Y"],
        "10YR": ["US10Y", "10Y"],
        "US10Y": ["10YR", "10Y"],
    }
    for orig_key in list(series_data.keys()):
        if series_data[orig_key]:
            for alias in alias_map.get(orig_key, []):
                if alias not in series_data or not series_data[alias]:
                    series_data[alias] = series_data[orig_key]

    return {
        "symbols": target_symbols,
        "series": series_data
    }
import json
from fastapi import HTTPException
from shared.utils.quiet_failures import swallowed

@router.get("/candles/{ticker}")
async def get_candles(
    ticker: str,
    timeframe: str = Query("1m", description="Options: 1m, 5m, 10m, 15m, 30m, 1h, 4h, 1d, 1w, 1M"),
    limit: int = Query(100, ge=1, le=1000),
    db = Depends(get_db_optional),
    redis = Depends(get_redis_optional)
):
    """
    Retrieve aggregated OHLCV candlesticks for a specific ticker across multiple timeframes.
    Redis/Lua multi-timeframe aggregator serves the low-latency hot cache (§2.5).
    TimescaleDB Continuous Aggregates (tradfi_bars_*) serve as durable fallback and historical source of truth.
    """
    valid_timeframes = {"1m", "5m", "10m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"}
    if timeframe not in valid_timeframes:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe. Must be one of: {valid_timeframes}")
        
    ticker = ticker.upper()
    candles = []
    source = "none"
    
    ticker_candidates = [ticker]
    alias_map = {
        "2YR": ["US02Y", "US2Y", "2Y", "SHY"],
        "2Y": ["US02Y", "US2Y", "2YR", "SHY"],
        "US2Y": ["US02Y", "2YR", "2Y", "SHY"],
        "US02Y": ["US2Y", "2YR", "2Y", "SHY"],
        "30YR": ["US30Y", "US30", "30Y"],
        "30Y": ["US30Y", "US30", "30YR"],
        "US30": ["US30Y", "30YR", "30Y"],
        "US30Y": ["US30", "30YR", "30Y"],
        "BTC": ["BTCUSD", "BTCUSDT"],
        "BTCUSD": ["BTC", "BTCUSDT"],
        "ETH": ["ETHUSD", "ETHUSDT"],
        "ETHUSD": ["ETH", "ETHUSDT"],
    }
    for a in alias_map.get(ticker, []):
        if a not in ticker_candidates:
            ticker_candidates.append(a)

    # 1. Hot path: Query Redis multi-timeframe cache
    if redis:
        try:
            # Both spellings of the same duration are tried. Producers disagreed
            # -- equities wrote "1h"/"4h" and crypto wrote "60m"/"240m" -- so
            # asking for 1h on a crypto pair found nothing and the chart came
            # back empty with a 200.
            # Built literally, not through candle_cache_key: that helper
            # normalises every alias back to the canonical label, so routing the
            # alternates through it produced the same key each time and the
            # legacy spelling was never actually queried.
            key_candidates = [
                f"{CANDLE_KEY_PREFIX}:{tf_alias}:{t_cand.upper()}"
                for t_cand in ticker_candidates
                for tf_alias in timeframe_aliases(timeframe)
            ]
            for key in key_candidates:
                raw_candles = await redis.raw.lrange(key, 0, limit - 1)
                if raw_candles:
                    for rc in raw_candles:
                        try:
                            candles.append(normalize_candle(json.loads(rc), ticker))
                        except Exception as _exc:
                            swallowed("api_gateway.routes.radar.get_candles", _exc, logger)
                    if candles:
                        source = "redis"
                        break
        except Exception as e:
            logger.warning(f"Error fetching candles for {ticker} from Redis: {e}")

    # 2. Durable fallback: Query TimescaleDB Continuous Aggregates (§2.1, §2.3, §2.5)
    if not candles and db:
        try:
            # Only these have continuous aggregates. 10m, 30m and 4h are served
            # from the Redis aggregator alone; there is no durable fallback for
            # them, which is worth knowing when a chart is empty.
            cagg_map = {
                "1m": ("tradfi_bars", "time"),
                "5m": ("tradfi_bars_5m", "bucket_time"),
                "15m": ("tradfi_bars_15m", "bucket_time"),
                "1h": ("tradfi_bars_1h", "bucket_time"),
                "1d": ("tradfi_bars_1d", "bucket_time"),
                "1w": ("tradfi_bars_1w", "bucket_time"),
                "1M": ("tradfi_bars_1mth", "bucket_time"),
            }
            timeframe = normalize_timeframe(timeframe)
            if timeframe in cagg_map:
                table_name, time_col = cagg_map[timeframe]
                rows = await db.query(
                    f"""
                    SELECT {time_col} as ts, open, high, low, close, volume
                    FROM {table_name}
                    WHERE ticker = $1
                    ORDER BY {time_col} DESC
                    LIMIT $2;
                    """,
                    ticker, limit
                )
                if rows:
                    source = f"timescale:{table_name}"
                for r in rows:
                    candles.append({
                        "ts": r["ts"].isoformat() if hasattr(r["ts"], "isoformat") else str(r["ts"]),
                        "open": float(r["open"]),
                        "high": float(r["high"]),
                        "low": float(r["low"]),
                        "close": float(r["close"]),
                        "volume": float(r["volume"]),
                        "ticker": ticker
                    })
        except Exception as db_err:
            # Warning, not debug. This is the durable path behind every chart on
            # the platform, and a failure here returns an empty series with a
            # 200 -- indistinguishable from "this ticker has no data". It was
            # logged below the default level, so the charts backbone could fail
            # continuously without leaving a trace.
            logger.warning(
                "Market-series DB query failed for %s %s: %s",
                ticker, timeframe, db_err, exc_info=True,
            )

    # Which path served this, recorded on the response. A chart that comes back
    # empty is otherwise indistinguishable from a ticker with no data, and the
    # two have completely different fixes.
    if not candles:
        logger.warning(
            "Market-series empty for %s %s (redis=%s, db=%s)",
            ticker, timeframe, redis is not None, db is not None,
        )

    return {
        "ticker": ticker,
        "timeframe": timeframe,
        "count": len(candles),
        "source": source,
        "candles": candles
    }


@router.get("/options/covered-calls")
async def get_covered_call_recommendations(
    ticker: str = Query("NVDA"),
    z_score: Optional[float] = Query(None, description="CAGG Z-score. If omitted, queries tradfi_bars_5m_zscore view."),
    current_price: Optional[float] = Query(None, description="Current spot price of underlying equity"),
    target_delta: float = Query(0.30),
    dte_days: int = Query(30),
    db = Depends(get_db_optional),
    redis = Depends(get_redis_optional)
):
    """Generates a closed-form Black-Scholes covered-call recommendation (§3.4 Phase 3 Flagship Feature)."""
    from shared.utils import quant_calc
    from fastapi import HTTPException
    
    # If z_score is omitted, query the durable TimescaleDB continuous aggregate Z-score view (§2.3, §2.6)
    if z_score is None:
        if db:
            try:
                row = await db.query_one(
                    """
                    SELECT z_score FROM tradfi_bars_5m_zscore 
                    WHERE ticker = $1 
                    ORDER BY bucket_time DESC 
                    LIMIT 1;
                    """,
                    ticker.upper()
                )
                if row and row.get("z_score") is not None:
                    z_score = float(row["z_score"])
            except Exception as e:
                logger.debug(f"Failed to query Z-score view for {ticker}: {e}")
        if z_score is None:
            # Not 2.8. `generate_covered_call_recommendation` returns None
            # below +2.5, so that literal was the one value that guaranteed
            # every ticker cleared the significance gate this endpoint exists
            # to apply -- on a cold or unavailable database, which is exactly
            # when nothing has been measured.
            #
            # The asymmetry was the tell: twenty lines down, a missing price
            # raises a 400 rather than being invented. The z-score decides
            # whether there is a trade at all, and it was the one being
            # supplied. The agent that calls the same function starts from 0.0
            # and computes from returns when the view is empty -- it fails
            # closed, and so does this now.
            raise HTTPException(
                status_code=400,
                detail=(
                    f"No 5m Z-score available for '{ticker}' -- the "
                    f"tradfi_bars_5m_zscore aggregate has no row for it. Pass "
                    f"'z_score' explicitly, or wait for the aggregate to fill."
                ),
            )

    # If current_price is omitted or non-positive, look up real cached price from Redis
    if current_price is None or current_price <= 0:
        if redis:
            try:
                for cand in (ticker.upper(), ticker):
                    raw_p = await redis.raw.get(quote_key(cand))
                    if raw_p:
                        current_price = float(raw_p)
                        break
            except Exception as e:
                logger.debug(f"Redis latest quote fetch failed for {ticker}: {e}")

    # Return explicit error if no live price is available
    if current_price is None or current_price <= 0:
        raise HTTPException(
            status_code=400,
            detail=f"No live price cached for '{ticker}'. 'current_price' query parameter required."
        )

    live_iv = None
    if redis:
        try:
            raw_iv = await redis.raw.get(f"sentinel:options:iv:{ticker}")
            if raw_iv:
                live_iv = float(raw_iv)
        except Exception as _exc:
            swallowed("api_gateway.routes.radar.get_covered_call_recommendations", _exc, logger)

    rec = quant_calc.generate_covered_call_recommendation(
        ticker=ticker,
        current_price=current_price,
        z_score=z_score,
        target_delta=target_delta,
        dte_days=dte_days,
        live_iv=live_iv
    )
    return rec or {"status": "GATED_OR_INVALID", "message": f"Covered call overlay requires CAGG Z >= 2.5 or valid ticker (Z={z_score})"}

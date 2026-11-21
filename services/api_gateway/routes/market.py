"""
services/api_gateway/routes/market.py

Market-wide views for the dashboard: breadth over time (shared/utils/breadth.py) over stocks with a known market cap of $1bn or more.
Computed in SQL from the official daily bars (history to 2016), cached, one value per completed session.
"""
import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils import breadth as br
from shared.utils import detached
from shared.utils.market_cap import MODEL_MIN_MARKET_CAP_USD, large_caps
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cached_json

logger = logging.getLogger("api.market")
router = APIRouter(prefix="/api/v1/market", tags=["Market"])

async def _detached_cache(raw: Any, key: str, compute: Any, ttl: float, max_stale: float) -> Any:
    """The cached value, or one detached computation shared by every caller: a cold cache costs about 20 to 45 s, and a viewer that gave up used to
    cancel it so the next one started over. After 20 s a caller gets 503 'still computing' with Retry-After while the work carries on and fills the cache."""
    try:
        return await detached.run(key, lambda: cached_json(raw, key, compute, ttl=ttl, max_stale=max_stale), wait_s=20.0)
    except detached.Warming:
        raise HTTPException(status_code=503, detail="Still computing for the first time; try again in about a minute.",
                            headers={"Retry-After": "30"})


MAX_BREADTH_DAYS = 120          # the longest series computed once; 250 sessions timed out the window query (13 Nov 2026)
_BREADTH_SQL = """
    WITH u AS (
        SELECT ticker, day, close, high, low FROM daily_bars
        WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - $2::int AND close > 0
    ), w AS (
        SELECT day, close, high, low,
               lag(close) OVER p AS pc,
               avg(close) OVER (p ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50,
               count(*) OVER (p ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS c50,
               avg(close) OVER (p ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
               count(*) OVER (p ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS c200,
               max(high) OVER (p ROWS BETWEEN 251 PRECEDING AND 1 PRECEDING) AS hh,
               min(low) OVER (p ROWS BETWEEN 251 PRECEDING AND 1 PRECEDING) AS ll,
               count(*) OVER (p ROWS BETWEEN 251 PRECEDING AND 1 PRECEDING) AS c252
        FROM u WINDOW p AS (PARTITION BY ticker ORDER BY day)
    )
    SELECT day, count(*) AS n, count(pc) AS n_prev,
           count(*) FILTER (WHERE pc IS NOT NULL AND close > pc) AS adv,
           count(*) FILTER (WHERE pc IS NOT NULL AND close < pc) AS dec,
           count(*) FILTER (WHERE c252 >= 240) AS n_52,
           count(*) FILTER (WHERE c252 >= 240 AND high > hh) AS hi52,
           count(*) FILTER (WHERE c252 >= 240 AND low < ll) AS lo52,
           count(*) FILTER (WHERE c50 >= 50) AS n_50,
           count(*) FILTER (WHERE c50 >= 50 AND close > ma50) AS above50,
           count(*) FILTER (WHERE c200 >= 200) AS n_200,
           count(*) FILTER (WHERE c200 >= 200 AND close > ma200) AS above200
    FROM w GROUP BY day ORDER BY day DESC LIMIT $3
"""


@router.get("/breadth")
async def breadth(
    days: int = Query(60, ge=5, le=120),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """{as_of, universe, series:[{day, n, advancers, decliners, unchanged, share_up, new_52w_highs, new_52w_lows, share_above_50d,
    share_above_200d}], dropped_thin, note}: one row per completed session, oldest first, over stocks with a known cap of $1bn or more.
    A session where under 80% of the usual number of stocks reported is dropped and listed in dropped_thin. Shares are fractions (0-1);
    a measure without enough history is null. Intraday breadth is not stored before 13 Jul 2026, so this is daily only."""
    if db is None or redis is None:
        raise HTTPException(status_code=503, detail="Market breadth is temporarily unavailable.")
    raw = getattr(redis, "raw", redis)

    async def compute() -> Dict[str, Any]:
        names = await large_caps(raw, MODEL_MIN_MARKET_CAP_USD)
        if not names:
            raise HTTPException(status_code=503, detail="The market-cap table is not loaded; breadth cannot be measured honestly yet.")
        try:
            rows = await db.query(_BREADTH_SQL, sorted(names), 380 + int(1.5 * MAX_BREADTH_DAYS), MAX_BREADTH_DAYS + 15) or []
        except Exception as exc:
            swallowed("api_gateway.market.breadth", exc, logger)
            raise HTTPException(status_code=503, detail="Market breadth is temporarily unavailable.")
        out = br.build_series([dict(r) for r in rows], MAX_BREADTH_DAYS)
        series = out["series"]
        return {"as_of": series[-1]["day"] if series else None, "universe": len(names), "series": series,
                "dropped_thin": out["dropped_thin"],
                "note": "Stocks with a known market cap of $1bn or more; official daily bars; sessions with thin coverage are dropped."}

    # One window query for every `days`: the longest series is computed and cached once and sliced per request (a key per `days` was an
    # unbounded set of 1M-row window queries).
    data = await _detached_cache(raw, f"api:market:breadth:{MAX_BREADTH_DAYS}", compute, ttl=6 * 3600, max_stale=3 * 86400)
    series = data["series"][-days:]
    return {**data, "series": series, "as_of": series[-1]["day"] if series else None}


HEAT_CAP_STEPS = (1e8, 3e8, 5e8, 1e9, 2e9, 5e9, 1e10, 2e10, 5e10, 1e11)
_PROFILE_SQL = "SELECT ticker, sic, sic_description FROM company_profile WHERE ticker = ANY($1::text[])"


async def _heat_stocks(db: Any, raw: Any, min_cap: float) -> Dict[str, Any]:
    """Every stock at or above `min_cap` with its sector, industry, day change, volume ratio and cap; one pass, cached by the caller."""
    from datetime import datetime, timezone

    from services.api_gateway.routes.screener import _BARS_SQL, _DAILY_SQL
    from shared.utils import market_quotes as mq
    from shared.utils import screener as sc
    from shared.utils.market_cap import CAPS_KEY
    from shared.utils.sectors import sector_of

    table = await raw.hgetall(CAPS_KEY) or {}
    caps = {(k.decode() if isinstance(k, bytes) else str(k)): float(v.decode() if isinstance(v, bytes) else v) for k, v in table.items()}
    names = sorted(t for t, c in caps.items() if c >= min_cap)
    if not names:
        raise HTTPException(status_code=503, detail="The market-cap table is not loaded; the heat map cannot be built honestly yet.")
    now = datetime.now(timezone.utc)
    try:
        bars = {str(r["ticker"]).upper(): (r["time"], r["close"]) for r in await db.query(_BARS_SQL, names) or []}
        daily: Dict[str, list] = {}
        vols: Dict[str, list] = {}
        for r in await db.query(_DAILY_SQL, names) or []:
            t = str(r["ticker"]).upper()
            daily.setdefault(t, []).append((r["day"], r["close"]))
            vols.setdefault(t, []).append(r["volume"])
        profile = {str(r["ticker"]).upper(): r for r in await db.query(_PROFILE_SQL, names) or []}
    except Exception as exc:
        swallowed("api_gateway.market.heatmap", exc, logger)
        raise HTTPException(status_code=503, detail="The heat map is temporarily unavailable.")
    session = mq.build_quote("SPY", None, [], now)["session"]
    stocks = []
    for t in names:
        closes = [float(c) for _, c in daily.get(t, [])]
        if session == "regular":
            change = mq.build_quote(t, bars.get(t), daily.get(t, []), now)["change_pct"]
        else:                                     # outside the regular session the day's move is the last completed session's
            change = round((closes[-1] / closes[-2] - 1.0) * 100.0, 2) if len(closes) >= 2 and closes[-2] > 0 else None
        p = profile.get(t) or {}
        stocks.append({"ticker": t, "sector": sector_of(p.get("sic")), "industry": p.get("sic_description"), "change_pct": change,
                       "market_cap": caps[t], "volume_vs_20d": sc.volume_vs_20d(vols.get(t, []))})
    return {"as_of": now.isoformat(), "session": session, "stocks": stocks}


@router.get("/heatmap")
async def heatmap(
    by: str = Query("sector", pattern="^(sector|industry)$"),
    min_cap: float = Query(1e9, ge=1e8, description="Minimum market cap in USD; stocks with an unknown cap are excluded."),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """{as_of, session, by, universe, groups:[{key, label, n, n_priced, market_cap, change_pct (cap-weighted), change_pct_median,
    breadth_up_share, volume_vs_20d_median, top:[{ticker, change_pct}], bottom:[...]}], note}. Sectors are a fixed mapping from SEC
    industry codes (SIC), an approximation of GICS (Alphabet and Meta read as Information Technology); industry is the SEC's own text, groups
    of fewer than 5 stocks omitted. change_pct is the day's move: live against the last completed close during the regular session, and the
    last completed session's close-to-close move at any other time (check `session`). Statistics are null for a group with fewer than 3
    priced stocks. Stocks whose market cap is unknown, or implausible against their trading volume, are not included."""
    from shared.utils.sectors import aggregate

    if db is None or redis is None:
        raise HTTPException(status_code=503, detail="The heat map is temporarily unavailable.")
    # The cap threshold snaps down to one of a fixed set so the cache holds a handful of universes, not one per typed number.
    min_cap = max([c for c in HEAT_CAP_STEPS if c <= min_cap] or [HEAT_CAP_STEPS[0]])
    raw = getattr(redis, "raw", redis)
    data = await _detached_cache(raw, f"api:market:heatstocks:{int(min_cap)}", lambda: _heat_stocks(db, raw, min_cap), ttl=60, max_stale=600)
    rows = []
    for s in data["stocks"]:
        key = s["sector"] if by == "sector" else s["industry"]
        if key:
            rows.append({**s, "key": key, "label": key})
    groups = aggregate(rows, by, min_group=3)
    if by == "industry":
        groups = [g for g in groups if g["n"] >= 5]
    return {"as_of": data["as_of"], "session": data["session"], "by": by, "min_cap": min_cap, "min_cap_used": min_cap, "universe": len(data["stocks"]), "groups": groups,
            "note": "Sectors map SEC industry codes (SIC) and approximate GICS; outside the regular session change is the last completed session's move."}

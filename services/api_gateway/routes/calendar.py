"""
services/api_gateway/routes/calendar.py

The earnings calendar: report dates the vendor lists for the next N days, for every stock, the equities watchlist, the traded book (B1) or
tickers the client names (shared/utils/earnings_calendar.py). Inferred dates are not mixed in; the snapshot route carries those.
"""
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Set

from fastapi import APIRouter, Depends, HTTPException, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils import earnings_calendar as ec
from shared.utils import market_quotes as mq
from shared.utils.market_cap import CAPS_KEY
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cached_json
from shared.utils.watchlists import WATCHED_EQUITIES_KEY

logger = logging.getLogger("api.calendar")
router = APIRouter(prefix="/api/v1/calendar", tags=["Calendar"])

_ROWS_SQL = """
    SELECT DISTINCT ON (ticker, report_date) ticker, report_date, hour, eps_estimate, revenue_estimate FROM earnings_estimates
    WHERE snap_day = (SELECT max(snap_day) FROM earnings_estimates) AND report_date BETWEEN $1 AND $2
    ORDER BY ticker, report_date
"""


async def _held_symbols(redis: Any, raw: Any) -> Set[str]:
    """Symbols held in the Alpaca paper account (the traded book), cached a minute: the broker call is slow."""
    from shared.broker import get_broker

    async def compute() -> Dict[str, Any]:
        positions = await get_broker("alpaca", redis_client=redis).get_positions()
        return {"symbols": sorted({str(p.symbol).upper() for p in positions})}

    return set((await cached_json(raw, "api:calendar:held", compute, ttl=60, max_stale=600))["symbols"])


@router.get("/earnings")
async def earnings(
    days: int = Query(7, ge=1, le=ec.MAX_DAYS),
    scope: str = Query("all", pattern="^(all|watchlist|traded)$"),
    tickers: Optional[str] = Query(None, description="Comma-separated tickers; overrides scope."),
    min_cap: Optional[float] = Query(None, ge=0, description="Minimum market cap in USD; unknown caps are excluded."),
    limit: int = Query(200, ge=1, le=ec.MAX_ROWS),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """{from, to, scope, count, rows:[{ticker, date, hour (bmo|amc|during_market|null), eps_estimate, revenue_estimate, estimated:false,
    market_cap}]}: listed report dates only, soonest first, larger companies first within a day. scope=traded is the stocks held in the Alpaca paper account,
    scope=watchlist the equities watchlist; `tickers` names stocks explicitly. Missing numbers are null."""
    if db is None or redis is None:
        raise HTTPException(status_code=503, detail="The calendar is temporarily unavailable.")
    raw = getattr(redis, "raw", redis)
    start, end = ec.window(datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).date(), days)
    universe: Optional[Set[str]] = None
    label = scope
    if tickers:
        universe, label = set(mq.clean_list(tickers)), "tickers"
    elif scope == "traded":
        try:
            universe = await _held_symbols(redis, raw)
        except Exception as exc:
            swallowed("api_gateway.calendar.book", exc, logger)
            raise HTTPException(status_code=503, detail="The traded book could not be read just now.")
    elif scope == "watchlist":
        try:
            universe = {(m.decode() if isinstance(m, bytes) else str(m)).upper() for m in await raw.zrange(WATCHED_EQUITIES_KEY, 0, -1)}
        except Exception as exc:
            swallowed("api_gateway.calendar.watchlist", exc, logger)
            raise HTTPException(status_code=503, detail="The watchlist could not be read just now.")

    async def compute() -> Dict[str, Any]:
        try:
            return {"rows": [dict(r) for r in await db.query(_ROWS_SQL, start, end) or []]}
        except Exception as exc:
            swallowed("api_gateway.calendar.rows", exc, logger)
            raise HTTPException(status_code=503, detail="The calendar is temporarily unavailable.")

    data = await cached_json(raw, f"api:calendar:earnings:{start.isoformat()}:{end.isoformat()}", compute, ttl=600, max_stale=3600)
    names = sorted({str(r["ticker"]).upper() for r in data["rows"]} if universe is None else universe)
    caps: Dict[str, float] = {}
    try:
        for t, v in zip(names, await raw.hmget(CAPS_KEY, names) if names else []):
            if v is not None:
                caps[t] = float(v.decode() if isinstance(v, bytes) else v)
    except Exception as exc:
        swallowed("api_gateway.calendar.caps", exc, logger)
    rows = ec.build_rows(data["rows"], start, end, universe, caps, min_cap, limit)
    return {"from": start.isoformat(), "to": end.isoformat(), "scope": label, "count": len(rows), "rows": rows}

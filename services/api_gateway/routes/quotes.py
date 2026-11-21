"""
services/api_gateway/routes/quotes.py

One price source for the dashboard: last price, its age, the session, a delayed flag and the close the day's move is measured from
(shared/utils/market_quotes.py). Hero, chart and compare read this so they agree.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils import market_quotes as mq
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cached_json

logger = logging.getLogger("api-gateway.quotes")
router = APIRouter(prefix="/api/v1/quotes", tags=["Quotes"])

_BARS_SQL = """
    SELECT DISTINCT ON (ticker) ticker, time, close FROM tradfi_bars
    WHERE ticker = ANY($1::text[]) AND time > NOW() - INTERVAL '10 days' AND close > 0
    ORDER BY ticker, time DESC
"""
_DAILY_SQL = """
    SELECT ticker, day, close FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 12 AND close > 0
    ORDER BY ticker, day
"""


@router.get("")
async def quotes(
    tickers: str = Query(..., description="Comma-separated tickers, at most 50."),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Quotes with age. `price` is the newest minute bar's close (else the last official close), `as_of` its time, `delayed` true when a
    live session's newest bar is over 20 minutes old (the free feed lags about 15). `change_pct` is measured from `prev_close`, the last
    completed session's close, dated `change_base_date`. Any field that cannot be known is null."""
    names: List[str] = mq.clean_list(tickers)
    if not names:
        raise HTTPException(status_code=422, detail="Name at least one ticker.")
    if db is None:
        raise HTTPException(status_code=503, detail="Quotes are temporarily unavailable.")
    raw = getattr(redis, "raw", redis)

    async def compute() -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        try:
            bars = {str(r["ticker"]).upper(): (r["time"], r["close"]) for r in await db.query(_BARS_SQL, names) or []}
            daily: Dict[str, list] = {}
            for r in await db.query(_DAILY_SQL, names) or []:
                daily.setdefault(str(r["ticker"]).upper(), []).append((r["day"], r["close"]))
        except Exception as exc:
            swallowed("api_gateway.quotes.read", exc, logger)
            raise HTTPException(status_code=503, detail="Quotes are temporarily unavailable.")
        return {"as_of": now.isoformat(), "quotes": [mq.build_quote(t, bars.get(t), daily.get(t, []), now) for t in names]}

    return await cached_json(raw, "api:quotes:" + ",".join(sorted(names)), compute, ttl=5, max_stale=30)

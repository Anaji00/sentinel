"""Chart markers for a stock: GET /api/v1/stocks/{ticker}/events?days=365 -> [{date, kind, label, url?}] (kind: earnings, filing, alert).

Stored rows only (earnings_estimates, filing events, alert-tier correlations), no model call, cached five minutes. shared/utils/stock_events.py builds the list."""

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils import closes_batch as cb
from shared.utils import stock_events as se
from shared.utils.quiet_failures import dropped as count_dropped
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cache_key, cached_json
from shared.utils.stock_peers import clean_ticker

logger = logging.getLogger("api.stock_events")

router = APIRouter(prefix="/api/v1/stocks", tags=["Stock events"])

_EARNINGS_SQL = """
    SELECT DISTINCT ON (report_date) report_date, eps_actual, eps_estimate FROM earnings_estimates
    WHERE ticker = $1 AND report_date >= CURRENT_DATE - $2::int AND report_date <= CURRENT_DATE AND eps_actual IS NOT NULL
    ORDER BY report_date DESC, snap_day DESC LIMIT 60
"""
_FILINGS_SQL = """
    SELECT occurred_at, filing_data->>'form_type' AS form_type, coalesce(filing_data->>'primary_doc_url', url) AS url FROM events
    WHERE type = 'filing' AND primary_entity_id = $1 AND occurred_at >= now() - make_interval(days => $2) ORDER BY occurred_at DESC LIMIT 200
"""
_ALERTS_SQL = """
    SELECT detected_at, rule_name FROM correlations
    WHERE primary_entity_id = $1 AND alert_tier >= 2 AND detected_at >= now() - make_interval(days => $2) ORDER BY detected_at DESC LIMIT 200
"""


@router.get("/{ticker}/events")
async def stock_events(
    ticker: str,
    days: int = Query(365, ge=7, le=1825),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
) -> List[Dict[str, Any]]:
    """Dated earnings, filing and alert markers for the chart, newest first; a part that fails to read is left out and counted, never faked."""
    t = clean_ticker(ticker)
    if not t:
        raise HTTPException(status_code=422, detail="Not a ticker.")
    if db is None:
        raise HTTPException(status_code=503, detail="Stock events are temporarily unavailable.")

    async def compute() -> List[Dict[str, Any]]:
        parts: Dict[str, list] = {}
        for name, sql in (("earnings", _EARNINGS_SQL), ("filings", _FILINGS_SQL), ("alerts", _ALERTS_SQL)):
            try:
                parts[name] = [dict(r) for r in await db.query(sql, t, days) or []]
            except Exception as exc:
                swallowed(f"api_gateway.stock_events.{name}", exc, logger, detail=t)
                parts[name] = []
        events, undated = se.build(parts["earnings"], parts["filings"], parts["alerts"], days)
        if undated:
            count_dropped("api_gateway.stock_events.undated", "row without a readable date left off the chart", logger, detail=f"{t} {undated}")
        return [e.model_dump(mode="json", exclude_none=True) for e in events]

    return await cached_json(getattr(redis, "raw", None), cache_key("stock_events", ticker=t, days=days), compute, ttl=300, max_stale=3600)


_CLOSES_SQL = """
    SELECT ticker, day, close FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day >= CURRENT_DATE - $2::int AND close > 0 ORDER BY ticker, day
"""


@router.get("/closes")
async def recent_closes(
    tickers: str = Query(..., description="Comma-separated tickers, at most 100"),
    days: int = Query(60, description="Sessions of closes per ticker, 5 to 250"),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
) -> Dict[str, List[float]]:
    """{ticker: [close, ...]} oldest first, the last `days` daily closes of each requested ticker (empty list when it has none), for trend lines in a table."""
    names = cb.parse_tickers(tickers, clean_ticker)
    if not names:
        raise HTTPException(status_code=422, detail="No valid tickers.")
    n = cb.clamp_days(days)
    if db is None:
        raise HTTPException(status_code=503, detail="Closes are temporarily unavailable.")

    async def compute() -> Dict[str, List[float]]:
        # Calendar days cover the sessions asked for across weekends and holidays.
        rows = await db.query(_CLOSES_SQL, names, int(n * 1.6) + 7) or []
        return cb.build(names, rows, n)

    return await cached_json(getattr(redis, "raw", None), cache_key("stock_closes", tickers=",".join(sorted(names)), days=n), compute, ttl=300, max_stale=3600)

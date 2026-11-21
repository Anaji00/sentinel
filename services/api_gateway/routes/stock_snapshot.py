"""
services/api_gateway/routes/stock_snapshot.py

The fundamentals strip for a stock page: market cap, 52-week range, next earnings, trailing P/E (shared/utils/stock_snapshot.py).
Every figure carries its date or basis; one that cannot be known is null.
"""
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils import stock_snapshot as ss
from shared.utils.market_cap import CAPS_KEY, CAPS_TTL_SEC, cached_market_cap
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cache_key, cached_json
from shared.utils.stock_peers import clean_ticker

logger = logging.getLogger("api.stock_snapshot")
router = APIRouter(prefix="/api/v1/stocks", tags=["Stock snapshot"])

_RANGE_SQL = "SELECT day, low, high FROM daily_bars WHERE ticker = $1 AND day > CURRENT_DATE - 366 ORDER BY day"
_NEXT_REPORT_SQL = "SELECT min(report_date) AS d FROM earnings_estimates WHERE ticker = $1 AND report_date >= CURRENT_DATE"
_LAST_RESULTS_SQL = """
    SELECT max(occurred_at) AS at FROM signal_event_archive
    WHERE type = 'filing' AND summary LIKE '%2.02:%' AND primary_entity_id = $1 AND occurred_at > NOW() - INTERVAL '200 days'
"""
_FACTS_SQL = """
    SELECT DISTINCT ON (concept, period_start, period_end) concept, period_start, period_end, value FROM fundamental_facts
    WHERE cik = (SELECT cik FROM issuer_ticker WHERE ticker = $1 ORDER BY last_filed DESC NULLS LAST LIMIT 1)
      AND concept = ANY($2::text[]) AND unit = 'USD' AND period_start IS NOT NULL AND period_end > CURRENT_DATE - 1400
    ORDER BY concept, period_start, period_end, filed DESC
"""
_EQUITY_SQL = """
    SELECT value FROM fundamental_facts
    WHERE cik = (SELECT cik FROM issuer_ticker WHERE ticker = $1 ORDER BY last_filed DESC NULLS LAST LIMIT 1)
      AND concept = 'StockholdersEquity' AND unit = 'USD' AND period_end > CURRENT_DATE - 400
    ORDER BY period_end DESC, filed DESC LIMIT 1
"""


async def _cap(raw: Any, ticker: str) -> tuple[Optional[float], Optional[str]]:
    """Best-known cap (the rebuilt table, else the vendor cache) and when the table was last rebuilt, estimated from its remaining TTL."""
    value: Optional[float] = None
    as_of: Optional[str] = None
    try:
        got = await raw.hget(CAPS_KEY, ticker)
        if got is not None:
            value = float(got.decode() if isinstance(got, bytes) else got)
            left = await raw.ttl(CAPS_KEY)
            if isinstance(left, int) and left > 0:
                as_of = (datetime.now(timezone.utc) - timedelta(seconds=max(0, CAPS_TTL_SEC - left))).date().isoformat()
    except Exception as exc:
        swallowed("api_gateway.stock_snapshot.cap_table", exc, logger)
    if value is None or value <= 0:
        value = await cached_market_cap(raw, ticker)
        as_of = None
    return (value if value and value > 0 else None), as_of


@router.get("/{ticker}/snapshot")
async def snapshot(
    ticker: str,
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """{ticker, market_cap, market_cap_as_of, range_52w:{low, high, from_date}, next_earnings:{date, estimated}, pe_trailing:{value, basis,
    as_of}, metrics:{revenue, revenue_growth_pct, gross_margin_pct, operating_margin_pct, net_margin_pct, cash_from_operations_margin_pct,
    return_on_equity_pct, price_to_sales, price_to_book, basis, as_of}}. P/E is market cap over trailing 12-month SEC net income, null for
    losses or missing filings; each metric is null when its inputs are missing. No consensus, forward P/E or debt (not on free data)."""
    t = clean_ticker(ticker)
    if not t:
        raise HTTPException(status_code=422, detail="Not a ticker.")
    if db is None:
        raise HTTPException(status_code=503, detail="Stock data is temporarily unavailable.")
    raw = getattr(redis, "raw", redis)

    async def compute() -> Dict[str, Any]:
        today = date.today()
        cap, cap_as_of = await _cap(raw, t)
        rng = earnings = pe = company = None
        degraded: List[str] = []
        try:
            rng = ss.range_52w([(r["day"], r["low"], r["high"]) for r in await db.query(_RANGE_SQL, t) or []])
        except Exception as exc:
            swallowed("api_gateway.stock_snapshot.range", exc, logger)
            degraded.append("range_52w")
        try:
            nxt = await db.query_one(_NEXT_REPORT_SQL, t)
            last = await db.query_one(_LAST_RESULTS_SQL, t)
            earnings = ss.next_earnings(ss.as_day(nxt["d"]) if nxt else None, ss.as_day(last["at"]) if last else None, today)
        except Exception as exc:
            swallowed("api_gateway.stock_snapshot.earnings", exc, logger)
            degraded.append("next_earnings")
        try:
            by_concept: Dict[str, list] = {}
            for r in await db.query(_FACTS_SQL, t, list(ss.FLOW_CONCEPTS)) or []:
                by_concept.setdefault(r["concept"], []).append((ss.as_day(r["period_start"]), ss.as_day(r["period_end"]), float(r["value"])))
            pe = ss.pe_trailing(cap, ss.ttm_net_income(by_concept.get("NetIncomeLoss") or []))
            eq = await db.query_one(_EQUITY_SQL, t)
            company = ss.metrics(cap, by_concept, float(eq["value"]) if eq and eq.get("value") is not None else None)
        except Exception as exc:
            swallowed("api_gateway.stock_snapshot.facts", exc, logger)
            degraded.append("pe_trailing")
            degraded.append("metrics")
        body = ss.snapshot(t, cap, cap_as_of, rng, earnings, pe, company)
        # Parts that failed to read, so a null in them is not mistaken for 'no data' (empty when everything was read).
        body["degraded"] = degraded
        return body

    return await cached_json(raw, cache_key("stock_snapshot_v2", ticker=t), compute, ttl=300, max_stale=3600)

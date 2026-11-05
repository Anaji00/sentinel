"""
What changed on a stock: GET /api/v1/stocks/{ticker}/changes.

Stored rows only, no model calls, opened from the stock drawer and cached five
minutes. shared/utils/stock_changes.py says what each part can and cannot claim.
"""

import logging
from datetime import date, datetime
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.api_gateway.dependencies import get_db, get_redis_optional
from shared.utils import stock_changes as sc
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cache_key, cached_json
from shared.utils.stock_peers import clean_ticker

logger = logging.getLogger("api.stock_changes")

router = APIRouter(prefix="/api/v1/stocks", tags=["Stock changes"])

SectionStatus = Literal["ok", "no_data", "unavailable"]


class PriceChange(BaseModel):
    as_of: date
    close: Optional[float] = None
    change_1d_pct: Optional[float] = None
    change_5d_pct: Optional[float] = None


class RankChange(BaseModel):
    as_of: date
    percentile: Optional[float] = None
    percentile_before: Optional[float] = None
    note: str


class OptionsChange(BaseModel):
    as_of: date
    atm_iv: Optional[float] = None
    atm_iv_before: Optional[float] = None
    put_call_ratio: Optional[float] = None


class ShortChange(BaseModel):
    as_of: date
    short_shares: Optional[float] = None
    change_pct: Optional[float] = None
    days_to_cover: Optional[float] = None


class FlowSide(BaseModel):
    kind: str
    buyer_usd: float
    seller_usd: float
    unknown_usd: float
    buyer_trades: int
    seller_trades: int
    unknown_trades: int
    note: str


class MacroLink(BaseModel):
    symbol: str
    name: str
    direction: Literal["with", "against"]
    t_stat: Optional[float] = None
    sessions: Optional[int] = None
    as_of: Optional[date] = None


class StockEvent(BaseModel):
    kind: str
    headline: Optional[str] = None
    at: datetime
    url: Optional[str] = None


class StockChanges(BaseModel):
    ticker: str
    window_days: int
    sections: Dict[str, SectionStatus]
    price: Optional[PriceChange] = None
    rank: Optional[RankChange] = None
    options: Optional[OptionsChange] = None
    short_interest: Optional[ShortChange] = None
    flow: List[FlowSide] = []
    macro_links: List[MacroLink] = []
    events: List[StockEvent] = []


_PRICE_SQL = "SELECT day, close FROM daily_bars WHERE ticker = $1 ORDER BY day DESC LIMIT 6"
_RANK_SQL = """SELECT as_of, percentile FROM factor_scores_daily WHERE ticker = $1 AND horizon = 'monthly'
               ORDER BY as_of DESC LIMIT 6"""
_OPTIONS_SQL = "SELECT day, atm_iv, put_call_ratio FROM option_iv_history WHERE ticker = $1 ORDER BY day DESC LIMIT 6"
_SHORT_SQL = """SELECT settlement_date, short_qty, change_pct, days_to_cover FROM short_interest
                WHERE ticker = $1 ORDER BY settlement_date DESC LIMIT 1"""
_FLOW_SQL = """
    SELECT type,
           CASE WHEN tags && ARRAY['buyer_initiated', 'aggressor_buy'] THEN 'buyer'
                WHEN tags && ARRAY['seller_initiated', 'aggressor_sell'] THEN 'seller'
                ELSE 'unknown' END AS side,
           count(*) AS trades,
           sum(CASE WHEN type = 'options_flow' THEN (financial_data->>'premium_usd')::float
                    ELSE (financial_data->>'notional_usd')::float END) AS usd
    FROM events
    WHERE primary_entity_id = $1 AND type IN ('equity_block', 'options_flow')
      AND occurred_at > NOW() - make_interval(days => $2)
    GROUP BY 1, 2"""
_MACRO_SQL = """SELECT macro, beta, t, n, as_of FROM macro_exposures WHERE ticker = $1
                ORDER BY abs(t) DESC LIMIT 5"""
_EVENTS_SQL = """
    SELECT type, headline, occurred_at, url FROM events
    WHERE primary_entity_id = $1 AND type IN ('filing', 'insider_trade', 'earnings_report', 'headline')
      AND occurred_at > NOW() - make_interval(days => $2)
    ORDER BY occurred_at DESC LIMIT $3"""


async def _section(name: str, ticker: str, read: Callable[[], Awaitable[Any]]) -> Tuple[Any, SectionStatus]:
    """One part's rows and status: a failed read is contained, counted and said."""
    try:
        rows = await read()
    except Exception as exc:
        swallowed(f"api_gateway.stock_changes.{name}", exc, logger, detail=ticker)
        return [], "unavailable"
    rows = [dict(r) for r in rows or []]
    return rows, "ok" if rows else "no_data"


async def _compute(db: Any, t: str) -> Dict[str, Any]:
    window = sc.WINDOW_DAYS
    price, s_price = await _section("price", t, lambda: db.query(_PRICE_SQL, t))
    rank, s_rank = await _section("rank", t, lambda: db.query(_RANK_SQL, t))
    options, s_options = await _section("options", t, lambda: db.query(_OPTIONS_SQL, t))
    short, s_short = await _section("short_interest", t, lambda: db.query(_SHORT_SQL, t))
    flow, s_flow = await _section("flow", t, lambda: db.query(_FLOW_SQL, t, window))
    macro, s_macro = await _section("macro_links", t, lambda: db.query(_MACRO_SQL, t))
    events, s_events = await _section("events", t, lambda: db.query(_EVENTS_SQL, t, window, sc.MAX_EVENTS))
    sections: Dict[str, SectionStatus] = {
        "price": s_price, "rank": s_rank, "options": s_options, "short_interest": s_short,
        "flow": s_flow, "macro_links": s_macro, "events": s_events,
    }
    return StockChanges(
        ticker=t, window_days=window, sections=sections,
        price=sc.price_change(price), rank=sc.rank_change(rank), options=sc.options_change(options),
        short_interest=sc.short_change(short[0] if short else None),
        flow=sc.flow_summary(flow), macro_links=sc.macro_links(macro),
        events=[{"kind": e["type"], "headline": e["headline"], "at": e["occurred_at"], "url": e["url"]} for e in events],
    ).model_dump(mode="json")


@router.get("/{ticker}/changes", response_model=StockChanges)
async def changes(
    ticker: str,
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
    db=Depends(get_db),
    redis=Depends(get_redis_optional),
):
    """What moved on this stock in the last week, each part dated and with its status."""
    t = clean_ticker(ticker)
    if not t:
        raise HTTPException(status_code=422, detail="Not a ticker.")
    raw = getattr(redis, "raw", None)
    return await cached_json(
        raw, cache_key("stock_changes", ticker=t), lambda: _compute(db, t), ttl=300, max_stale=3600,
    )

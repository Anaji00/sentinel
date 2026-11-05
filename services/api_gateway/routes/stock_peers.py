"""
Stocks related to a stock: GET /api/v1/stocks/{ticker}/related.

Reads sympathy_peers and text_peers, which jobs rebuild each night, so the answer
changes once a day and is cached an hour. See shared/utils/stock_peers.py for what
it can and cannot say.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.api_gateway.dependencies import get_db, get_redis_optional
from services.api_gateway.routes.ownership import _ticker_names
from shared.utils.money import company_name
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cache_key, cached_json
from shared.utils.stock_peers import MAX_MOVES_WITH, MAX_SIMILAR, build_related, clean_ticker

logger = logging.getLogger("api.stock_peers")

router = APIRouter(prefix="/api/v1/stocks", tags=["Stock relationships"])


class MovesWith(BaseModel):
    ticker: str
    name: str
    correlation: float
    sessions: int
    closeness: str


class SimilarBusiness(BaseModel):
    ticker: str
    name: str
    similarity: float


class RelatedStocks(BaseModel):
    ticker: str
    as_of: Optional[date] = None
    stale: bool
    window_sessions: int
    moves_with: List[MovesWith]
    similar_business: List[SimilarBusiness]
    empty_reason: Optional[Literal["not_computed", "none_found"]] = None
    note: str


async def _compute(db: Any, raw: Any, ticker: str) -> Dict[str, Any]:
    try:
        sympathy = await db.query(
            "SELECT peer, corr, n, as_of FROM sympathy_peers WHERE ticker = $1 ORDER BY corr DESC LIMIT $2",
            ticker, MAX_MOVES_WITH,
        )
        text = await db.query(
            "SELECT peer, sim, as_of FROM text_peers WHERE ticker = $1 ORDER BY sim DESC LIMIT $2",
            ticker, MAX_SIMILAR,
        )
        latest = await db.query(
            "SELECT GREATEST((SELECT max(as_of) FROM sympathy_peers), (SELECT max(as_of) FROM text_peers)) AS d"
        )
    except Exception as exc:
        swallowed("api_gateway.stock_peers.read", exc, logger, detail=ticker)
        raise HTTPException(status_code=503, detail="Related stocks are unavailable right now.")
    peers = {r["peer"] for r in list(sympathy) + list(text)}
    if raw is not None and peers:
        names = await _ticker_names(raw, peers)
    else:
        names = {t: company_name(t, []) for t in peers}
    related = build_related(
        ticker, [dict(r) for r in sympathy], [dict(r) for r in text], names,
        latest[0]["d"] if latest else None, date.today(),
    )
    return RelatedStocks(**related).model_dump(mode="json")


@router.get("/{ticker}/related", response_model=RelatedStocks)
async def related(
    ticker: str,
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
    db=Depends(get_db),
    redis=Depends(get_redis_optional),
):
    """Stocks that have moved with this one, and stocks with a similar business."""
    t = clean_ticker(ticker)
    if not t:
        raise HTTPException(status_code=422, detail="Not a ticker.")
    raw = getattr(redis, "raw", None)
    return await cached_json(
        raw, cache_key("stock_related", ticker=t), lambda: _compute(db, raw, t), ttl=3600, max_stale=2 * 86400,
    )

"""
Commodity prices, read from commodity_prices (filled once a day by the tradfi collector):

  GET /api/v1/commodities/chips    memory-chip spot (DRAMeXchange) and GPU rental rates (RunPod)
  GET /api/v1/commodities/futures  oil, metals, grains, softs, livestock, lumber and gas futures (Yahoo)

The prices change once a day, so each answer is cached ten minutes. See shared/utils/chip_prices.py and
shared/utils/commodity_board.py.
"""

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from services.api_gateway.dependencies import get_db, get_redis_optional
from shared.utils.chip_prices import build_chip_prices
from shared.utils.commodity_board import build_commodity_board
from shared.utils.commodity_links import FUTURES
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cache_key, cached_json

logger = logging.getLogger("api.chip_prices")

router = APIRouter(prefix="/api/v1/commodities", tags=["Commodities"])


class HistoryPoint(BaseModel):
    day: date
    value: float


class PriceItem(BaseModel):
    name: str
    variant: Optional[str] = None
    latest: float
    as_of: date
    since: date
    change_pct: Optional[float] = None
    unit: str
    source: Optional[str] = None
    history: List[HistoryPoint]


class PriceSection(BaseModel):
    key: str
    title: str
    note: str
    items: List[PriceItem]
    gain_when_up: List[str]
    pay_when_up: List[str]


class ChipPrices(BaseModel):
    as_of: Optional[date] = None
    since: Optional[date] = None
    record_days: int
    stale: bool
    sections: List[PriceSection]
    note: str


async def _compute(db: Any, days: int) -> Dict[str, Any]:
    try:
        rows = await db.query(
            "SELECT series, day, value, unit, source FROM commodity_prices "
            "WHERE (series LIKE 'dram:%' OR series LIKE 'nand:%' OR series LIKE 'gddr:%' OR series LIKE 'gpu:%') "
            "AND day >= $1 ORDER BY series, day",
            date.today() - timedelta(days=days),
        )
    except Exception as exc:
        swallowed("api_gateway.chip_prices.read", exc, logger)
        raise HTTPException(status_code=503, detail="Chip prices are unavailable right now.")
    return ChipPrices(**build_chip_prices([dict(r) for r in rows], date.today())).model_dump(mode="json")


@router.get("/chips", response_model=ChipPrices)
async def chip_prices(
    days: int = Query(60, ge=7, le=365),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
    db=Depends(get_db),
    redis=Depends(get_redis_optional),
):
    """DRAM, NAND flash and GDDR spot prices and GPU rental rates, with what each bears on."""
    raw = getattr(redis, "raw", None)
    return await cached_json(
        raw, cache_key("chip_prices", days=days), lambda: _compute(db, days), ttl=600, max_stale=86400,
    )


class FuturesItem(BaseModel):
    key: str
    name: str
    group: str
    latest: float
    as_of: date
    history_days: int
    stale: bool
    change_day: Optional[float] = None
    change_week: Optional[float] = None
    change_month: Optional[float] = None
    change_year: Optional[float] = None
    history: List[HistoryPoint]
    gain_when_up: List[str]
    pay_when_up: List[str]


class FuturesCategory(BaseModel):
    key: str
    title: str
    items: List[FuturesItem]


class Futures(BaseModel):
    as_of: Optional[date] = None
    stale: bool
    categories: List[FuturesCategory]
    note: str


async def _compute_futures(db: Any) -> Dict[str, Any]:
    try:
        rows = await db.query(
            "SELECT series, day, value FROM commodity_prices WHERE series = ANY($1::text[]) AND day >= $2 ORDER BY series, day",
            list(FUTURES), date.today() - timedelta(days=400),
        )
    except Exception as exc:
        swallowed("api_gateway.commodity_board.read", exc, logger)
        raise HTTPException(status_code=503, detail="Commodity prices are unavailable right now.")
    return Futures(**build_commodity_board([dict(r) for r in rows], date.today())).model_dump(mode="json")


@router.get("/futures", response_model=Futures)
async def futures_prices(
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
    db=Depends(get_db),
    redis=Depends(get_redis_optional),
):
    """Commodity futures with changes over a day, a week, a month and a year, and the stocks each helps or hurts."""
    raw = getattr(redis, "raw", None)
    return await cached_json(raw, cache_key("commodity_board"), lambda: _compute_futures(db), ttl=600, max_stale=86400)

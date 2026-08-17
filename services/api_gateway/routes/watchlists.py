"""
services/api_gateway/routes/watchlists.py

Dynamic Watchlist Governance & Collector Sync API.
Enforces the Finnhub 50-ticker clamp, triggers instant collector repointing
via Redis Pub/Sub, and records hash-chained audit trails for all mutations.
"""

import json
import logging
import time
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from services.api_gateway.dependencies import (
    get_redis_client,
    get_redis_optional,
    get_db_optional,
)
from shared.utils.rbac import require_role, Role, get_current_user_role
from shared.utils.audit_ledger import AuditLedger
from shared.utils.equities import is_valid_primary_equity

logger = logging.getLogger("api-gateway.watchlists")

router = APIRouter(prefix="/api/v1/watchlists", tags=["Watchlist Governance"])

REDIS_EQUITIES_KEY = "sentinel:watched:equities"
WATCHLIST_SYNC_CHANNEL = "sentinel:collector:watchlist_sync"
MAX_WATCHLIST_LIMIT = 50


class TickerAddRequest(BaseModel):
    tickers: List[str] = Field(..., min_length=1, max_length=50, description="List of primary equity symbols to add")
    priority_score: Optional[float] = Field(default=None, description="Optional custom priority score")

    @field_validator("tickers")
    @classmethod
    def validate_tickers(cls, v):
        clean = [t.strip().upper() for t in v if t and isinstance(t, str) and t.strip()]
        if not clean:
            raise ValueError("Must provide at least one valid ticker symbol.")
        return clean


class WatchlistSyncRequest(BaseModel):
    tickers: List[str] = Field(..., max_length=50, description="Exact replacement list of up to 50 tickers")

    @field_validator("tickers")
    @classmethod
    def validate_tickers(cls, v):
        clean = [t.strip().upper() for t in v if t and isinstance(t, str) and t.strip()]
        if len(clean) > MAX_WATCHLIST_LIMIT:
            raise ValueError(f"Watchlist exceeds maximum allowed limit of {MAX_WATCHLIST_LIMIT} symbols.")
        return clean


class TeamWatchlistCreateRequest(BaseModel):
    team_name: str
    description: Optional[str] = None
    tickers: List[str] = Field(default_factory=list, max_length=50)


async def _publish_sync_signal(redis_client, mutated_by: str, count: int):
    """Broadcasts a sync trigger on Redis PubSub to immediately repoint collectors."""
    if not redis_client:
        return
    try:
        raw_redis = getattr(redis_client, "raw", redis_client)
        msg = json.dumps({
            "action": "WATCHLIST_MUTATED",
            "timestamp": time.time(),
            "mutated_by": mutated_by,
            "active_count": count,
        })
        await raw_redis.publish(WATCHLIST_SYNC_CHANNEL, msg)
        logger.info(f"📡 Broadcasted watchlist sync trigger to '{WATCHLIST_SYNC_CHANNEL}' (active_count={count})")
    except Exception as e:
        logger.error(f"Failed to publish watchlist sync message: {e}")


@router.get("/equities")
async def get_equities_watchlist(redis = Depends(get_redis_client)):
    """Retrieve the active equities watchlist with dynamic priority scores."""
    raw_redis = getattr(redis, "raw", redis)
    raw_items = await raw_redis.zrevrange(REDIS_EQUITIES_KEY, 0, -1, withscores=True)

    items = []
    for item in raw_items:
        ticker = item[0].decode("utf-8") if isinstance(item[0], bytes) else str(item[0])
        score = float(item[1])
        items.append({
            "ticker": ticker,
            "priority_score": score,
            "is_valid_equity": is_valid_primary_equity(ticker),
        })

    return {
        "count": len(items),
        "max_limit": MAX_WATCHLIST_LIMIT,
        "is_clamped": len(items) >= MAX_WATCHLIST_LIMIT,
        "watchlist": items,
    }


@router.post("/equities", dependencies=[Depends(require_role(Role.ANALYST))])
async def add_equities_to_watchlist(
    req: TickerAddRequest,
    request: Request,
    redis = Depends(get_redis_client),
    db = Depends(get_db_optional),
):
    """
    Adds tickers to the dynamic watchlist.
    Enforces the 50-ticker clamp and triggers instantaneous Finnhub WebSocket repointing.
    """
    raw_redis = getattr(redis, "raw", redis)
    current_count = await raw_redis.zcard(REDIS_EQUITIES_KEY)

    # Check capacity
    to_add = [t for t in req.tickers if is_valid_primary_equity(t)]
    if not to_add:
        raise HTTPException(status_code=400, detail="None of the provided symbols are valid US equities.")

    if current_count + len(to_add) > MAX_WATCHLIST_LIMIT:
        available_slots = max(0, MAX_WATCHLIST_LIMIT - current_count)
        if available_slots == 0:
            raise HTTPException(
                status_code=400,
                detail=f"Watchlist is at capacity ({current_count}/{MAX_WATCHLIST_LIMIT}). Remove tickers before adding."
            )
        to_add = to_add[:available_slots]

    now_ts = time.time()
    pipe = raw_redis.pipeline()
    for t in to_add:
        score = req.priority_score if req.priority_score is not None else now_ts
        pipe.zadd(REDIS_EQUITIES_KEY, {t: score})
    await pipe.execute()

    new_count = await raw_redis.zcard(REDIS_EQUITIES_KEY)
    actor = getattr(request.state, "identity", "analyst")

    # Broadcast sync signal to collectors
    await _publish_sync_signal(redis, actor, new_count)

    # Record in immutable audit ledger
    ledger = AuditLedger(redis_client=redis, db_client=db)
    await ledger.record_entry(
        actor=actor,
        action="ADD_WATCHLIST_TICKERS",
        resource_type="WATCHLIST",
        resource_id=REDIS_EQUITIES_KEY,
        details={"added_tickers": to_add, "new_total": new_count},
    )

    return {
        "status": "ADDED",
        "added_tickers": to_add,
        "total_active": new_count,
        "max_limit": MAX_WATCHLIST_LIMIT,
    }


@router.delete("/equities/{ticker}", dependencies=[Depends(require_role(Role.ANALYST))])
async def remove_equity_from_watchlist(
    ticker: str,
    request: Request,
    redis = Depends(get_redis_client),
    db = Depends(get_db_optional),
):
    """
    Removes a ticker from the dynamic watchlist.
    Broadcasts sync signal to collectors and records audit entry.
    """
    sym = ticker.strip().upper()
    raw_redis = getattr(redis, "raw", redis)
    removed = await raw_redis.zrem(REDIS_EQUITIES_KEY, sym)

    if removed == 0:
        raise HTTPException(status_code=404, detail=f"Ticker '{sym}' was not in the active watchlist.")

    new_count = await raw_redis.zcard(REDIS_EQUITIES_KEY)
    actor = getattr(request.state, "identity", "analyst")

    # Broadcast sync signal to collectors
    await _publish_sync_signal(redis, actor, new_count)

    # Record in immutable audit ledger
    ledger = AuditLedger(redis_client=redis, db_client=db)
    await ledger.record_entry(
        actor=actor,
        action="REMOVE_WATCHLIST_TICKER",
        resource_type="WATCHLIST",
        resource_id=REDIS_EQUITIES_KEY,
        details={"removed_ticker": sym, "new_total": new_count},
    )

    return {
        "status": "REMOVED",
        "removed_ticker": sym,
        "total_active": new_count,
    }


@router.put("/equities/sync", dependencies=[Depends(require_role(Role.ADMIN))])
async def full_sync_equities_watchlist(
    req: WatchlistSyncRequest,
    request: Request,
    redis = Depends(get_redis_client),
    db = Depends(get_db_optional),
):
    """
    Full atomic replacement of the active equities watchlist (max 50 tickers).
    """
    valid_tickers = [t for t in req.tickers if is_valid_primary_equity(t)]
    if len(valid_tickers) > MAX_WATCHLIST_LIMIT:
        valid_tickers = valid_tickers[:MAX_WATCHLIST_LIMIT]

    raw_redis = getattr(redis, "raw", redis)
    pipe = raw_redis.pipeline()
    pipe.delete(REDIS_EQUITIES_KEY)
    now_ts = time.time()
    for idx, t in enumerate(valid_tickers):
        pipe.zadd(REDIS_EQUITIES_KEY, {t: now_ts - idx})
    await pipe.execute()

    actor = getattr(request.state, "identity", "admin")
    new_count = len(valid_tickers)

    # Broadcast sync signal
    await _publish_sync_signal(redis, actor, new_count)

    # Record in immutable audit ledger
    ledger = AuditLedger(redis_client=redis, db_client=db)
    await ledger.record_entry(
        actor=actor,
        action="FULL_SYNC_WATCHLIST",
        resource_type="WATCHLIST",
        resource_id=REDIS_EQUITIES_KEY,
        details={"tickers_count": new_count, "tickers": valid_tickers},
    )

    return {
        "status": "SYNCED",
        "tickers": valid_tickers,
        "total_active": new_count,
    }

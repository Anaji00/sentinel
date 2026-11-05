"""
The bull and bear case for a stock, written by the local model on request.

POST /api/v1/stocks/{ticker}/thesis starts it (or returns what is already there);
GET reads it. A local model takes a minute or more, so this is a background job the
page polls, never a request held open.

The model call, as the project rules require it to be stated:
  trigger     a person pressed the button for one stock
  dedupe      the answer is kept six hours per stock; one job per stock at a time
  cap         RATE_PER_HOUR new generations per person, one inference slot for all (lane "thesis")
  admission   InferenceBudget: when the model is busy this says so and stops. It never queues
              and never retries an overloaded model.
  consumer    the stock drawer, which shows the points with the facts each rests on
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

import aiohttp
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.api_gateway.dependencies import get_db, get_redis_optional
from services.api_gateway.routes.predictions import _load_json
from shared.utils import thesis as th
from shared.utils.inference_budget import InferenceBudget
from shared.utils.ollama import InferenceError, OllamaClient, SchemaViolationError
from shared.utils.quiet_failures import dropped, swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.signal_plain import stand_out
from shared.utils.signal_store import FACTOR_REPORT_KEY, FACTOR_SCORES_KEY
from shared.utils.stock_peers import clean_ticker

logger = logging.getLogger("api.stock_thesis")

router = APIRouter(prefix="/api/v1/stocks", tags=["Stock thesis"])

THESIS_KEY = "sentinel:thesis:{ticker}"
RATE_KEY = "sentinel:thesis:rate:{user}"
COOLDOWN_SEC = int(os.getenv("THESIS_COOLDOWN_SEC", "60"))
CHAIN_DEADLINE_SEC = float(os.getenv("THESIS_DEADLINE_SEC", "300"))
NUM_PREDICT = int(os.getenv("THESIS_NUM_PREDICT", "700"))
_TASKS: set = set()

_CLOSES_SQL = "SELECT close FROM daily_bars WHERE ticker = $1 AND close > 0 ORDER BY day DESC LIMIT 64"
_NEWS_SQL = """
    SELECT created_at, headline, (positive - negative) AS tone FROM stock_news
    WHERE $1 = ANY(symbols) AND scored_at IS NOT NULL AND cardinality(symbols) <= 3
    ORDER BY created_at DESC LIMIT 8
"""
_EVENTS_SQL = """
    SELECT created_at, COALESCE(NULLIF(headline, ''), summary) AS text FROM events
    WHERE financial_data->>'ticker' = $1 AND created_at > NOW() - INTERVAL '3 days'
      AND type IN ('equity_block', 'options_flow', 'insider_trade', 'filing', 'market_anomaly', 'earnings_report')
    ORDER BY anomaly_score DESC NULLS LAST LIMIT 6
"""
_MOVE_SESSIONS = (("the last session", 1), ("the last 5 sessions", 5), ("the last month", 21), ("the last 3 months", 63))


class Source(BaseModel):
    id: str
    text: str


class Point(BaseModel):
    point: str
    sources: List[Source]


class ThesisState(BaseModel):
    ticker: str
    status: Literal["none", "pending", "ready", "failed", "busy", "limited"]
    message: str = ""
    generated_at: Optional[str] = None
    model: Optional[str] = None
    bull: List[Point] = []
    bear: List[Point] = []
    would_change_it: str = ""
    facts_used: int = 0
    disclaimer: str = th.DISCLAIMER


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _state(ticker: str, status: str, **fields: Any) -> Dict[str, Any]:
    return ThesisState(ticker=ticker, status=status, **fields).model_dump(mode="json")


async def _save(raw: Any, ticker: str, state: Dict[str, Any], ttl: int) -> None:
    await raw.set(THESIS_KEY.format(ticker=ticker), json.dumps(state), ex=ttl)


async def _read(raw: Any, ticker: str) -> Optional[Dict[str, Any]]:
    blob = await raw.get(THESIS_KEY.format(ticker=ticker))
    return _load_json(blob) if blob else None


def _since(iso: str) -> str:
    return str(iso)[:10]


async def _gather(db: Any, raw: Any, ticker: str) -> List[str]:
    """The numbered facts about this stock, from the platform's own data."""
    closes = [float(r["close"]) for r in await db.query(_CLOSES_SQL, ticker)]
    moves = {
        label: (closes[0] / closes[n] - 1) if len(closes) > n else None for label, n in _MOVE_SESSIONS
    }
    report = _load_json(await raw.get(FACTOR_REPORT_KEY)) or {}
    scores = _load_json(await raw.get(FACTOR_SCORES_KEY)) or {}
    entry = scores.get(ticker) if isinstance(scores, dict) else None
    percentile = entry.get("percentile") if isinstance(entry, dict) else None
    traits = [t["text"] for t in stand_out((entry or {}).get("features") if isinstance(entry, dict) else None)]
    headlines = [
        (_since(r["created_at"]), str(r["headline"])[:200], float(r["tone"]) if r["tone"] is not None else None)
        for r in await db.query(_NEWS_SQL, ticker)
    ]
    events = [(_since(r["created_at"]), str(r["text"])[:200]) for r in await db.query(_EVENTS_SQL, ticker) if r["text"]]
    return th.facts(ticker, percentile, report.get("horizon_days"), traits, moves, headlines, events)


async def _run(db: Any, raw: Any, redis: Any, ticker: str, budget: InferenceBudget, model: str) -> None:
    """The background job. Always leaves a ready or failed state behind."""
    final = _state(ticker, "failed", message="Something went wrong writing this. Try again in a few minutes.")
    ttl = th.FAILED_TTL_SEC
    try:
        lines = await _gather(db, raw, ticker)
        if len(lines) < th.MIN_FACTS:
            final = _state(ticker, "failed", message="There is too little recent information on this stock to argue both sides.")
            return
        system, user = th.prompt(ticker, lines)
        async with aiohttp.ClientSession() as session:
            client = OllamaClient(session, model=model, redis_client=redis)
            view = await client.infer(
                system, user, th.ThesisView, temperature=0.3, max_retries=2, model=model, fallback_model=None,
                num_predict=NUM_PREDICT, deadline=time.monotonic() + CHAIN_DEADLINE_SEC,
            )
        shown = th.validate(view.model_dump(), lines)
        if shown is None:
            dropped("api_gateway.stock_thesis", "ungrounded_answer", logger, detail=ticker)
            final = _state(ticker, "failed", message="The model did not ground both sides in the facts it was given, so nothing is shown.")
            return
        final = _state(ticker, "ready", generated_at=_now(), model=model, facts_used=len(lines), **shown)
        ttl = th.CACHE_TTL_SEC
    except (InferenceError, SchemaViolationError) as exc:
        # An overloaded or timing-out model is not retried: that is the host saying it is full.
        swallowed("api_gateway.stock_thesis.model", exc, logger, detail=ticker)
        final = _state(ticker, "failed", message="The local model could not answer in time. It is probably busy; try again later.")
    except Exception as exc:
        swallowed("api_gateway.stock_thesis.run", exc, logger, detail=ticker)
    finally:
        try:
            await _save(raw, ticker, final, ttl)
            await budget.finish()
        except Exception as exc:
            swallowed("api_gateway.stock_thesis.save", exc, logger, detail=ticker)


@router.get("/{ticker}/thesis", response_model=ThesisState)
async def read_thesis(
    ticker: str,
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
    redis=Depends(get_redis_optional),
):
    """What there is for this stock: nothing yet, running, ready or failed."""
    t = clean_ticker(ticker)
    if not t:
        raise HTTPException(status_code=422, detail="Not a ticker.")
    raw = getattr(redis, "raw", None)
    if raw is None:
        return _state(t, "none")
    return await _read(raw, t) or _state(t, "none")


@router.post("/{ticker}/thesis", response_model=ThesisState)
async def start_thesis(
    ticker: str,
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
    db=Depends(get_db),
    redis=Depends(get_redis_optional),
):
    """Starts writing the bull and bear case, unless one is ready or already running."""
    t = clean_ticker(ticker)
    if not t:
        raise HTTPException(status_code=422, detail="Not a ticker.")
    raw = getattr(redis, "raw", None)
    if raw is None:
        raise HTTPException(status_code=503, detail="Not available right now.")
    have = await _read(raw, t)
    if have and have.get("status") in ("ready", "pending"):
        return have

    who = str(user.get("email") or user.get("sub") or user.get("id") or "anonymous")
    rate_key = RATE_KEY.format(user=who)
    if int(await raw.get(rate_key) or 0) >= th.RATE_PER_HOUR:
        return _state(t, "limited", message=f"You have asked for {th.RATE_PER_HOUR} of these in the last hour. Try again later.")

    # The model the agents really run. REASONING_MODEL names a 7b that needs more memory
    # than this host has free, so asking for it fails once and falls back every time.
    model = os.getenv("THESIS_MODEL") or os.getenv("AGENT_MODEL", "qwen2.5:3b")
    budget = InferenceBudget(redis, model, cooldown_sec=COOLDOWN_SEC, lane="thesis", owner="thesis")
    if not await budget.try_acquire():
        return _state(t, "busy", message="The AI model is busy with other work. Try again in a minute or two.")

    try:
        pending = _state(t, "pending", message="Writing it now. This usually takes one to two minutes.")
        await _save(raw, t, pending, th.PENDING_TTL_SEC)
        used = await raw.incr(rate_key)
        if used == 1:
            await raw.expire(rate_key, 3600)
    except Exception as exc:
        await budget.release()
        swallowed("api_gateway.stock_thesis.start", exc, logger, detail=t)
        raise HTTPException(status_code=503, detail="Not available right now.")
    task = asyncio.create_task(_run(db, raw, redis, t, budget, model))
    _TASKS.add(task)
    task.add_done_callback(_TASKS.discard)
    return pending

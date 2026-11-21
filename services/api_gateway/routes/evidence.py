"""
services/api_gateway/routes/evidence.py

One call for a stock page: quote, snapshot, held-back signals, regime label, producer grades, positioning and the model's rank, each as its
own section that succeeds or fails alone (shared/utils/evidence_bundle.py). Built from the same functions as the individual routes, with no
HTTP fan-out, each under a time limit.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict

from fastapi import APIRouter, Depends, HTTPException

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils import evidence_bundle as eb
from shared.utils import market_quotes as mq
from shared.utils.producer_grades import rule_family
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cache_key, cached_json
from shared.utils.signal_store import FACTOR_REPORT_KEY, FACTOR_SCORES_KEY, PRODUCER_GRADES_KEY
from shared.utils.stock_peers import clean_ticker

logger = logging.getLogger("api.evidence")
router = APIRouter(prefix="/api/v1/stocks", tags=["Evidence"])
SECTION_TIMEOUT_SEC = 12.0

_REGIME_SQL = """
    SELECT detected_at, rule_name, metrics_summary FROM correlations
    WHERE primary_entity_id = $1 AND 'divergence' = ANY(tags) AND detected_at > NOW() - INTERVAL '90 days'
    ORDER BY detected_at DESC LIMIT 20
"""
_RULES_SQL = """
    SELECT rule_name FROM correlations WHERE primary_entity_id = $1 AND detected_at > NOW() - INTERVAL '30 days'
    ORDER BY detected_at DESC LIMIT 60
"""


def _json(blob: Any) -> Any:
    if not blob:
        return None
    try:
        return json.loads(blob.decode() if isinstance(blob, bytes) else blob)
    except ValueError:
        return None


async def _run(name: str, make: Callable[[], Awaitable[Dict[str, Any]]]) -> Dict[str, Any]:
    try:
        return await asyncio.wait_for(make(), SECTION_TIMEOUT_SEC)
    except asyncio.TimeoutError as exc:
        swallowed(f"api_gateway.evidence.{name}", exc, logger, detail="timeout")
    except Exception as exc:
        swallowed(f"api_gateway.evidence.{name}", exc, logger)
    return eb.section("error", note="This source could not be read just now.")


@router.get("/{ticker}/evidence")
async def evidence(
    ticker: str,
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """{ticker, as_of, sections:{quote, snapshot, held_back, regime, producer_grade, positioning, model}}, each {status: ok|missing|error,
    data, note}. quote and snapshot are the shapes of /quotes and /stocks/{ticker}/snapshot; held_back and positioning are those routes'
    bodies; regime is the newest divergence alert's label with its reasons; producer_grade grades the rules behind this stock's recent
    alerts; model is the nightly rank with its as-of date."""
    t = clean_ticker(ticker)
    if not t:
        raise HTTPException(status_code=422, detail="Not a ticker.")
    if db is None or redis is None:
        raise HTTPException(status_code=503, detail="Stock data is temporarily unavailable.")
    raw = getattr(redis, "raw", redis)

    async def quote() -> Dict[str, Any]:
        from services.api_gateway.routes.quotes import _BARS_SQL, _DAILY_SQL

        bar = await db.query(_BARS_SQL, [t])
        daily = await db.query(_DAILY_SQL, [t])
        q = mq.build_quote(t, (bar[0]["time"], bar[0]["close"]) if bar else None, [(r["day"], r["close"]) for r in daily or []],
                           datetime.now(timezone.utc))
        return eb.section("ok", q) if q["price"] is not None else eb.section("missing", note="No price on file.")

    async def snapshot() -> Dict[str, Any]:
        from services.api_gateway.routes.stock_snapshot import snapshot as route

        body = await route(t, db=db, redis=redis, user=None)
        if body.get("degraded"):
            return eb.section("error", note="Part of the fundamentals could not be read just now: " + ", ".join(body["degraded"]) + ".")
        has = any(body.get(k) for k in ("market_cap", "range_52w", "next_earnings", "pe_trailing", "metrics"))
        return eb.section("ok", body) if has else eb.section("missing", note="No fundamentals on file for this stock.")

    async def held_back() -> Dict[str, Any]:
        from services.api_gateway.routes.signals import held_back as route

        body = await route(t, limit=10, redis_client=redis, user=None)
        return eb.section("ok", body) if body.get("count") else eb.section("missing", note=body.get("note"))

    async def regime() -> Dict[str, Any]:
        found = eb.regime_of(await db.query(_REGIME_SQL, t) or [])
        return eb.section("ok", found) if found else eb.section("missing", note="No recent peer-divergence alert on this stock.")

    async def producer_grade() -> Dict[str, Any]:
        names = [str(r["rule_name"]) for r in await db.query(_RULES_SQL, t) or []]
        found = eb.grades_for(names, _json(await raw.get(PRODUCER_GRADES_KEY)), rule_family)
        return eb.section("ok", found) if found else eb.section("missing", note="No graded rule fired on this stock in the last 30 days.")

    async def positioning() -> Dict[str, Any]:
        from services.api_gateway.routes.predictions import positioning_for

        body = await positioning_for(t, redis_client=redis, db=db, user=None)
        has = body.get("live") or body.get("daily") or body.get("short_interest")
        return eb.section("ok", body) if has else eb.section("missing", note="No options or short-interest data for this stock.")

    async def model() -> Dict[str, Any]:
        scores = _json(await raw.get(FACTOR_SCORES_KEY))
        report = _json(await raw.get(FACTOR_REPORT_KEY)) or {}
        row = eb.model_row(scores, t, report.get("as_of"))
        return eb.section("ok", row) if row else eb.section("missing", note="The model does not rank this stock.")

    async def compute() -> Dict[str, Any]:
        names = ("quote", "snapshot", "held_back", "regime", "producer_grade", "positioning", "model")
        makers = (quote, snapshot, held_back, regime, producer_grade, positioning, model)
        results = await asyncio.gather(*[_run(n, m) for n, m in zip(names, makers)])
        return {"ticker": t, "as_of": datetime.now(timezone.utc).isoformat(), "sections": dict(zip(names, results))}

    return await cached_json(raw, cache_key("stock_evidence", ticker=t), compute, ttl=15, max_stale=120)

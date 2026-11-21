"""
services/api_gateway/routes/strip.py

A watchlist strip: up to 50 tickers in one call (shared/utils/watch_strip.py). The queries are batched across the tickers (not one set per stock) and the
whole answer is cached for a few seconds per ticker list. Every field matches the single-stock routes it is built from.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from services.api_gateway.routes.quotes import _BARS_SQL, _DAILY_SQL
from services.api_gateway.routes.screener import _EVENTS_SQL, _LAST_RESULTS_SQL
from shared.utils import market_quotes as mq
from shared.utils import stock_snapshot as ss
from shared.utils import watch_strip as ws
from shared.utils.evidence_bundle import model_row, regime_of
from shared.utils.held_back import for_tickers as held_back_for_all
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cached_json
from shared.utils.signal_families import FAMILY_DESCRIPTIONS
from shared.utils.signal_store import FACTOR_REPORT_KEY, FACTOR_SCORES_KEY, SHADOW_KEY

SHADOW_READ = 2000

logger = logging.getLogger("api.strip")
router = APIRouter(prefix="/api/v1/stocks", tags=["Watchlist strip"])

_REGIME_SQL = """
    SELECT detected_at, rule_name, primary_entity_id, metrics_summary FROM correlations
    WHERE 'divergence' = ANY(tags) AND primary_entity_id = ANY($1::text[]) AND detected_at > NOW() - INTERVAL '90 days'
    ORDER BY detected_at DESC LIMIT 600
"""


def _json(blob: Any) -> Any:
    if not blob:
        return None
    try:
        return json.loads(blob.decode() if isinstance(blob, bytes) else blob)
    except ValueError:
        return None


@router.get("/strip")
async def strip(
    tickers: str = Query(..., description="Comma-separated tickers, at most 50."),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """{as_of, rows:[{ticker, price, change_pct, session, delayed, as_of, next_earnings:{date, estimated}, model_percentile, model_as_of,
    held_back_count, regime:{label, action, alert_at}}]} in the order asked. Each field is the single-stock route's own value (quote, snapshot's
    earnings rule, evidence's regime and model rank); held_back_count counts the newest 20 held-back signals. Missing is null, never 0."""
    names: List[str] = mq.clean_list(tickers, ws.MAX_TICKERS)
    if not names:
        raise HTTPException(status_code=422, detail="Name at least one ticker.")
    if db is None or redis is None:
        raise HTTPException(status_code=503, detail="The strip is temporarily unavailable.")
    raw = getattr(redis, "raw", redis)

    async def compute() -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        today = now.astimezone(ZoneInfo("America/New_York")).date()
        try:
            bars = {str(r["ticker"]).upper(): (r["time"], r["close"]) for r in await db.query(_BARS_SQL, names) or []}
            daily: Dict[str, list] = {}
            for r in await db.query(_DAILY_SQL, names) or []:
                daily.setdefault(str(r["ticker"]).upper(), []).append((r["day"], r["close"]))
        except Exception as exc:
            swallowed("api_gateway.strip.quotes", exc, logger)
            raise HTTPException(status_code=503, detail="The strip is temporarily unavailable.")
        listed: Dict[str, Any] = {}
        last: Dict[str, Any] = {}
        regimes: Dict[str, Any] = {}
        try:
            listed = {str(r["ticker"]).upper(): ss.as_day(r["d"]) for r in await db.query(_EVENTS_SQL, names) or []}
            last = {str(r["ticker"]).upper(): ss.as_day(r["at"]) for r in await db.query(_LAST_RESULTS_SQL, names) or []}
        except Exception as exc:
            swallowed("api_gateway.strip.earnings", exc, logger)
        try:
            for r in await db.query(_REGIME_SQL, names) or []:
                t = str(r["primary_entity_id"]).upper()
                if t not in regimes:
                    found = regime_of([r])
                    if found:
                        regimes[t] = found
        except Exception as exc:
            swallowed("api_gateway.strip.regime", exc, logger)
        scores = _json(await raw.get(FACTOR_SCORES_KEY)) or {}
        as_of = (_json(await raw.get(FACTOR_REPORT_KEY)) or {}).get("as_of")
        shadow: List[Any] = []
        try:
            # The newest SHADOW_READ entries only (the writer keeps up to 5,000 JSON blobs): the strip shows at most 20 per name, and each entry is parsed once.
            shadow = await raw.zrevrange(SHADOW_KEY, 0, SHADOW_READ - 1)
        except Exception as exc:
            swallowed("api_gateway.strip.held_back", exc, logger)
        held_by = held_back_for_all(shadow, names, FAMILY_DESCRIPTIONS, 20) if shadow else {}
        rows = []
        for t in names:
            q = mq.build_quote(t, bars.get(t), daily.get(t, []), now)
            earnings = ss.next_earnings(listed.get(t), last.get(t), today)
            held = len(held_by.get(t.strip().upper(), [])) if shadow else None
            rows.append(ws.build_row(t, q if q["price"] is not None else None, earnings, model_row(scores, t, as_of), held, regimes.get(t)))
        return {"as_of": now.isoformat(), "rows": rows}

    return await cached_json(raw, "api:strip:" + ",".join(names), compute, ttl=10, max_stale=60)

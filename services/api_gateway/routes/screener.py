"""
services/api_gateway/routes/screener.py

A screener over the names the nightly model ranks: price, day change, volume against its 20-day average, the model's percentile and the
next earnings date (shared/utils/screener.py). The universe is built once and cached; each request sorts and filters it.
"""
import json
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils import market_quotes as mq
from shared.utils import screener as sc
from shared.utils import stock_snapshot as ss
from shared.utils.sectors import sector_of
from shared.utils.market_cap import CAPS_KEY
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cached_json
from shared.utils.signal_store import FACTOR_REPORT_KEY, FACTOR_SCORES_KEY, factor_scores_key

logger = logging.getLogger("api.screener")
router = APIRouter(prefix="/api/v1/stocks", tags=["Screener"])

_BARS_SQL = """
    SELECT DISTINCT ON (ticker) ticker, time, close FROM tradfi_bars
    WHERE ticker = ANY($1::text[]) AND time > NOW() - INTERVAL '10 days' AND close > 0
    ORDER BY ticker, time DESC
"""
_DAILY_SQL = """
    SELECT ticker, day, close, volume FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 45 AND close > 0 ORDER BY ticker, day
"""
_PROFILE_SQL = "SELECT ticker, sic, sic_description FROM company_profile WHERE ticker = ANY($1::text[])"
_LAST_RESULTS_SQL = """
    SELECT primary_entity_id AS ticker, max(occurred_at) AS at FROM signal_event_archive
    WHERE type = 'filing' AND summary LIKE '%2.02:%' AND primary_entity_id = ANY($1::text[])
      AND occurred_at > NOW() - INTERVAL '200 days' GROUP BY 1
"""
_EVENTS_SQL = """
    SELECT ticker, min(report_date) AS d FROM earnings_estimates
    WHERE ticker = ANY($1::text[]) AND report_date >= CURRENT_DATE GROUP BY ticker
"""


def _json(blob: Any) -> Any:
    if not blob:
        return None
    try:
        return json.loads(blob.decode() if isinstance(blob, bytes) else blob)
    except ValueError:
        return None


async def _universe(db: Any, raw: Any, horizon: Optional[str]) -> Dict[str, Any]:
    scores = _json(await raw.get(factor_scores_key(horizon) if horizon else FACTOR_SCORES_KEY)) or {}
    report = _json(await raw.get(FACTOR_REPORT_KEY)) or {}
    model_as_of = report.get("as_of")
    names: List[str] = sorted(scores)
    if not names:
        return {"as_of": datetime.now(timezone.utc).isoformat(), "model_as_of": model_as_of, "horizon": horizon, "rows": []}
    now = datetime.now(timezone.utc)
    today = now.astimezone(ZoneInfo("America/New_York")).date()
    try:
        bars = {str(r["ticker"]).upper(): (r["time"], r["close"]) for r in await db.query(_BARS_SQL, names) or []}
        daily: Dict[str, list] = {}
        vols: Dict[str, list] = {}
        for r in await db.query(_DAILY_SQL, names) or []:
            t = str(r["ticker"]).upper()
            daily.setdefault(t, []).append((r["day"], r["close"]))
            vols.setdefault(t, []).append(r["volume"])
        events = {str(r["ticker"]).upper(): r["d"] for r in await db.query(_EVENTS_SQL, names) or []}
        profile = {str(r["ticker"]).upper(): r for r in await db.query(_PROFILE_SQL, names) or []}
        last_results = {str(r["ticker"]).upper(): ss.as_day(r["at"]) for r in await db.query(_LAST_RESULTS_SQL, names) or []}
    except Exception as exc:
        swallowed("api_gateway.screener.read", exc, logger)
        raise HTTPException(status_code=503, detail="The screener is temporarily unavailable.")
    caps: Dict[str, float] = {}
    try:
        for t, v in zip(names, await raw.hmget(CAPS_KEY, names)):
            if v is not None:
                caps[t] = float(v.decode() if isinstance(v, bytes) else v)
    except Exception as exc:
        swallowed("api_gateway.screener.caps", exc, logger)
    rows = []
    for t in names:
        ev = events.get(t)
        if ev:
            event = {"kind": "earnings", "date": ev.isoformat() if hasattr(ev, "isoformat") else str(ev), "estimated": False}
        else:
            # No listed date: one quarter after the last results 8-K, flagged as an estimate (the snapshot route does the same).
            est = ss.next_earnings(None, last_results.get(t), today)
            event = {"kind": "earnings", "date": est["date"], "estimated": True} if est else None
        prof = profile.get(t) or {}
        rows.append(sc.build_row(t, mq.build_quote(t, bars.get(t), daily.get(t, []), now), vols.get(t, []),
                                 (scores[t] or {}).get("percentile"), model_as_of, event, caps.get(t),
                                 sector=sector_of(prof.get("sic")) if prof else None, industry=prof.get("sic_description")))
    return {"as_of": now.isoformat(), "model_as_of": model_as_of, "horizon": horizon, "rows": rows}


@router.get("/screener")
async def screener(
    sort: str = Query("change_pct", pattern="^(change_pct|rel_volume|model_pct|next_event)$"),
    dir: Optional[str] = Query(None, pattern="^(asc|desc)$", description="Default: descending, except next_event (soonest first)."),
    limit: int = Query(100, ge=1, le=sc.MAX_LIMIT),
    min_cap: Optional[float] = Query(None, ge=0, description="Minimum market cap in USD; names with an unknown cap are excluded."),
    horizon: Optional[str] = Query(None, pattern="^(daily|weekly|monthly)$"),
    sector: Optional[str] = Query(None, description="One of the heat map's sectors, e.g. 'Energy'; stocks without a sector are excluded."),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """{as_of, model_as_of, horizon, universe, rows:[{ticker, price, change_pct, volume_vs_20d, model_percentile, model_as_of, next_event:
    {kind, date, estimated}, market_cap, sector, industry}]} over the names the model ranks. next_event.estimated is true when no vendor
    date is listed and the date is one quarter after the last results 8-K; sector is the same 11 plain sectors as the heat map (null when
    the stock has no SEC industry code). change_pct uses the corrected base (last completed close);
    volume_vs_20d is the latest completed session over its 20 prior sessions; next_event is the listed earnings date. Missing is null,
    sorted last either way; `next_event` ascending is soonest first."""
    if db is None or redis is None:
        raise HTTPException(status_code=503, detail="The screener is temporarily unavailable.")
    raw = getattr(redis, "raw", redis)
    data = await cached_json(raw, f"api:screener:universe:v2:{horizon or 'primary'}", lambda: _universe(db, raw, horizon), ttl=30, max_stale=300)
    rows = sc.select(data["rows"], sort, dir or ("asc" if sort == "next_event" else "desc"), limit, min_cap, sector)
    return {"as_of": data["as_of"], "model_as_of": data["model_as_of"], "horizon": data["horizon"],
            "universe": len(data["rows"]), "rows": rows}

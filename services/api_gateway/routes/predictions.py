"""
services/api_gateway/routes/predictions.py

Stock predictions: every directional call the agents have made on a listed
instrument, open and resolved, with the record that should decide how much any
of them is worth.

/predictions/stocks   open calls with live price and progress toward target,
                      resolved calls with outcome, the hit rate as a shrunk
                      estimate with its interval (shared.utils.bayes), and a
                      calibration table: realised hit rate by conviction band.

A raw hit rate on a few dozen calls is mostly noise, and a conviction score
means nothing until it is checked against what happened. Both are shown here
so a reader does not have to take either on trust.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_client
from shared.utils.agent_conclusions import PREDICTIONS_KEY_PATTERN
from shared.utils.bayes import posterior_hit_rate
from shared.utils.quiet_failures import swallowed
from shared.utils.quote_cache import parse_quote, quote_key
from shared.utils.rbac import Role, require_role
from shared.utils.options_positioning import positioning_key
from shared.utils.signal_store import (
    FACTOR_REPORT_KEY, FACTOR_SCORES_KEY, RECOMMENDATIONS_KEY, factor_scores_key,
)

logger = logging.getLogger("api.predictions")

router = APIRouter(prefix="/api/v1/predictions", tags=["Predictions"])

# A listed stock or ETF symbol; crypto pairs and futures are other desks.
_STOCK = re.compile(r"^[A-Z]{1,5}(\.[A-Z])?$")
_CRYPTO = {"BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LTC", "BNB", "DOT", "LINK"}
CONVICTION_BANDS = [(0.0, 0.55), (0.55, 0.65), (0.65, 0.75), (0.75, 1.01)]


def is_stock(ticker: str) -> bool:
    t = str(ticker or "").upper()
    return bool(_STOCK.match(t)) and t not in _CRYPTO


def _decode(v: Any) -> str:
    return v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else str(v)


def interval(hits: int, n: int, z: float = 1.64) -> Optional[List[float]]:
    """A 90% Wilson interval for a hit rate; None without data."""
    if n <= 0:
        return None
    p = hits / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / denom
    return [round(max(0.0, centre - half), 3), round(min(1.0, centre + half), 3)]


# How far a recorded entry may sit from the stock's own price at the time.
MAX_ENTRY_DEVIATION = 0.15


def entry_check(entry: Any, price: Any) -> str:
    """'ok', 'no_entry', 'mismatch' (entry is not this stock's price) or 'unverified'."""
    try:
        e = float(entry or 0)
    except (TypeError, ValueError):
        e = 0.0
    if e <= 0:
        return "no_entry"
    try:
        p = float(price) if price is not None else None
    except (TypeError, ValueError):
        p = None
    if not p:
        return "unverified"
    return "ok" if abs(e / p - 1.0) <= MAX_ENTRY_DEVIATION else "mismatch"


def summarise(resolved: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Hit rate raw and shrunk, Brier, and calibration by conviction band.

    Calls whose entry was not the stock's price, or had none, are excluded:
    the model-written advisory recorded peers at the triggering ticker's price,
    and a wargamer "call" on YEMEN at 0.00 is not a stock prediction."""
    excluded = sum(1 for r in resolved if r.get("entry_check") in ("mismatch", "no_entry"))
    resolved = [r for r in resolved if r.get("entry_check") not in ("mismatch", "no_entry")]
    n = len(resolved)
    hits = sum(1 for r in resolved if r.get("outcome_correct"))
    briers = [float(r["brier_score"]) for r in resolved if r.get("brier_score") is not None]
    bands = []
    for lo, hi in CONVICTION_BANDS:
        rows = [r for r in resolved if r.get("confidence") is not None and lo <= float(r["confidence"]) < hi]
        h = sum(1 for r in rows if r.get("outcome_correct"))
        bands.append({
            "band": f"{lo:.2f}-{min(hi, 1.0):.2f}",
            "calls": len(rows),
            "mean_conviction": round(sum(float(r["confidence"]) for r in rows) / len(rows), 3) if rows else None,
            "hit_rate": round(h / len(rows), 3) if rows else None,
        })
    return {
        "resolved": n,
        "excluded_invalid": excluded,
        "hits": hits,
        "raw_hit_rate": round(hits / n, 3) if n else None,
        "shrunk_hit_rate": round(posterior_hit_rate(hits, n), 3) if n else None,
        "interval_90": interval(hits, n),
        "brier": round(sum(briers) / len(briers), 4) if briers else None,
        "calibration": bands,
    }


@router.get("/stocks")
async def stock_predictions(
    days: int = Query(30, ge=1, le=365),
    redis_client=Depends(get_redis_client),
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    raw = getattr(redis_client, "raw", redis_client)
    now = datetime.now(timezone.utc)

    open_calls: List[Dict[str, Any]] = []
    try:
        cursor, keys = 0, []
        while True:
            cursor, batch = await raw.scan(cursor=cursor, match=PREDICTIONS_KEY_PATTERN, count=500)
            keys.extend(k for k in batch or [] if _decode(k).count(":") == 3)
            if cursor == 0 or len(keys) >= 3000:
                break
        blobs = await raw.mget(keys) if keys else []
        preds = []
        for blob in blobs:
            if not blob:
                continue
            try:
                p = json.loads(_decode(blob))
            except ValueError:
                continue
            if p.get("verified") or p.get("prediction_kind", "price") != "price" or not is_stock(p.get("ticker", "")):
                continue
            preds.append(p)
        quotes = await raw.mget([quote_key(p["ticker"]) for p in preds]) if preds else []
        for p, q in zip(preds, quotes):
            price = parse_quote(q)
            entry = float(p.get("entry_price") or 0)
            target = float(p.get("target_price") or 0)
            sign = 1 if p.get("direction") == "up" else -1 if p.get("direction") == "down" else 0
            move_pct = round((price / entry - 1) * 100 * (sign or 1), 3) if (price and entry) else None
            progress = None
            if price and entry and target and target != entry:
                progress = round(max(-1.0, min(1.5, (price - entry) / (target - entry))), 3)
            created = p.get("created_at")
            try:
                age_h = (now - datetime.fromisoformat(str(created).replace("Z", "+00:00"))).total_seconds() / 3600
            except ValueError:
                age_h = None
            features = p.get("features") or {}
            open_calls.append({
                "entry_check": entry_check(entry, price) if price else ("no_entry" if not entry else "unverified"),
                "prediction_id": p.get("prediction_id"),
                "agent": p.get("agent_name"),
                "ticker": p.get("ticker"),
                "direction": p.get("direction"),
                "conviction": p.get("conviction"),
                "entry_price": entry or None,
                "target_price": target or None,
                "price": price,
                "move_in_favour_pct": move_pct,
                "progress_to_target": progress,
                "horizon_hours": p.get("time_horizon_hours"),
                "hours_left": round(float(p.get("time_horizon_hours") or 0) - age_h, 1) if age_h is not None else None,
                "created_at": created,
                "evidence_status": features.get("evidence_status"),
                "trigger_families": features.get("trigger_families") or [],
            })
    except Exception as exc:
        swallowed("api.predictions.open", exc, logger)
    open_calls.sort(key=lambda c: (c.get("hours_left") is None, c.get("hours_left") or 0))

    resolved: List[Dict[str, Any]] = []
    if db:
        try:
            rows = await db.query(
                """
                SELECT p.prediction_id, p.agent_name, p.ticker, p.direction, p.confidence,
                       p.entry_price, p.horizon_hours, p.occurred_at, p.resolved_at,
                       p.outcome_correct, p.brier_score, b.close AS price_then
                FROM agent_predictions p
                LEFT JOIN LATERAL (
                    SELECT close FROM tradfi_bars
                    WHERE ticker = p.ticker
                      -- The nearest bar within three days: a weekend or a thin
                      -- name has none within the hour, and an entry of 7.16 on
                      -- GOOGL is wrong whichever day's bar it is checked against.
                      AND time BETWEEN p.occurred_at - INTERVAL '3 days' AND p.occurred_at + INTERVAL '3 days'
                    ORDER BY abs(extract(epoch FROM time - p.occurred_at))
                    LIMIT 1
                ) b ON TRUE
                WHERE p.resolved_at > NOW() - ($1 || ' days')::INTERVAL
                  AND p.ticker IS NOT NULL
                ORDER BY p.resolved_at DESC
                LIMIT 2000
                """,
                str(days),
            )
            resolved = [dict(r) for r in rows or [] if is_stock(dict(r).get("ticker", ""))]
            for r in resolved:
                r["entry_check"] = entry_check(r.get("entry_price"), r.pop("price_then", None))
        except Exception as exc:
            swallowed("api.predictions.resolved", exc, logger)

    by_agent: Dict[str, Any] = {}
    for agent in sorted({r.get("agent_name") or "unattributed" for r in resolved}):
        by_agent[agent] = summarise([r for r in resolved if (r.get("agent_name") or "unattributed") == agent])

    return {
        "generated_at": now.isoformat(),
        "window_days": days,
        "open": open_calls,
        "summary": summarise(resolved),
        "by_agent": by_agent,
        "recent": [
            {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items()}
            for r in resolved[:100]
        ],
    }


def _load_json(blob: Any) -> Any:
    if not blob:
        return None
    try:
        return json.loads(_decode(blob))
    except ValueError:
        return None


@router.get("/model")
async def model_ranking(
    limit: int = Query(20, ge=1, le=100),
    horizon: Optional[str] = Query(None, pattern="^(daily|weekly|monthly)$"),
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """The nightly cross-sectional model: its walk-forward validation and today's
    strongest and weakest ranks, for the primary horizon or the one asked for.
    Rankings from an unvalidated model are marked so."""
    raw = getattr(redis_client, "raw", redis_client)
    report = _load_json(await raw.get(FACTOR_REPORT_KEY))
    if not report:
        return {"report": None, "top": [], "bottom": [],
                "note": "The model runs nightly once enough daily history has loaded."}
    horizons = report.get("horizons") or {}
    if horizon and horizon in horizons:
        h = horizons[horizon]
        report = {**report, "models": h.get("models", []), "chosen": h.get("chosen"),
                  "validated": h.get("validated"), "horizon_days": h.get("horizon_days"),
                  "selected_horizon": horizon}
        scores = _load_json(await raw.get(factor_scores_key(horizon))) or {}
    else:
        report = {**report, "selected_horizon": report.get("primary_horizon")}
        scores = _load_json(await raw.get(FACTOR_SCORES_KEY)) or {}
    ranked = sorted(scores.items(), key=lambda kv: kv[1].get("percentile", 0), reverse=True)
    row = lambda kv: {"ticker": kv[0], **kv[1]}  # noqa: E731
    return {
        "report": report,
        "top": [row(kv) for kv in ranked[:limit]],
        "bottom": [row(kv) for kv in ranked[-limit:][::-1]],
    }


@router.get("/model/{ticker}")
async def model_rank_for(
    ticker: str,
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    raw = getattr(redis_client, "raw", redis_client)
    report = _load_json(await raw.get(FACTOR_REPORT_KEY)) or {}
    scores = _load_json(await raw.get(FACTOR_SCORES_KEY)) or {}
    t = ticker.strip().upper()
    return {
        "ticker": t,
        "entry": scores.get(t),
        "validated": bool(report.get("validated")),
        "model": report.get("chosen"),
        "as_of": report.get("as_of"),
        "horizon_days": report.get("horizon_days"),
    }


_OPTION_DAILY_SQL = """
    SELECT day, put_call_ratio, iv_skew, atm_iv, cp_iv_spread, cp_pairs, call_volume, put_volume
    FROM option_daily WHERE ticker = $1 ORDER BY day DESC LIMIT 30
"""
_SHORT_INTEREST_SQL = """
    SELECT settlement_date, short_qty, avg_daily_volume, days_to_cover, change_pct
    FROM short_interest WHERE ticker = $1 ORDER BY settlement_date DESC LIMIT 12
"""


@router.get("/positioning/{ticker}")
async def positioning_for(
    ticker: str,
    redis_client=Depends(get_redis_client),
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Option-implied and short-side positioning for one name: today's put/call
    volume, IV skew and call-put IV spread, their daily history, and FINRA short
    interest with days-to-cover. Measured, not yet studied on this system."""
    t = ticker.strip().upper()
    if not _STOCK.match(t):
        return {"ticker": t, "live": None, "daily": [], "short_interest": []}
    raw = getattr(redis_client, "raw", redis_client)
    live = _load_json(await raw.get(positioning_key(t)))
    daily: List[Dict[str, Any]] = []
    short: List[Dict[str, Any]] = []
    if db:
        try:
            daily = [{**dict(r), "day": str(r["day"])} for r in await db.query(_OPTION_DAILY_SQL, t) or []]
            short = [{**dict(r), "settlement_date": str(r["settlement_date"])}
                     for r in await db.query(_SHORT_INTEREST_SQL, t) or []]
        except Exception as e:
            swallowed("api_gateway.predictions.positioning", e, logger)
    return {"ticker": t, "live": live, "daily": daily, "short_interest": short}


@router.get("/recommendations")
async def recommendations(
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Trade ideas by horizon -- stock and option structure -- and the signal
    engine's live intraday signals (services/agents/recommendation_engine.py).
    Each horizon says whether its model passed validation; ideas from one that
    has not are research, not evidence."""
    raw = getattr(redis_client, "raw", redis_client)
    data = _load_json(await raw.get(RECOMMENDATIONS_KEY))
    if not data:
        return {"horizons": {}, "intraday": [],
                "note": "Ideas are built from the nightly model; none yet."}
    return data

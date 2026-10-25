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
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_client
from shared.utils.agent_conclusions import PREDICTIONS_KEY_PATTERN
from shared.utils.bayes import posterior_hit_rate
from shared.utils.quiet_failures import swallowed
from shared.utils.quote_cache import parse_quote, quote_key
from shared.utils.rbac import Role, require_role
from shared.utils.signal_plain import stand_out
from shared.utils.options_positioning import positioning_key
from shared.utils.signal_store import (
    FACTOR_REPORT_KEY, FACTOR_SCORES_KEY, PRODUCER_GRADES_KEY, RECOMMENDATIONS_KEY, factor_scores_key,
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
    entry = scores.get(t)
    return {
        "ticker": t,
        "entry": entry,
        # What stands out about it, in words, for the stock page.
        "traits": stand_out((entry or {}).get("features") if isinstance(entry, dict) else None),
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


_CLOSES_SQL = """
    SELECT day, close FROM (
        SELECT day, close FROM daily_bars WHERE ticker = $1 ORDER BY day DESC LIMIT $2
    ) recent ORDER BY day
"""


@router.get("/closes/{ticker}")
async def closes_for(
    ticker: str,
    days: int = Query(90, ge=2, le=400),
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Daily closes for one stock, oldest first, from the daily history the
    model itself reads -- the stock page's chart and the model agree on price."""
    t = ticker.strip().upper()
    if not _STOCK.match(t) or not db:
        return {"ticker": t, "closes": []}
    try:
        rows = await db.query(_CLOSES_SQL, t, days) or []
    except Exception as e:
        swallowed("api_gateway.predictions.closes", e, logger)
        rows = []
    return {"ticker": t, "closes": [{"day": str(r["day"]), "close": float(r["close"])} for r in rows]}


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


_GRADED_IDEAS_SQL = """
    SELECT horizon, validated, structure, direction, signed_excess, hit, day FROM recommendation_log
    WHERE graded_at IS NOT NULL
"""
_GRADED_SNAPSHOTS_SQL = """
    SELECT net, proven_net, supported_net, published_net, excess_1d, excess_5d, day FROM signal_matrix_log
    WHERE graded_1d_at IS NOT NULL OR graded_5d_at IS NOT NULL
"""


@router.get("/track-record")
async def track_record(
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Every graded trade idea and signal-board snapshot, net of SPY.

    Ideas by horizon; the board by its net vote and by which tier carried it --
    the test of whether the board's votes predict anything, published as it
    accumulates rather than asserted."""
    from shared.utils.track_record import bucket_of, day_clustered_t, summarize

    out: Dict[str, Any] = {"ideas": {}, "board": {}, "pending": {}}
    if not db:
        return out
    try:
        ideas = await db.query(_GRADED_IDEAS_SQL) or []
        snaps = await db.query(_GRADED_SNAPSHOTS_SQL) or []
        pend = await db.query(
            "SELECT (SELECT count(*) FROM recommendation_log WHERE graded_at IS NULL) AS ideas, "
            "(SELECT count(*) FROM signal_matrix_log WHERE graded_5d_at IS NULL) AS snapshots") or [{}]
    except Exception as e:
        swallowed("api_gateway.predictions.track_record", e, logger)
        return out
    out["pending"] = dict(pend[0]) if pend else {}
    by_h: Dict[str, List[float]] = {}
    for r in ideas:
        by_h.setdefault(str(r["horizon"]), []).append(r["signed_excess"])
    out["ideas"] = {h: summarize(v) for h, v in by_h.items()}
    out["ideas"]["all"] = summarize([r["signed_excess"] for r in ideas])
    def dated(rows: List[Tuple[Any, float]], overlap: int) -> Dict[str, Any]:
        # Row t counts every stock-day as independent; t_days counts days.
        return {**summarize([x for _, x in rows]), **day_clustered_t(rows, overlap)}

    for k, overlap in (("excess_1d", 1), ("excess_5d", 5)):
        buckets: Dict[str, List[Tuple[Any, float]]] = {}
        signed: Dict[str, List[Tuple[Any, float]]] = {"proven": [], "supported": [], "published only": [],
                                                      "any vote": []}
        for r in snaps:
            x = r[k]
            if x is None:
                continue
            d = r["day"]
            net = float(r["net"] or 0)
            buckets.setdefault(bucket_of(net), []).append((d, x))
            if net:
                s = x * (1 if net > 0 else -1)
                signed["any vote"].append((d, s))
                if r["proven_net"]:
                    signed["proven"].append((d, x * (1 if r["proven_net"] > 0 else -1)))
                elif r["supported_net"]:
                    signed["supported"].append((d, x * (1 if r["supported_net"] > 0 else -1)))
                elif r["published_net"]:
                    signed["published only"].append((d, s))
        out["board"][k] = {
            "by_net_bucket": {b: dated(v, overlap) for b, v in buckets.items()},
            "signed_by_tier": {t: dated(v, overlap) for t, v in signed.items()},
        }
    return out


_PAPER_SQL = """
    SELECT idea_day, horizon, ticker, direction, model, status, entry_day, entry_price, cost_bps, adv_usd,
           exit_day, exit_price, exit_reason, gross_excess, net_excess, pnl_usd, notional_usd, scale, pnl_scaled_usd
    FROM paper_trades ORDER BY idea_day DESC, horizon, ticker LIMIT 2000
"""


_OPEN_MARKS_SQL = """
    SELECT p.horizon, p.direction, p.entry_price, p.spy_entry, p.cost_bps, p.notional_usd, b.close, s.close AS spy_close
    FROM paper_trades p
    JOIN LATERAL (SELECT close FROM daily_bars WHERE ticker = p.ticker ORDER BY day DESC LIMIT 1) b ON true
    JOIN LATERAL (SELECT close FROM daily_bars WHERE ticker = 'SPY' ORDER BY day DESC LIMIT 1) s ON true
    WHERE p.status = 'open'
"""


@router.get("/paper-ledger")
async def paper_ledger(
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Every trade idea traded on paper: filled at the next open, costed both
    ways, exited at its stop or horizon, net of SPY (shared/utils/paper_ledger.py).
    The record of what the ideas would actually have made."""
    from shared.utils.paper_ledger import open_mark
    from shared.utils.track_record import summarize

    out: Dict[str, Any] = {"by_horizon": {}, "open": 0, "pending": 0, "trades": []}
    if not db:
        return out
    try:
        rows = await db.query(_PAPER_SQL) or []
    except Exception as e:
        swallowed("api_gateway.predictions.paper_ledger", e, logger)
        return out
    closed: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        if r["status"] == "closed":
            closed.setdefault(str(r["horizon"]), []).append(r)
        out["open"] += r["status"] == "open"
        out["pending"] += r["status"] == "pending"
    for h, rs in list(closed.items()) + [("all", [r for v in closed.values() for r in v])]:
        net = summarize([r["net_excess"] for r in rs])
        out["by_horizon"][h] = {
            **net,
            "gross": summarize([r["gross_excess"] for r in rs]),
            "pnl_usd": round(sum(float(r["pnl_usd"] or 0) for r in rs), 0),
            # Volatility-managed sizing on the same trades (Moreira & Muir).
            "pnl_scaled_usd": round(sum(float(r["pnl_scaled_usd"] or 0) for r in rs), 0),
            "mean_cost_bps": round(sum(float(r["cost_bps"] or 0) for r in rs) / len(rs), 1) if rs else None,
            "stopped": sum(1 for r in rs if r["exit_reason"] == "stop"),
        }
    try:
        marks = await db.query(_OPEN_MARKS_SQL) or []
    except Exception as e:
        swallowed("api_gateway.predictions.paper_ledger_marks", e, logger)
        marks = []
    # Open positions at the latest close, both sides of costs charged. A horizon
    # longer than the ledger's history has closed only the trades its stops cut,
    # so each horizon also reports its open trades and everything taken together.
    open_net: Dict[str, List[float]] = {}
    unreal = 0.0
    for m in marks:
        net = open_mark(int(m["direction"]), m["entry_price"], m["spy_entry"], m["close"], m["spy_close"],
                        m["cost_bps"])
        if net is None:
            continue
        open_net.setdefault(str(m["horizon"]), []).append(net)
        unreal += float(m["notional_usd"] or 0) * net
    out["open_mark_usd"] = round(unreal, 0)
    for h in set(closed) | set(open_net):
        nets = open_net.get(h, [])
        row = out["by_horizon"].setdefault(h, {**summarize([]), "stopped": 0})
        row["open_marked"] = summarize(nets)
        row["with_open"] = summarize([r["net_excess"] for r in closed.get(h, [])] + nets)
    if open_net:
        all_open = [x for v in open_net.values() for x in v]
        row = out["by_horizon"].setdefault("all", {**summarize([]), "stopped": 0})
        row["open_marked"] = summarize(all_open)
        row["with_open"] = summarize([r["net_excess"] for v in closed.values() for r in v] + all_open)
    out["trades"] = [{k: (str(v) if hasattr(v, "isoformat") else v) for k, v in dict(r).items()} for r in rows[:200]]
    return out


_AUTO_ORDERS_SQL = """
    SELECT day, symbol, side, qty, reason, status, error, submitted_at FROM auto_trade_orders
    WHERE portfolio = $1 ORDER BY submitted_at DESC LIMIT 60
"""
_AUTO_DAYS_SQL = """
    SELECT day, count(*) FILTER (WHERE status = 'submitted') AS sent,
           count(*) FILTER (WHERE status = 'rejected') AS rejected
    FROM auto_trade_orders WHERE portfolio = $1 GROUP BY day ORDER BY day DESC LIMIT 10
"""


@router.get("/auto-trade")
async def auto_trade(
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """The model portfolio's positions as taken in the Alpaca paper account
    (services/agents/auto_trader.py): whether it is on, the account, what it
    holds, and the orders it sent. Read-only."""
    from services.agents.auto_trader import PORTFOLIO, enabled
    from shared.broker.alpaca import AlpacaBroker

    def plain(r: Dict[str, Any]) -> Dict[str, Any]:
        return {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in dict(r).items()}

    out: Dict[str, Any] = {"enabled": enabled(), "venue": "Alpaca paper", "portfolio": PORTFOLIO,
                           "account": None, "positions": [], "orders": [], "days": []}
    if db:
        try:
            out["orders"] = [plain(r) for r in await db.query(_AUTO_ORDERS_SQL, PORTFOLIO) or []]
            out["days"] = [plain(r) for r in await db.query(_AUTO_DAYS_SQL, PORTFOLIO) or []]
        except Exception as e:
            swallowed("api_gateway.predictions.auto_trade_orders", e, logger)
    broker = AlpacaBroker(paper=True)
    try:
        acct = await broker.get_account()
        positions = await broker.get_positions()
    except Exception as e:
        swallowed("api_gateway.predictions.auto_trade_account", e, logger)
        return out
    out["account"] = {"portfolio_value": acct.portfolio_value, "cash": acct.cash, "is_live": acct.is_live}
    rows = sorted(positions, key=lambda p: -abs(p.market_value))
    out["positions_count"] = len(rows)
    out["long_value"] = round(sum(p.market_value for p in rows if p.qty > 0), 2)
    out["short_value"] = round(sum(p.market_value for p in rows if p.qty < 0), 2)
    out["unrealized_pl"] = round(sum(p.unrealized_pl for p in rows), 2)
    out["positions"] = [{"symbol": p.symbol, "qty": p.qty, "market_value": p.market_value,
                         "unrealized_pl": p.unrealized_pl} for p in rows[:25]]
    return out


_MP_STATE_SQL = "SELECT * FROM model_portfolio_state WHERE portfolio = $1"
_MP_NAV_SQL = """
    SELECT day, nav, spy_nav, ret, spy_ret, n_long, n_short, missing FROM model_portfolio_nav
    WHERE portfolio = $1 ORDER BY day
"""
_MP_ORDERS_SQL = """
    SELECT decided_on, ticker, action, kind, reason, percentile, fill_day, fill_price FROM model_portfolio_orders
    WHERE portfolio = $1 AND decided_on IN (
        SELECT DISTINCT decided_on FROM model_portfolio_orders WHERE portfolio = $1 ORDER BY decided_on DESC LIMIT 5)
    ORDER BY decided_on DESC, action, ticker
"""
_MP_SINCE_SQL = "SELECT count(*) AS n FROM daily_bars WHERE ticker = 'SPY' AND day > $1::date"


@router.get("/model-portfolio")
async def model_portfolio(
    redis_client=Depends(get_redis_client),
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """The actively managed model portfolio on paper (shared/utils/model_portfolio.py):
    SPY plus the monthly model's long/short overlay, its holdings, its latest
    buy/sell/short/cover orders with their reasons, and its value against SPY."""
    from shared.utils import model_portfolio as mp

    def plain(r: Dict[str, Any]) -> Dict[str, Any]:
        return {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in dict(r).items()}

    out: Dict[str, Any] = {
        "portfolio": mp.HEADLINE_PORTFOLIO,
        "design": {"core": "SPY", "horizon": mp.HORIZON, "overlay_gross": mp.OVERLAY_GROSS,
                   "rebalance_sessions": mp.REBALANCE_SESSIONS, "borrow_fee_annual": mp.BORROW_FEE_ANNUAL},
        "summary": {"sessions": 0}, "nav": [], "orders": [], "holdings": {"long": [], "short": []},
    }
    if not db:
        return out
    try:
        state = await db.query_one(_MP_STATE_SQL, mp.HEADLINE_PORTFOLIO)
        navs = await db.query(_MP_NAV_SQL, mp.HEADLINE_PORTFOLIO) or []
        orders = await db.query(_MP_ORDERS_SQL, mp.HEADLINE_PORTFOLIO) or []
    except Exception as e:
        swallowed("api_gateway.predictions.model_portfolio", e, logger)
        return out
    if not state:
        return out
    scores = _load_json(await getattr(redis_client, "raw", redis_client).get(factor_scores_key(mp.HORIZON))) or {}

    def held(col: str) -> List[Dict[str, Any]]:
        names = _load_json(state.get(col)) if isinstance(state.get(col), str) else state.get(col)
        rows = [{"ticker": t, "percentile": (scores.get(t) or {}).get("percentile")} for t in names or []]
        return sorted(rows, key=lambda r: -(r["percentile"] if r["percentile"] is not None else -1))

    out["holdings"] = {"long": held("long_book"), "short": held("short_book")}
    since = None
    if state.get("rebalanced_on"):
        try:
            since = int(((await db.query_one(_MP_SINCE_SQL, state["rebalanced_on"])) or {}).get("n") or 0)
        except Exception as e:
            swallowed("api_gateway.predictions.model_portfolio_since", e, logger)
    pending = _load_json(state.get("pending")) if isinstance(state.get("pending"), str) else state.get("pending")
    out["state"] = {
        "started_on": str(state.get("started_on")) if state.get("started_on") else None,
        "marked_through": str(state.get("marked_through")) if state.get("marked_through") else None,
        "rebalanced_on": str(state.get("rebalanced_on")) if state.get("rebalanced_on") else None,
        "next_rebalance_in_sessions": max(0, mp.REBALANCE_SESSIONS - since) if since is not None else None,
        "pending": {"decided_on": pending.get("decided_on"), "kind": pending.get("kind")} if pending else None,
    }
    out["summary"] = mp.summary(navs)
    # The overlay's size in force for the next session (volatility target,
    # equity-regime gauge, beta hedge; shared/utils/model_portfolio.py).
    sizing = _load_json(state.get("sizing")) if isinstance(state.get("sizing"), str) else state.get("sizing")
    out["sizing"] = (sizing or [None])[-1]
    out["nav"] = [{"day": str(r["day"]), "nav": round(float(r["nav"]), 6), "spy_nav": round(float(r["spy_nav"]), 6)}
                  for r in navs]
    out["orders"] = [plain(r) for r in orders]
    # Shadow books on the same scores (shared/utils/model_portfolio.py): what a
    # different construction would have done over the same days, never traded.
    out["shadows"] = {}
    for label, name in mp.SHADOW_PORTFOLIOS:
        try:
            sh_state = await db.query_one(_MP_STATE_SQL, name)
            sh_navs = await db.query(_MP_NAV_SQL, name) or []
        except Exception as e:
            swallowed("api_gateway.predictions.model_portfolio_shadow", e, logger)
            continue
        if sh_state:
            out["shadows"][label] = {
                "started_on": str(sh_state.get("started_on")) if sh_state.get("started_on") else None,
                "summary": mp.summary(sh_navs),
                "nav": [{"day": str(r["day"]), "nav": round(float(r["nav"]), 6),
                         "spy_nav": round(float(r["spy_nav"]), 6)} for r in sh_navs],
            }
    return out


@router.get("/producer-grades")
async def producer_grades(
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Correlation rules, scenarios and agent calls graded against prices
    (shared/utils/producer_grades.py, refreshed daily by the grader): whether a
    rule's stocks then moved more than usual, and whether a claim beat its placebo."""
    raw = getattr(redis_client, "raw", redis_client)
    return _load_json(await raw.get(PRODUCER_GRADES_KEY)) or {
        "note": "Graded daily once the grader has run."}


_IC_SQL = "SELECT as_of, horizon, ic, n, end_day FROM factor_ic WHERE as_of > CURRENT_DATE - 400 ORDER BY as_of"
_LATEST_REPORT_SQL = "SELECT report FROM factor_model_runs ORDER BY run_at DESC LIMIT 1"


@router.get("/model-health")
async def model_health(
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Is the factor model still working? The live rank correlation (IC) of each
    night's scores with what the stocks then did against SPY, beside the IC the
    walk-forward promised for the chosen model (shared/utils/ic_monitor.py)."""
    from shared.utils import ic_monitor as icm

    out: Dict[str, Any] = {"horizons": {}}
    if not db:
        return out
    try:
        rows = await db.query(_IC_SQL) or []
        rep_row = await db.query_one(_LATEST_REPORT_SQL)
    except Exception as e:
        swallowed("api_gateway.predictions.model_health", e, logger)
        return out
    rep = _load_json(rep_row.get("report")) if rep_row and isinstance(rep_row.get("report"), str) else (
        rep_row.get("report") if rep_row else {}) or {}
    for h, days in icm.HORIZON_DAYS.items():
        hv = (rep.get("horizons") or {}).get(h) or {}
        chosen = hv.get("chosen")
        expected = next((m.get("mean_ic") for m in hv.get("models") or [] if m.get("model") == chosen), None)
        series = [r for r in rows if r["horizon"] == h]
        out["horizons"][h] = {
            "chosen_model": chosen,
            "supported": bool(hv.get("support")),
            **icm.health([r["ic"] for r in series], expected, days),
            "recent": [{"as_of": str(r["as_of"]), "end_day": str(r["end_day"]) if r["end_day"] else None,
                        "ic": r["ic"], "n": r["n"]} for r in series[-30:]],
        }
    return out



@router.get("/portfolio-lab")
async def portfolio_lab(
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """The portfolio constructions judged on the model's own out-of-sample
    history: the live book, the industry-neutral book and the probability-sized
    book, with how much of each is an industry bet (shared/utils/portfolio_lab.py)."""
    from shared.utils.portfolio_lab import PORTFOLIO_LAB_KEY
    raw = getattr(redis_client, "raw", redis_client)
    return _load_json(await raw.get(PORTFOLIO_LAB_KEY)) or {
        "note": "Run weekly with the nightly model; the first report follows its next run."}



@router.get("/equity-regime")
async def equity_regime(
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """The stock market's own regime: the momentum factor's drawdown and
    volatility, dispersion and the market's trend, and the overlay multiplier
    it sets (shared/utils/equity_regime.py)."""
    from shared.utils.equity_regime import EQUITY_REGIME_KEY
    raw = getattr(redis_client, "raw", redis_client)
    return _load_json(await raw.get(EQUITY_REGIME_KEY)) or {"level": None, "note": "Computed with the nightly model."}


@router.get("/news-model")
async def news_model(
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """The news-embedding model's walk-forward (shared/utils/news_model.py):
    whether headlines' embeddings predict the next session, and whether live
    scores are switched on."""
    from shared.utils.news_model import NEWS_MODEL_KEY
    raw = getattr(redis_client, "raw", redis_client)
    rep = _load_json(await raw.get(NEWS_MODEL_KEY)) or {"note": "Runs weekly in the correlation service."}
    rep.pop("weights", None)
    return rep


_STOCK_NEWS_SQL = """
    SELECT id, created_at, symbols, headline, source, url, positive - negative AS tone
    FROM stock_news
    WHERE ($1::text IS NULL OR symbols @> ARRAY[$1::text]) AND created_at > NOW() - make_interval(days => $2)
    ORDER BY created_at DESC LIMIT $3
"""
_NEWS_MOVERS_SQL = """
    SELECT sym AS ticker, count(*) AS n, avg(positive - negative) AS tone,
           (array_agg(headline ORDER BY abs(positive - negative) DESC))[1] AS top_headline
    FROM stock_news, unnest(symbols) AS sym
    WHERE scored_at IS NOT NULL AND created_at > NOW() - make_interval(hours => $1)
      AND cardinality(symbols) BETWEEN 1 AND 3
    GROUP BY sym HAVING count(*) >= $2
"""


@router.get("/stock-news")
async def stock_news(
    ticker: Optional[str] = Query(None, pattern=r"^[A-Za-z][A-Za-z0-9.\-]{0,9}$"),
    days: int = Query(3, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200),
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Company news from Alpaca's feed, newest first, with FinBERT's tone
    (positive minus negative, -1..1; null until scored). shared/utils/stock_news.py."""
    if not db:
        return {"articles": []}
    try:
        rows = await db.query(_STOCK_NEWS_SQL, ticker.upper() if ticker else None, days, limit) or []
    except Exception as e:
        swallowed("api_gateway.predictions.stock_news", e, logger)
        return {"articles": []}
    return {"articles": [{"id": int(r["id"]), "at": str(r["created_at"]), "symbols": list(r["symbols"] or []),
                          "headline": r["headline"], "source": r["source"], "url": r["url"],
                          "tone": round(float(r["tone"]), 3) if r["tone"] is not None else None} for r in rows]}


@router.get("/news-movers")
async def news_movers(
    hours: int = Query(24, ge=1, le=168),
    min_articles: int = Query(2, ge=1, le=20),
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """The stocks whose news read most positive and most negative over the
    last `hours`, weighted by how much news there was."""
    if not db:
        return {"positive": [], "negative": []}
    try:
        rows = await db.query(_NEWS_MOVERS_SQL, hours, min_articles) or []
    except Exception as e:
        swallowed("api_gateway.predictions.news_movers", e, logger)
        return {"positive": [], "negative": []}
    import math
    items = [{"ticker": r["ticker"], "articles": int(r["n"]), "tone": round(float(r["tone"]), 3),
              "weight": round(float(r["tone"]) * math.sqrt(int(r["n"])), 3), "headline": r["top_headline"]} for r in rows]
    return {"hours": hours,
            "positive": sorted((i for i in items if i["tone"] > 0), key=lambda i: -i["weight"])[:10],
            "negative": sorted((i for i in items if i["tone"] < 0), key=lambda i: i["weight"])[:10]}

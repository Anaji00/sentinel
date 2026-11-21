"""Stores each resolved agent prediction's return against SPY and its sector ETF (report only; no gate reads these columns).

`grade_pending` is used by the daily grader pass and by scripts/backfill_excess_returns.py. It is idempotent (a graded row is
never selected again), bounded (a row that finds no bars is parked after MAX_ATTEMPTS) and contains each row's errors.
"""
from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

from shared.utils.excess_return import (
    ALIGNED_MIN_HORIZON_HOURS, BETA_SESSIONS, aligned_window, bars_late, beta_excess, beta_to_spy, call_window, direction_sign, excess_returns, sector_etf_for_sic, window_return,
)
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agents.excess_grading")

MAX_ATTEMPTS = 3


def aligned_windows_enabled() -> bool:
    """Session-aligned windows (layer audit 5.2) are off until the stored grades are re-graded with them (scripts/regrade_agent_windows.py), so one series never
    holds two window definitions. Set EXCESS_ALIGNED_WINDOWS=1 together with that re-grade."""
    return os.environ.get("EXCESS_ALIGNED_WINDOWS", "") == "1"
LOOKBACK_PAD_DAYS = 10      # bars needed before the first call date so "last close on or before" finds one

_PENDING_SQL = """
    SELECT p.id, upper(p.ticker) AS ticker, p.direction, p.occurred_at, p.horizon_hours, cp.sic
    FROM agent_predictions p LEFT JOIN company_profile cp ON cp.ticker = upper(p.ticker)
    WHERE p.resolved_at IS NOT NULL AND p.excess_graded_at IS NULL AND p.excess_attempts < $1
      AND p.ticker IS NOT NULL AND coalesce(p.horizon_hours, 0) > 0
      AND lower(p.direction) IN ('up', 'down', 'long', 'short', 'bullish', 'bearish')
      AND (p.occurred_at + make_interval(hours => p.horizon_hours))::date < CURRENT_DATE - 1
    ORDER BY p.occurred_at LIMIT $2
"""
_BARS_SQL = """
    SELECT ticker, day, close FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day >= $2::date AND day <= $3::date ORDER BY ticker, day
"""
_GRADED_SQL = """
    UPDATE agent_predictions SET stock_return = $2, spy_return = $3, sector_etf = $4, sector_return = $5,
        excess_spy = $6, excess_sector = $7, excess_graded_at = NOW()
    WHERE id = $1 AND excess_graded_at IS NULL
"""
_ATTEMPT_SQL = "UPDATE agent_predictions SET excess_attempts = excess_attempts + 1 WHERE id = $1"
_BETA_PENDING_SQL = """
    SELECT id, upper(ticker) AS ticker, occurred_at, stock_return, spy_return FROM agent_predictions
    WHERE excess_graded_at IS NOT NULL AND excess_beta IS NULL AND excess_beta_attempts < $1
      AND stock_return IS NOT NULL AND spy_return IS NOT NULL
    ORDER BY occurred_at LIMIT $2
"""
_BETA_BARS_SQL = """
    SELECT ticker, day, close FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day >= $2::date AND day < $3::date ORDER BY ticker, day
"""
_BETA_SET_SQL = "UPDATE agent_predictions SET beta_126d = $2, excess_beta = $3 WHERE id = $1 AND excess_beta IS NULL"
_BETA_ATTEMPT_SQL = "UPDATE agent_predictions SET excess_beta_attempts = excess_beta_attempts + 1 WHERE id = $1"


async def grade_pending(db: Any, limit: int = 500) -> Dict[str, int]:
    """Grades up to `limit` waiting rows. Returns counts: selected, graded, no_bars (attempt counted), late (bars not loaded up to the exit date yet; no attempt counted), failed.
    The window starts at the last close on or before the call's date; a call made during that session therefore starts from the same day's close, which is after the call
    (a half-day bias on the 24 h horizons that the aligned window, EXCESS_ALIGNED_WINDOWS, removes)."""
    out = {"selected": 0, "graded": 0, "no_bars": 0, "late": 0, "failed": 0}
    rows = await db.query(_PENDING_SQL, MAX_ATTEMPTS, limit) or []
    out["selected"] = len(rows)
    if not rows:
        return out
    windows: List[Tuple[Any, date, date]] = []
    for r in rows:
        try:
            windows.append((r, *call_window(r["occurred_at"], int(r["horizon_hours"]))))
        except Exception as e:
            swallowed("agents.excess_grading.window", e, logger)
            out["failed"] += 1
            try:
                await db.execute(_ATTEMPT_SQL, r["id"])
            except Exception as e2:
                swallowed("agents.excess_grading.attempt", e2, logger)
    if not windows:
        return out
    tickers = {"SPY"}
    for r, _, _ in windows:
        tickers.add(str(r["ticker"]))
        etf = sector_etf_for_sic(r["sic"])
        if etf:
            tickers.add(etf)
    first = min(s for _, s, _ in windows) - timedelta(days=LOOKBACK_PAD_DAYS)
    last = max(e for _, _, e in windows) + timedelta(days=LOOKBACK_PAD_DAYS)
    bars: Dict[str, Tuple[List[date], List[float]]] = defaultdict(lambda: ([], []))
    for b in await db.query(_BARS_SQL, sorted(tickers), first, last) or []:
        d, c = bars[str(b["ticker"]).upper()]
        d.append(b["day"])
        c.append(float(b["close"]))
    spy_days = bars["SPY"][0] if "SPY" in bars else []
    for r, start, end in windows:
        try:
            if direction_sign(r["direction"]) is None:
                continue
            if aligned_windows_enabled() and int(r["horizon_hours"]) >= ALIGNED_MIN_HORIZON_HOURS:
                # Session-aligned: a Friday 24 h call is no longer graded as exactly zero, a pre-open call starts from the last completed close.
                placed = aligned_window(r["occurred_at"], int(r["horizon_hours"]), spy_days)
                if placed is None:
                    if bars_late(spy_days, end):
                        out["late"] += 1
                        continue
                    await db.execute(_ATTEMPT_SQL, r["id"])
                    out["no_bars"] += 1
                    continue
                start, end = placed
            sym = str(r["ticker"])
            etf = sector_etf_for_sic(r["sic"])

            def ret(t: str):
                d, c = bars.get(t, ([], []))
                return window_return(d, c, start, end) if d else None

            stock, spy = ret(sym), ret("SPY")
            sector = ret(etf) if etf else None
            if stock is None or spy is None:
                # Late bars are not missing bars: while the loaded bars stop short of the exit date a top-up outage would otherwise park the row for good (review F13).
                if bars_late(spy_days, end):
                    out["late"] += 1
                    continue
                await db.execute(_ATTEMPT_SQL, r["id"])
                out["no_bars"] += 1
                continue
            x = excess_returns(stock, spy, sector)
            await db.execute(_GRADED_SQL, r["id"], x["stock_return"], x["spy_return"], etf if sector is not None else None,
                             x["sector_return"], x["excess_spy"], x["excess_sector"])
            out["graded"] += 1
        except Exception as e:
            swallowed("agents.excess_grading.row", e, logger)
            out["failed"] += 1
            try:
                await db.execute(_ATTEMPT_SQL, r["id"])
            except Exception as e2:
                swallowed("agents.excess_grading.attempt", e2, logger)
    return out


async def grade_beta_pending(db: Any, limit: int = 500) -> Dict[str, int]:
    """Adds the beta-adjusted excess to rows already graded against SPY. Beta is the stock's 126-session beta to SPY before the call; a row whose stock has
    too little history is parked after MAX_ATTEMPTS. Counts: selected, graded, no_beta (attempt counted), failed."""
    out = {"selected": 0, "graded": 0, "no_beta": 0, "failed": 0}
    rows = await db.query(_BETA_PENDING_SQL, MAX_ATTEMPTS, limit) or []
    out["selected"] = len(rows)
    if not rows:
        return out
    starts: Dict[Any, date] = {}
    for r in rows:
        try:
            starts[r["id"]] = call_window(r["occurred_at"], 0)[0]
        except Exception as e:
            swallowed("agents.excess_grading.beta_window", e, logger)
    if not starts:
        return out
    tickers = sorted({str(r["ticker"]) for r in rows if r["id"] in starts} | {"SPY"})
    first = min(starts.values()) - timedelta(days=int(BETA_SESSIONS * 1.6) + LOOKBACK_PAD_DAYS)
    last = max(starts.values())
    bars: Dict[str, Tuple[List[date], List[float]]] = defaultdict(lambda: ([], []))
    for i in range(0, len(tickers), 200):
        for b in await db.query(_BETA_BARS_SQL, tickers[i:i + 200], first, last) or []:
            d, c = bars[str(b["ticker"]).upper()]
            d.append(b["day"] if isinstance(b["day"], date) else date.fromisoformat(str(b["day"])[:10]))
            c.append(float(b["close"]))
    spy_d, spy_c = bars.get("SPY", ([], []))
    for r in rows:
        if r["id"] not in starts:
            continue
        try:
            d, c = bars.get(str(r["ticker"]), ([], []))
            beta = beta_to_spy(d, c, spy_d, spy_c, starts[r["id"]]) if d and spy_d else None
            x = beta_excess(float(r["stock_return"]), float(r["spy_return"]), beta)
            if x is None:
                await db.execute(_BETA_ATTEMPT_SQL, r["id"])
                out["no_beta"] += 1
                continue
            await db.execute(_BETA_SET_SQL, r["id"], beta, x)
            out["graded"] += 1
        except Exception as e:
            swallowed("agents.excess_grading.beta_row", e, logger)
            out["failed"] += 1
            try:
                await db.execute(_BETA_ATTEMPT_SQL, r["id"])
            except Exception as e2:
                swallowed("agents.excess_grading.beta_attempt", e2, logger)
    return out

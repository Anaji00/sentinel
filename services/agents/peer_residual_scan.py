"""
services/agents/peer_residual_scan.py

The registered multi-horizon rule P1 (shared/utils/peer_residual.py; reports/Hardening audit - twelve-layer brief.md) run
live inside the divergence watch, for the two horizons that passed its rate test: the session so far and five sessions.
(15 minutes and 1 hour failed it and are not scanned.)

Session so far, in two stages so the database is asked about few names:
  stage one, prices only: every stock whose group moved the other way for at least BREADTH_MIN of its members, with a
    daily beta and a loose residual z (a stock's partial-day residual is usually smaller than a full day's, so the
    registered bar is checked only in stage two);
  stage two, for at most MAX_CANDIDATES of them: the same clock time on the previous 20 sessions, giving the residual's
    sigma, the stock's usual move and the usual volume so far, then the registered gates (z 2.5, breadth 70%, move 2% or
    1.5 ATR, relative volume 1.25). A name with too little history, or no volume figure, does not pass.
Five sessions, from the daily arrays the watch already holds: the last four completed sessions plus today so far.

No model is called. Regular session only for the session horizon (09:45 to 16:00 New York): the registered test did not
cover pre-market, where the watch's own D2 rule still runs.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from shared.utils import divergence as dv
from shared.utils import peer_residual as pr
from shared.utils.quiet_failures import dropped

logger = logging.getLogger("agent.peer_residual_scan")

MAX_CANDIDATES = 40
MAX_MEMBERS = 12
PRE_Z = 1.0                 # stage one: loose residual z against the full-day sigma
PRE_MOVE = 0.005
PRE_REF_MOVE = 0.003
HISTORY_DAYS = 20
SCAN_FROM = dtime(9, 45)
SCAN_TO = dtime(16, 0)
OPEN = dtime(9, 30)

SLOT_SQL = """
    WITH days AS (
        SELECT d::date AS d FROM generate_series($2::date - 40, $2::date - 1, interval '1 day') g(d)
        WHERE extract(isodow FROM d) < 6
    )
    SELECT t.ticker, days.d,
        (SELECT b.close FROM tradfi_bars_5m b WHERE b.ticker = t.ticker AND b.close > 0
            AND b.bucket_time <= (days.d + $3::time) AT TIME ZONE 'America/New_York' - interval '5 minutes'
            AND b.bucket_time >= (days.d + $3::time) AT TIME ZONE 'America/New_York' - interval '25 minutes'
         ORDER BY b.bucket_time DESC LIMIT 1) AS px,
        (SELECT sum(b.volume) FROM tradfi_bars_5m b WHERE b.ticker = t.ticker
            AND b.bucket_time >= (days.d + time '09:30') AT TIME ZONE 'America/New_York'
            AND b.bucket_time <= (days.d + $3::time) AT TIME ZONE 'America/New_York' - interval '5 minutes') AS cumvol
    FROM unnest($1::text[]) AS t(ticker) CROSS JOIN days
"""
# The earlier sessions come from the 5-minute aggregate (150 ms for 11 names against 9.7 s on the raw minute bars,
# which sit in compressed chunks): the bars that ended at or before the clock time $3. Today's volume is summed on the
# same 5-minute boundary so the two are like for like.
CLOSE_SQL = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2::date AND close > 0"
TODAY_VOL_SQL = """
    SELECT ticker, sum(volume) AS v FROM tradfi_bars
    WHERE ticker = ANY($1::text[]) AND time >= ($2::date + time '09:30') AT TIME ZONE 'America/New_York'
      AND time < ($2::date + $3::time) AT TIME ZONE 'America/New_York' GROUP BY ticker
"""


def in_session(now_ny: datetime) -> bool:
    return now_ny.weekday() < 5 and SCAN_FROM <= now_ny.time() < SCAN_TO


def stage_one(watch: Any, ret: Mapping[str, float], news: frozenset) -> List[Dict[str, Any]]:
    """Session-so-far candidates from prices alone. `watch` is the DivergenceWatch holding the daily arrays."""
    out: List[Dict[str, Any]] = []
    if watch._rets is None:
        return out
    for t, r in ret.items():
        if abs(r) < PRE_MOVE or t not in watch._col:
            continue
        own = watch._rets[:, watch._col[t]]
        for name, members in watch._refs.get(t, {}).items():
            mem = [m for m in members if m in ret][:MAX_MEMBERS]
            ref_now = dv.mean_return([ret[m] for m in mem])
            if ref_now is None or abs(ref_now) < PRE_REF_MOVE:
                continue
            breadth, n = pr.breadth_opposite(r, [ret[m] for m in mem])
            if breadth is None or breadth < pr.BREADTH_MIN:
                continue
            ref_mean, _ = watch._ref_history(mem, t)
            b = pr.beta(own, ref_mean)
            if b is None:
                continue
            ok = np.isfinite(own) & np.isfinite(ref_mean)
            resid = own[ok] - b * ref_mean[ok]
            sd = float(np.std(resid, ddof=1)) if resid.size > 2 else 0.0
            if sd <= 0:
                continue
            zd = (r - b * ref_now) / sd
            if abs(zd) >= PRE_Z:
                out.append({"ticker": t, "reference": name, "members": mem, "r": float(r), "ref": float(ref_now),
                            "beta": float(b), "breadth": float(breadth), "zd": float(zd), "news": t in news})
    out.sort(key=lambda c: -abs(c["zd"]))
    if len(out) > MAX_CANDIDATES:
        dropped("agents.peer_residual_scan.candidate_cap", f"{len(out)} candidates, {MAX_CANDIDATES} examined", logger)
        out = out[:MAX_CANDIDATES]
    return out


def _by_ticker_day(rows: Sequence[Mapping[str, Any]], field: str) -> Dict[Tuple[str, date], float]:
    out: Dict[Tuple[str, date], float] = {}
    for r in rows:
        v = r.get(field)
        if v is not None:
            try:
                out[(str(r["ticker"]), r["d"])] = float(v)
            except (TypeError, ValueError):
                continue
    return out


def confirm_session(cands: Sequence[Mapping[str, Any]], slot_rows: Sequence[Mapping[str, Any]],
                    close_rows: Sequence[Mapping[str, Any]], today_vol: Mapping[str, float]) -> List[dv.PeerDivergence]:
    """Stage two, pure: the registered gates with the residual statistics taken at this clock time on earlier sessions."""
    px = _by_ticker_day(slot_rows, "px")
    cv = _by_ticker_day(slot_rows, "cumvol")
    closes: Dict[str, List[Tuple[date, float]]] = {}
    for r in close_rows:
        closes.setdefault(str(r["ticker"]), []).append((r["day"], float(r["close"])))
    for v in closes.values():
        v.sort()

    def prev_close(t: str, d: date) -> Optional[float]:
        prior = [c for dd, c in closes.get(t, []) if dd < d]
        return prior[-1] if prior else None

    def ret_on(t: str, d: date) -> Optional[float]:
        p, pc = px.get((t, d)), prev_close(t, d)
        return p / pc - 1.0 if p and pc else None

    out: List[dv.PeerDivergence] = []
    for c in cands:
        t, mem = c["ticker"], c["members"]
        days = sorted({d for (tt, d) in px if tt == t})[-HISTORY_DAYS:]
        y = np.array([ret_on(t, d) if ret_on(t, d) is not None else np.nan for d in days], dtype=float)
        x = np.array([dv.mean_return([ret_on(m, d) for m in mem]) if dv.mean_return([ret_on(m, d) for m in mem]) is not None
                      else np.nan for d in days], dtype=float)
        stats = pr.window_stats(y, x, c["beta"])
        vols = [cv[(t, d)] for d in days if (t, d) in cv and cv[(t, d)] > 0]
        if stats is None or len(vols) < 10 or not today_vol.get(t):
            continue                                  # no history or no volume figure: not a pass
        sigma, atr = stats
        z = float(pr.residual_z(c["r"], c["ref"], c["beta"], sigma))
        rvol = float(today_vol[t]) / float(np.mean(vols))
        if pr.qualifies(z, c["breadth"], c["r"], atr, rvol, True, "session"):
            out.append(dv.PeerDivergence(
                ticker=t, reference=c["reference"], ret=c["r"], ref_ret=c["ref"], gap=c["r"] - c["beta"] * c["ref"], z=z,
                news=bool(c["news"]), members=tuple(mem), horizon="session", breadth=c["breadth"], beta=c["beta"], rvol=rvol))
    return out


async def session_hits(db: Any, cands: Sequence[Mapping[str, Any]], now_ny: datetime) -> List[dv.PeerDivergence]:
    if not cands:
        return []
    names = sorted({c["ticker"] for c in cands} | {m for c in cands for m in c["members"]})
    today = now_ny.date()
    tau = now_ny.time().replace(minute=now_ny.minute - now_ny.minute % 5, second=0, microsecond=0)    # last full 5-minute bar
    slot_rows = await db.query(SLOT_SQL, names, today, tau) or []
    close_rows = await db.query(CLOSE_SQL, names, today - timedelta(days=60)) or []
    vol_rows = await db.query(TODAY_VOL_SQL, sorted({c["ticker"] for c in cands}), today, tau) or []
    today_vol = {str(r["ticker"]): float(r["v"] or 0.0) for r in vol_rows}
    return confirm_session(cands, slot_rows, close_rows, today_vol)


def five_day(watch: Any, ret: Mapping[str, float], news: frozenset) -> List[dv.PeerDivergence]:
    """The 5-session horizon: the last four completed sessions plus today so far, against the group's, beta-adjusted."""
    out: List[dv.PeerDivergence] = []
    if watch._rets is None or watch._rets.shape[0] < 40:
        return out
    block = watch._rets
    with np.errstate(all="ignore"):
        logs = np.log1p(block)
        windows = np.lib.stride_tricks.sliding_window_view(logs, 5, axis=0).sum(axis=-1)
    r5_all = np.expm1(windows)                                  # (T-4, tickers): trailing 5-session returns
    for t, r_today in ret.items():
        if t not in watch._col:
            continue
        j = watch._col[t]
        own_tail = block[-4:, j]
        r5 = pr.compound(list(own_tail) + [r_today])
        if r5 is None or abs(r5) < 0.01:
            continue
        for name, members in watch._refs.get(t, {}).items():
            mem = [m for m in members if m in ret and m in watch._col][:MAX_MEMBERS]
            m_now = [pr.compound(list(block[-4:, watch._col[m]]) + [ret[m]]) for m in mem]
            ref_now = dv.mean_return(m_now)
            breadth, n = pr.breadth_opposite(r5, m_now)
            if ref_now is None or breadth is None or breadth < pr.BREADTH_MIN:
                continue
            ref_mean, _ = watch._ref_history(mem, t)
            b = pr.beta(block[:, j], ref_mean)
            if b is None:
                continue
            idx = [watch._col[m] for m in mem]
            with np.errstate(all="ignore"):
                ref_hist = np.where(np.isfinite(r5_all[:, idx]).sum(axis=1) >= dv.MIN_MEMBERS, np.nanmean(r5_all[:, idx], axis=1), np.nan)
            stats = pr.window_stats(r5_all[:, j], ref_hist, b, min_days=30)
            if stats is None:
                continue
            sigma, atr = stats
            z = float(pr.residual_z(r5, ref_now, b, sigma))
            if pr.qualifies(z, breadth, r5, atr, None, False, "5d"):
                out.append(dv.PeerDivergence(
                    ticker=t, reference=name, ret=r5, ref_ret=float(ref_now), gap=r5 - b * float(ref_now), z=z, news=t in news,
                    members=tuple(mem), horizon="5d", breadth=float(breadth), beta=float(b), rvol=None))
    return out


def merge(d2: Sequence[dv.Divergence], p1: Sequence[dv.PeerDivergence]) -> List[dv.Divergence]:
    """One divergence per stock: the registered P1 reading where it fired (it carries the stricter gates), else D2's;
    where a stock fired on both horizons of P1, the larger residual."""
    best: Dict[str, dv.Divergence] = {d.ticker: d for d in d2}
    chosen: Dict[str, dv.PeerDivergence] = {}
    for d in p1:
        if d.ticker not in chosen or abs(d.z) > abs(chosen[d.ticker].z):
            chosen[d.ticker] = d
    best.update(chosen)
    return list(best.values())


__all__ = ["confirm_session", "five_day", "in_session", "merge", "session_hits", "stage_one"]

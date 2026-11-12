"""Daily-path variants of the B1 book, for the registered phase 5 test (Upgrade pre-registrations - 12 Nov). Simulation only: nothing here
touches the live trader.

Input: the out-of-sample positions the walk-forward collects (test date row, panel column, side, prediction rank, forward excess return)
and the panel's daily closes. Each test date holds its longs and shorts for `horizon` sessions, equal-weighted within a leg, as B1 does.

  V0  hold to the horizon (B1 as is).
  V1  time-decay exit at 1.5 x the signal's accrual half-life (the session by which half of the horizon's average aligned return is in).
  V2  invalidation proxy: exit when a position's cumulative aligned return falls to -3 sigma of its own daily volatility (the live veto's
      3-sigma adverse move, applied to the position instead of the peer gap).
  V3  weights from a shrinkage covariance (min-variance within each leg) instead of equal weights.
  V4  trailing stop: once up 1.5 horizon-sigma, exit if the position gives back one sigma of a quarter horizon from its peak.

An early exit leaves its capital in cash until the next test date and costs EXIT_COST_BPS. Every variant pays the same 44 bps a month on
the short book as the RB1 trial did. Pure; no I/O.
"""
from __future__ import annotations

import math
from typing import Dict, List, Sequence, Tuple

import numpy as np

SHORT_FEE_BPS = 44.0
EXIT_COST_BPS = 5.0
VOL_WINDOW = 63
Position = Tuple[int, int, int, float, float]          # (date row, panel column, side +1/-1, prediction rank, forward excess return)


def _daily_excess(close: np.ndarray, market_col: int) -> np.ndarray:
    """[T, N] daily log returns net of the market's; NaN where a price is missing."""
    with np.errstate(all="ignore"):
        r = np.diff(np.log(close), axis=0, prepend=np.nan)
    return r - r[:, [market_col]]


def accrual_half_life(paths: np.ndarray, horizon: int) -> int:
    """Session (1..horizon) by which half of the average final aligned return has accrued; `paths` is [positions, horizon] cumulative."""
    mean_path = np.nanmean(paths, axis=0)
    total = mean_path[-1]
    if not np.isfinite(total) or total <= 0:
        return horizon
    hit = np.where(mean_path >= 0.5 * total)[0]
    return int(hit[0]) + 1 if len(hit) else horizon


def _exit_day(path: np.ndarray, daily_sigma: float, horizon: int, variant: str, decay_day: int) -> int:
    """Session on which a position leaves (horizon = held to the end). `path` is the cumulative aligned return per session."""
    if variant == "V1":
        return min(horizon, decay_day)
    if variant == "V2":
        for d in range(1, horizon + 1):
            if path[d - 1] <= -3.0 * daily_sigma * math.sqrt(d):
                return d
    if variant == "V4":
        sig_h, sig_q = daily_sigma * math.sqrt(horizon), daily_sigma * math.sqrt(max(1, horizon // 4))
        peak, armed = -np.inf, False
        for d in range(1, horizon + 1):
            peak = max(peak, path[d - 1])
            armed = armed or peak >= 1.5 * sig_h
            if armed and path[d - 1] <= peak - sig_q:
                return d
    return horizon


def shrinkage_min_variance(returns: np.ndarray, shrink: float = 0.5) -> np.ndarray:
    """Long-only weights summing to 1 from the minimum-variance solution of a covariance shrunk toward constant correlation;
    equal weights when the matrix is unusable. `returns` is [obs, names]."""
    n = returns.shape[1]
    eq = np.full(n, 1.0 / n)
    if n < 2 or returns.shape[0] < 20:
        return eq
    x = returns - np.nanmean(returns, axis=0)
    x = np.nan_to_num(x)
    cov = x.T @ x / max(1, x.shape[0] - 1)
    sd = np.sqrt(np.clip(np.diag(cov), 1e-12, None))
    corr = cov / np.outer(sd, sd)
    rho = (corr.sum() - n) / (n * (n - 1))
    target = np.full((n, n), rho)
    np.fill_diagonal(target, 1.0)
    shrunk = (1 - shrink) * corr + shrink * target
    cov_s = shrunk * np.outer(sd, sd)
    try:
        w = np.linalg.solve(cov_s + 1e-8 * np.eye(n), np.ones(n))
    except np.linalg.LinAlgError:
        return eq
    w = np.clip(w, 0.0, None)
    return w / w.sum() if w.sum() > 0 else eq


def simulate(positions: Sequence[Position], close: np.ndarray, market_col: int, horizon: int,
             variants: Sequence[str] = ("V0", "V1", "V2", "V3", "V4")) -> Dict[str, List[float]]:
    """Per test date, each variant's net return (long leg minus short leg, in log excess terms) after the short fee and exit costs."""
    de = _daily_excess(close, market_col)
    T = close.shape[0]
    by_row: Dict[int, List[Position]] = {}
    for p in positions:
        by_row.setdefault(p[0], []).append(p)
    # aligned cumulative paths for every position, for the accrual half-life
    paths_all = []
    for row, plist in by_row.items():
        if row + horizon >= T:
            continue
        for _, col, side, _, _ in plist:
            seg = de[row + 1: row + 1 + horizon, col] * side
            if np.isfinite(seg).all():
                paths_all.append(np.cumsum(seg))
    decay_day = int(math.ceil(1.5 * accrual_half_life(np.array(paths_all), horizon))) if paths_all else horizon
    out: Dict[str, List[float]] = {v: [] for v in variants}
    for row in sorted(by_row):
        if row + horizon >= T or row < VOL_WINDOW:
            continue
        legs = {1: [], -1: []}
        for _, col, side, _, _ in by_row[row]:
            seg = de[row + 1: row + 1 + horizon, col]
            vol = np.nanstd(de[row - VOL_WINDOW + 1: row + 1, col])
            if np.isfinite(seg).all() and np.isfinite(vol) and vol > 0:
                legs[side].append((col, seg, vol))
        if not legs[1] or not legs[-1]:
            continue
        for v in variants:
            total = 0.0
            for side, items in legs.items():
                if v == "V3":
                    hist = de[row - VOL_WINDOW + 1: row + 1, [c for c, _, _ in items]]
                    weights = shrinkage_min_variance(hist)
                else:
                    weights = np.full(len(items), 1.0 / len(items))
                leg = 0.0
                for w, (col, seg, vol) in zip(weights, items):
                    path = np.cumsum(seg * side)
                    d = _exit_day(path, vol, horizon, v, decay_day)
                    leg += w * (path[d - 1] - (EXIT_COST_BPS / 1e4 if d < horizon else 0.0))
                total += leg                                          # the leg's aligned return already carries its side
            out[v].append(total / 2.0 - SHORT_FEE_BPS / 1e4 / 2.0)    # half the capital on each leg; fee on the short half
    return out


def sharpe(period_returns: Sequence[float], horizon: int) -> float:
    r = np.asarray(period_returns, dtype=float)
    if len(r) < 3 or np.std(r) == 0:
        return 0.0
    return float(np.mean(r) / np.std(r, ddof=1) * math.sqrt(252.0 / horizon))


__all__ = ["EXIT_COST_BPS", "SHORT_FEE_BPS", "accrual_half_life", "shrinkage_min_variance", "sharpe", "simulate"]

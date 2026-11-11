"""
shared/utils/peer_residual.py

The multi-horizon peer-residual rule P1 (registered in reports/Hardening audit - twelve-layer brief.md).

A stock's move over a horizon, less what its reference group's move implies through the stock's own beta, in units of
that residual's usual size; it fires only when the group moved the other way, the move is large against the stock's own
normal range, and volume confirms. Pure numpy: arrays in, arrays out, so the same rule runs over a backtest and live.
"""
from __future__ import annotations

from typing import Optional, Sequence, Tuple

import numpy as np

HORIZONS = ("15m", "1h", "session", "5d")
Z_MIN = 2.5
# P2 (reports/Hardening audit - twelve-layer brief.md): the short horizons are looked at 26 and 23 times a day, so they need a higher bar.
Z_MIN_BY_HORIZON = {"15m": 4.0, "1h": 3.5, "session": 2.5, "5d": 2.5}
BREADTH_MIN = 0.70
MOVE_MIN = 0.02
ATR_MULT = 1.5
RVOL_MIN = 1.25
BETA_SESSIONS = 60
SIGMA_SESSIONS_INTRADAY = 20


def beta(stock_daily: Sequence[float], ref_daily: Sequence[float]) -> Optional[float]:
    """OLS slope of the stock's daily returns on the reference's; None with fewer than 30 paired finite sessions or a flat reference."""
    y, x = np.asarray(stock_daily, dtype=float), np.asarray(ref_daily, dtype=float)
    ok = np.isfinite(y) & np.isfinite(x)
    if ok.sum() < 30:
        return None
    y, x = y[ok], x[ok]
    var = float(np.var(x))
    if var <= 0:
        return None
    return float(np.mean((x - x.mean()) * (y - y.mean())) / var)


def compound(returns: Sequence[Optional[float]]) -> Optional[float]:
    """The total return of a run of simple returns; None if any is missing or not finite."""
    total = 1.0
    for r in returns:
        if r is None or not np.isfinite(float(r)):
            return None
        total *= 1.0 + float(r)
    return total - 1.0


def window_stats(stock_rets: Sequence[float], ref_rets: Sequence[float], b: float,
                 min_days: int = 15) -> Optional[Tuple[float, float]]:
    """(sigma of the residual, mean absolute stock return) over aligned past windows, or None when too few usable days or flat.

    The live form of the registered rule's time-of-day statistics: the windows are the same clock time on earlier sessions."""
    y, x = np.asarray(stock_rets, dtype=float), np.asarray(ref_rets, dtype=float)
    ok = np.isfinite(y) & np.isfinite(x)
    if ok.sum() < min_days:
        return None
    resid = y[ok] - b * x[ok]
    sigma = float(np.std(resid, ddof=1))
    if not np.isfinite(sigma) or sigma <= 0:
        return None
    return sigma, float(np.mean(np.abs(y[ok])))


def residual_z(r_stock, r_ref, b, sigma):
    """(r_stock - b * r_ref) / sigma, elementwise; NaN where sigma is not positive or an input is missing."""
    r_stock, r_ref, sigma = (np.asarray(a, dtype=float) for a in (r_stock, r_ref, sigma))
    with np.errstate(all="ignore"):
        z = (r_stock - b * r_ref) / sigma
    return np.where(np.isfinite(z) & (sigma > 0), z, np.nan)


def breadth_opposite(r_stock: float, member_returns: Sequence[Optional[float]]) -> Tuple[Optional[float], int]:
    """(share of members with a return that moved opposite to the stock, how many had a return)."""
    xs = [float(m) for m in member_returns if m is not None and np.isfinite(float(m))]
    if not xs or not np.isfinite(r_stock) or r_stock == 0:
        return None, len(xs)
    return sum(1 for m in xs if (m > 0) != (r_stock > 0) and m != 0) / len(xs), len(xs)


def qualifies_array(z, breadth, r_stock, atr, rvol, use_rvol=True, z_min=Z_MIN):
    """Elementwise P1/P2 decision. NaN inputs never qualify."""
    z, breadth, r_stock, atr, rvol = (np.asarray(a, dtype=float) for a in (z, breadth, r_stock, atr, rvol))
    with np.errstate(invalid="ignore"):
        big = (np.abs(r_stock) >= MOVE_MIN) | (np.abs(r_stock) >= ATR_MULT * atr)
        ok = (np.abs(z) >= z_min) & (breadth >= BREADTH_MIN) & big
        if use_rvol:
            ok &= rvol >= RVOL_MIN
    return ok


def qualifies(z: float, breadth: Optional[float], r_stock: float, atr: float, rvol: Optional[float], use_rvol: bool = True,
              horizon: Optional[str] = None) -> bool:
    return bool(qualifies_array(z, np.nan if breadth is None else breadth, r_stock, atr,
                                np.nan if rvol is None else rvol, use_rvol, Z_MIN_BY_HORIZON.get(horizon or "", Z_MIN)))


__all__ = ["BREADTH_MIN", "HORIZONS", "Z_MIN", "Z_MIN_BY_HORIZON", "beta", "breadth_opposite", "qualifies", "qualifies_array", "residual_z"]

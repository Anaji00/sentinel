"""
shared/utils/gex.py

Dealer gamma exposure from a public option chain: how many dollars of the underlying market makers would have to
buy or sell for a 1% move, and the price at which that sign changes (the gamma flip).

The convention is the common naive one, and it is an assumption, not a measurement: customers are net long calls and
net short puts, so dealers hold the other side. Calls then add positive gamma and puts negative. Open interest says
how many contracts exist, not who holds them, so the sign can be wrong for a given name on a given day. It is
recorded here to build a history that a test can judge (reports/Hardening audit - phases 2 and 3.md); nothing in the
live regime classifier reads it yet.

Pure: numbers in, numbers out; None where the inputs cannot support a figure.
"""
from __future__ import annotations

import math
from typing import Iterable, List, Optional, Sequence, Tuple

CONTRACT_SIZE = 100
MIN_CONTRACTS = 20          # fewer usable contracts than this is not a chain
Row = Tuple[str, float, float, Optional[float], Optional[float]]     # right "C"/"P", strike, open interest, iv, years to expiry


def bs_gamma(s: float, k: float, t: float, sigma: float, r: float = 0.0) -> Optional[float]:
    """Black-Scholes gamma per share, or None when an input is unusable."""
    if not (s > 0 and k > 0 and t > 0 and sigma > 0) or not all(math.isfinite(x) for x in (s, k, t, sigma)):
        return None
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
    return math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi) / (s * sigma * math.sqrt(t))


def _sign(right: str) -> float:
    return 1.0 if str(right).upper().startswith("C") else -1.0


def net_gex(spot: float, rows: Iterable[Tuple[str, float, Optional[float]]]) -> Optional[float]:
    """Dollars of delta per 1% move, from (right, open interest, reported gamma) rows at the current spot.

    None when the spot is unusable or fewer than MIN_CONTRACTS rows carry both open interest and gamma."""
    if not (spot and math.isfinite(spot) and spot > 0):
        return None
    total, used = 0.0, 0
    for right, oi, gamma in rows:
        if gamma is None or oi is None or not (math.isfinite(gamma) and math.isfinite(oi)) or oi <= 0 or gamma < 0:
            continue
        total += _sign(right) * gamma * oi * CONTRACT_SIZE * spot * spot * 0.01
        used += 1
    return total if used >= MIN_CONTRACTS else None


def gamma_flip(spot: float, rows: Sequence[Row], span: float = 0.2, steps: int = 81) -> Optional[float]:
    """The spot level, within +-span of today's, where net GEX changes sign; the nearest crossing to spot.

    Gamma is recomputed at each trial level from each contract's own implied volatility and time to expiry.
    None when the chain gives no crossing in the range or too few usable contracts."""
    usable: List[Row] = [r for r in rows if r[3] and r[4] and r[2] and r[2] > 0 and r[4] > 0 and r[3] > 0]
    if not (spot and spot > 0) or len(usable) < MIN_CONTRACTS or steps < 3:
        return None

    def total(level: float) -> float:
        g = 0.0
        for right, strike, oi, iv, t in usable:
            gm = bs_gamma(level, strike, t, iv)
            if gm is not None:
                g += _sign(right) * gm * oi * CONTRACT_SIZE * level * level * 0.01
        return g

    levels = [spot * (1.0 - span + 2.0 * span * i / (steps - 1)) for i in range(steps)]
    values = [total(x) for x in levels]
    crossings: List[float] = []
    for (x0, v0), (x1, v1) in zip(zip(levels, values), zip(levels[1:], values[1:])):
        if v0 == 0.0:
            crossings.append(x0)
        elif v0 * v1 < 0:
            crossings.append(x0 + (x1 - x0) * (0.0 - v0) / (v1 - v0))
    return min(crossings, key=lambda x: abs(x - spot)) if crossings else None


def regime(net: Optional[float], spot: Optional[float], flip: Optional[float]) -> Optional[str]:
    """'positive' (dealers damp moves), 'negative' (they add to them) or None when it cannot be said."""
    if net is None:
        return None
    if flip is not None and spot:
        return "positive" if spot > flip else "negative"
    return "positive" if net > 0 else "negative"


__all__ = ["CONTRACT_SIZE", "MIN_CONTRACTS", "bs_gamma", "gamma_flip", "net_gex", "regime"]

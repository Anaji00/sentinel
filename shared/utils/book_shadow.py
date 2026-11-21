"""The S5 shadow book: what B1's weights would be under conviction weighting, recorded beside the real equal weights, never traded.

Registered 13 Nov 2026 (reports/Upgrade pre-registrations - 12 Nov.md, 'Conviction weights (S5), one forward test'): same names as B1, same slope and bounds as
the book trial (shared/utils/book_strategies.conviction_weights), judged only against the registered bar on data after 12 Nov. This module is the pure part: it
turns B1's stored book and the monthly percentiles into two weight sets (S0 equal, S5 conviction) and marks a held set of weights against daily closes. It places no
orders and changes no sizing; the live trader never reads it. Pure; no I/O.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from shared.utils import auto_trade as at
from shared.utils.book_strategies import conviction_weights

VARIANTS = ("S0", "S5")      # the two weight sets a rebalance records
LEG = 0.5      # each leg's share of the account, as the trader's target_weights(leg=0.5)


def shadow_weights(longs: Sequence[str], shorts: Sequence[str], pct: Mapping[str, float]) -> Dict[str, Dict[str, float]]:
    """{"S0": signed weights (equal within each leg), "S5": conviction weights within each leg}. Names with no finite percentile get the leg's average
    conviction (extremity of its leg's mean), so a missing score never removes a held name or adds a made-up one."""
    out: Dict[str, Dict[str, float]] = {"S0": at.target_weights(longs, shorts, LEG)}

    def leg_conviction(names: Sequence[str], side: int) -> Dict[str, float]:
        names = [str(t).upper() for t in names]
        raw = []
        for t in names:
            p = pct.get(t)
            raw.append((p if side > 0 else 1.0 - p) if p is not None and math.isfinite(float(p)) else float("nan"))
        arr = np.array(raw, dtype=float)
        if arr.size and np.isnan(arr).any():
            arr = np.where(np.isnan(arr), np.nanmean(arr) if not np.isnan(arr).all() else 0.0, arr)
        w = conviction_weights(arr) if arr.size else arr
        return {t: float(x) for t, x in zip(names, w)}

    weights = {**leg_conviction(longs, +1), **leg_conviction(shorts, -1)}
    out["S5"] = at.target_weights(longs, shorts, LEG, weights=weights)
    return out


def hold_return(weights: Mapping[str, float], closes: Mapping[str, Sequence[Tuple[date, float]]], start: date, end: date) -> Optional[float]:
    """Return of holding `weights` (signed share of the account per name, buy and hold, no rebalancing inside) from the close on or before `start` to the close on or
    before `end`. A name with no close at either end contributes nothing (not zero return invented: its weight is dropped and the others are not rescaled);
    None when no name could be priced."""
    total = 0.0
    priced = 0
    for t, w in weights.items():
        series = closes.get(t) or []
        a = _close_on_or_before(series, start)
        b = _close_on_or_before(series, end)
        if a is None or b is None or a <= 0:
            continue
        total += w * (b / a - 1.0)
        priced += 1
    return total if priced else None


def _close_on_or_before(series: Sequence[Tuple[date, float]], day: date) -> Optional[float]:
    best: Optional[float] = None
    for d, c in series:
        if d <= day:
            best = c
        else:
            break
    return best


def paired_blocks(s0: Sequence[float], s5: Sequence[float]) -> List[float]:
    """Per-block return difference S5 minus S0 (equal-length lists; the shorter decides)."""
    return [float(b) - float(a) for a, b in zip(s0, s5)]


__all__ = ["LEG", "VARIANTS", "hold_return", "paired_blocks", "shadow_weights"]

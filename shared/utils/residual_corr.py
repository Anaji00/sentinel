"""Market-neutral correlation between stocks on daily returns, with false-discovery control over all pairs tested.

At a one-day horizon the market factor swamps pairwise relationships, so raw correlations say little about two companies and survive
multiple testing only because both move with the market. This removes each stock's market component (a regression on the market return),
correlates the residuals, converts to p-values, and applies Benjamini-Hochberg across every pair. Shadow tool: nothing here publishes.

Pure; no I/O. Sources: factor-adjusted multiple testing of correlations (Du, Lan, Luo, Zhong 2018); dependence under FDR control (arXiv 2102.07826).
"""
from __future__ import annotations

import math
from typing import Dict, List, Sequence, Tuple

import numpy as np

MIN_OBS = 120


def daily_returns(closes: Sequence[float]) -> np.ndarray:
    c = np.asarray(closes, dtype=float)
    return c[1:] / c[:-1] - 1.0 if len(c) > 1 else np.array([])


def residuals(stock: np.ndarray, market: np.ndarray) -> np.ndarray:
    """Stock returns minus beta x market returns (intercept included); aligned arrays of equal length."""
    x = np.column_stack([np.ones(len(market)), market])
    coef, *_ = np.linalg.lstsq(x, stock, rcond=None)
    return stock - x @ coef


def _t_to_p(t: float, dof: int) -> float:
    """Two-sided p-value of a t statistic; normal approximation above 30 degrees of freedom (the case here)."""
    return math.erfc(abs(t) / math.sqrt(2.0)) if dof > 30 else 1.0


def corr_p(a: np.ndarray, b: np.ndarray) -> Tuple[float, float]:
    """(Pearson r, two-sided p) on two aligned series; (0, 1) when there is too little or no variation."""
    n = len(a)
    if n < MIN_OBS or np.std(a) == 0 or np.std(b) == 0:
        return 0.0, 1.0
    r = float(np.corrcoef(a, b)[0, 1])
    t = r * math.sqrt((n - 2) / max(1e-12, 1.0 - r * r))
    return r, _t_to_p(t, n - 2)


def benjamini_hochberg(pvalues: Sequence[float], q: float = 0.05) -> List[bool]:
    """Which hypotheses are rejected at false-discovery rate q, in input order."""
    m = len(pvalues)
    order = sorted(range(m), key=lambda i: pvalues[i])
    cutoff = -1
    for rank, i in enumerate(order, start=1):
        if pvalues[i] <= q * rank / m:
            cutoff = rank
    keep = [False] * m
    for rank, i in enumerate(order, start=1):
        if rank <= cutoff:
            keep[i] = True
    return keep


def _neutral(closes: Dict[str, Sequence[float]], market: Sequence[float], q: float = 0.05) -> Dict[str, object]:
    """Test every pair of `closes` (equal-length daily close series ending the same day) after removing the market component.

    Returns {"tested": n_pairs, "raw_significant": pairs significant at q before removing the market, "neutral_significant": pairs that
    survive after, "pairs": [(a, b, residual r, p)] for the survivors, by |r|}."""
    mkt = daily_returns(market)
    rets = {t: daily_returns(c) for t, c in closes.items()}
    names = sorted(t for t, r in rets.items() if len(r) == len(mkt) and len(r) >= MIN_OBS)
    res = {t: residuals(rets[t], mkt) for t in names}
    raw_p, neu_p, keys, rvals = [], [], [], []
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            _, p_raw = corr_p(rets[a], rets[b])
            r_n, p_n = corr_p(res[a], res[b])
            raw_p.append(p_raw)
            neu_p.append(p_n)
            keys.append((a, b))
            rvals.append(r_n)
    keep_raw = benjamini_hochberg(raw_p, q) if raw_p else []
    keep_neu = benjamini_hochberg(neu_p, q) if neu_p else []
    survivors = sorted(((keys[i][0], keys[i][1], round(rvals[i], 3), neu_p[i]) for i, k in enumerate(keep_neu) if k), key=lambda x: -abs(x[2]))
    return {"tested": len(keys), "raw_significant": sum(keep_raw), "neutral_significant": sum(keep_neu), "pairs": survivors,
            "_residuals": res}


def market_neutral_pairs(closes: Dict[str, Sequence[float]], market: Sequence[float], q: float = 0.05) -> Dict[str, object]:
    """Public form of _neutral, without its private residual cache."""
    out = _neutral(closes, market, q)
    out.pop("_residuals", None)
    return out


def stable_pairs(closes: Dict[str, Sequence[float]], market: Sequence[float], q: float = 0.05, r_floor: float = 0.35,
                 half_floor: float = 0.25) -> Dict[str, object]:
    """Market-neutral pairs that also clear an effect-size floor and hold in both halves of the sample.

    With 250 sessions almost every pair is statistically significant, so the p-value is not selective; the floor keeps links that matter
    (|residual r| of at least `r_floor`) and the split-half check keeps ones that persisted (same sign, at least `half_floor` in each
    half). Returns the same keys as market_neutral_pairs plus "stable": [(a, b, r, r_first_half, r_second_half)] by |r|."""
    out = _neutral(closes, market, q)
    res = out.pop("_residuals")
    stable = []
    for a, b, r, _p in out["pairs"]:
        if abs(r) < r_floor:
            continue
        n = len(res[a])
        h = n // 2
        r2 = float(np.corrcoef(res[a][h:], res[b][h:])[0, 1]) if np.std(res[a][h:]) and np.std(res[b][h:]) else 0.0
        r1 = float(np.corrcoef(res[a][:h], res[b][:h])[0, 1]) if np.std(res[a][:h]) and np.std(res[b][:h]) else 0.0
        if r1 * r2 > 0 and min(abs(r1), abs(r2)) >= half_floor:
            stable.append((a, b, r, round(r1, 3), round(r2, 3)))
    out["stable"] = stable
    return out


__all__ = ["MIN_OBS", "benjamini_hochberg", "corr_p", "daily_returns", "market_neutral_pairs", "residuals", "stable_pairs"]

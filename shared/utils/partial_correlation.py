"""Direct links between stocks, with the market and everything else held fixed.

Pairwise correlation of raw returns mostly rediscovers the market: on a broad
universe the first principal component -- the market -- explains the largest
share of variance, and two unrelated names correlate because both load on it.
Pairwise discovery here also paired two separately-fetched series by position,
so for any thinly traded name it was correlating different hours.

This estimates a sparse conditional-dependence network instead:

  1. align   returns on the bars every name traded (no positional pairing)
  2. net     each name's market beta out against SPY (OLS residuals)
  3. shrink  the residual covariance with Ledoit-Wolf (Ledoit & Wolf 2004),
             which keeps the inverse well conditioned when names approach
             the number of observations
  4. invert  to the precision matrix P; the partial correlation of i and j
             given every other name is -P_ij / sqrt(P_ii * P_jj)
  5. test    each with a Fisher z on n - k - 3 degrees of freedom, where k is
             the number of names conditioned on, then Benjamini-Hochberg

Sector ETFs belong in the universe: conditioning on them removes the sector
factor the same way step 2 removes the market. What survives is the link a
supplier, competitor or substitute actually carries.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Hashable, List, Optional, Sequence, Tuple

import numpy as np

# Observations needed per conditioned name; fewer and the precision is noise.
MIN_OBS_PER_NAME = 3
MIN_OBS = 60


@dataclass
class PartialEdge:
    a: str
    b: str
    partial: float          # partial correlation given all other names
    raw: float              # raw correlation of the residual returns, for contrast
    p_value: float
    q_value: float
    n_obs: int
    n_conditioned: int


# On the reference calendar, a name may skip this many consecutive bars and be
# carried at its last price; one missing more than MAX_MISSING_SHARE of the
# calendar is dropped rather than shrinking everyone to its history.
MAX_FFILL = 2
MAX_MISSING_SHARE = 0.25


def aligned_returns(
    closes: Dict[str, Dict[Hashable, float]],
    reference: Optional[str] = None,
) -> Tuple[List[str], np.ndarray]:
    """Simple returns on a common calendar, oldest first.

    With a `reference` (the market), the calendar is the reference's bars: a
    name's short gaps are carried at its last price, and a name too sparse on
    that calendar is dropped. Without one, the calendar is the bars every name
    shares, dropping the thinnest names until it is long enough."""
    if reference and reference in closes and len(closes[reference]) >= MIN_OBS + 1:
        cal = sorted(closes[reference])
        kept: List[str] = []
        cols: List[List[float]] = []
        for n, series in closes.items():
            col: List[float] = []
            last, gap, missing, started = None, 0, 0, False
            for t in cal:
                if t in series:
                    last, gap, started = series[t], 0, True
                    col.append(last)
                else:
                    missing += 1
                    gap += 1
                    col.append(last if (started and gap <= MAX_FFILL) else float("nan"))
            if missing / len(cal) <= MAX_MISSING_SHARE:
                kept.append(n)
                cols.append(col)
        if len(kept) >= 3:
            prices = np.array(cols, dtype=np.float64).T
            rows_ok = np.all(np.isfinite(prices) & (prices > 0), axis=1)
            prices = prices[rows_ok]
            if prices.shape[0] >= MIN_OBS + 1:
                returns = prices[1:] / prices[:-1] - 1.0
                alive = returns.std(axis=0) > 1e-9
                return [n for n, k in zip(kept, alive) if k], returns[:, alive]
        return [], np.empty((0, 0))

    names = [n for n, s in closes.items() if len(s) >= MIN_OBS + 1]
    if len(names) < 3:
        return [], np.empty((0, 0))
    common = set.intersection(*(set(closes[n]) for n in names))
    # Drop the thinnest names until the shared history is long enough.
    while len(common) < MIN_OBS + 1 and len(names) > 3:
        thinnest = min(names, key=lambda n: len(closes[n]))
        names.remove(thinnest)
        common = set.intersection(*(set(closes[n]) for n in names))
    if len(common) < MIN_OBS + 1:
        return [], np.empty((0, 0))
    ts = sorted(common)
    prices = np.array([[closes[n][t] for n in names] for t in ts], dtype=np.float64)
    if np.any(prices <= 0) or not np.all(np.isfinite(prices)):
        keep = np.all(prices > 0, axis=0) & np.all(np.isfinite(prices), axis=0)
        names = [n for n, k in zip(names, keep) if k]
        prices = prices[:, keep]
    returns = prices[1:] / prices[:-1] - 1.0
    # A name that never moved has no variance to condition on.
    alive = returns.std(axis=0) > 1e-9
    return [n for n, k in zip(names, alive) if k], returns[:, alive]


def residualize(returns: np.ndarray, market: np.ndarray) -> np.ndarray:
    """Each column minus its OLS fit on the market (alpha + beta * market)."""
    X = np.column_stack([np.ones_like(market), market])
    coef, *_ = np.linalg.lstsq(X, returns, rcond=None)
    return returns - X @ coef


def ledoit_wolf_covariance(X: np.ndarray) -> np.ndarray:
    """Ledoit-Wolf shrinkage toward a scaled identity.

    scikit-learn's estimator when available (agents and correlation have it);
    the same closed form otherwise, so a container without it still runs."""
    try:
        from sklearn.covariance import LedoitWolf

        return LedoitWolf(assume_centered=False).fit(X).covariance_
    except ImportError:
        pass
    Xc = X - X.mean(axis=0)
    n, p = Xc.shape
    S = Xc.T @ Xc / n
    mu = np.trace(S) / p
    delta = np.sum((S - mu * np.eye(p)) ** 2) / p
    beta_ = 0.0
    for row in Xc:
        o = np.outer(row, row) - S
        beta_ += np.sum(o ** 2)
    beta_ = beta_ / (n * n * p)
    shrink = min(1.0, beta_ / delta) if delta > 0 else 1.0
    return shrink * mu * np.eye(p) + (1.0 - shrink) * S


def partial_correlation_matrix(X: np.ndarray) -> np.ndarray:
    cov = ledoit_wolf_covariance(X)
    prec = np.linalg.pinv(cov)
    d = np.sqrt(np.clip(np.diag(prec), 1e-18, None))
    pc = -prec / np.outer(d, d)
    np.fill_diagonal(pc, 1.0)
    return np.clip(pc, -1.0, 1.0)


def _two_sided_normal_p(z: float) -> float:
    return math.erfc(abs(z) / math.sqrt(2.0))


def _bh(p_values: Sequence[float], alpha: float) -> Tuple[List[bool], List[float]]:
    m = len(p_values)
    if m == 0:
        return [], []
    order = sorted(range(m), key=lambda i: p_values[i])
    q = [0.0] * m
    running = 1.0
    for rank in range(m, 0, -1):
        i = order[rank - 1]
        running = min(running, p_values[i] * m / rank)
        q[i] = running
    return [qi <= alpha for qi in q], q


def partial_network(
    closes: Dict[str, Dict[Hashable, float]],
    market: str = "SPY",
    alpha: float = 0.05,
    min_abs_partial: float = 0.15,
    exclude_from_edges: Optional[set] = None,
) -> Tuple[List[PartialEdge], Dict[str, int]]:
    """Significant partial-correlation edges among `closes`, net of `market`.

    `exclude_from_edges` names are conditioned on but never reported as an
    endpoint (sector ETFs: they are there to absorb the sector factor)."""
    names, R = aligned_returns(closes, reference=market)
    stats = {"names": len(names), "observations": int(R.shape[0]) if R.size else 0, "tested": 0, "edges": 0}
    if market not in names or len(names) < 4:
        return [], stats
    mi = names.index(market)
    others = [i for i in range(len(names)) if i != mi]
    resid = residualize(R[:, others], R[:, mi])
    sub = [names[i] for i in others]
    n, k_total = resid.shape
    if n < max(MIN_OBS, MIN_OBS_PER_NAME * k_total):
        return [], stats
    pc = partial_correlation_matrix(resid)
    raw = np.corrcoef(resid, rowvar=False)
    dof = n - (k_total - 2) - 3
    if dof <= 3:
        return [], stats

    excl = set(exclude_from_edges or ())
    pairs, ps = [], []
    for i in range(k_total):
        for j in range(i + 1, k_total):
            if sub[i] in excl or sub[j] in excl:
                continue
            r = float(pc[i, j])
            z = 0.5 * math.log((1 + r) / (1 - r)) * math.sqrt(dof) if abs(r) < 0.999999 else 50.0
            pairs.append((i, j, r))
            ps.append(_two_sided_normal_p(z))
    reject, qs = _bh(ps, alpha)
    stats["tested"] = len(pairs)
    edges = [
        PartialEdge(sub[i], sub[j], round(r, 4), round(float(raw[i, j]), 4),
                    round(p, 6), round(q, 6), n, k_total - 2)
        for (i, j, r), p, q, ok in zip(pairs, ps, qs, reject)
        if ok and abs(r) >= min_abs_partial
    ]
    edges.sort(key=lambda e: -abs(e.partial))
    stats["edges"] = len(edges)
    return edges, stats


__all__ = [
    "PartialEdge", "aligned_returns", "ledoit_wolf_covariance",
    "partial_correlation_matrix", "partial_network", "residualize",
]

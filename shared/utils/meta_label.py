"""
shared/utils/meta_label.py

A sizing layer over the model's picks (meta-labeling, Lopez de Prado 2018).

The primary model chooses direction: its top and bottom fifths. A secondary
classifier, trained only on earlier picks, estimates the probability that a
new pick beats its trading costs, from what was known when it was made -- how
extreme the pick was, the stock's volatility, liquidity, beta and short-term
extremes, and the market's volatility regime -- and sizes it by that
probability. It is a sizing tool, not a source of edge: with a thin primary
model (IC ~0.02) it can only help by trading less of what tends to fail.

Evaluated walk-forward: for each date, the classifier is fitted on picks whose
outcome was known (horizon plus embargo earlier), then the period's book is
returned twice -- equal weight, and weighted by size -- net of costs.

Pure: picks and features in, period returns and a comparison out.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

META_FEATURES = ("vol_63", "dvol_21", "beta_126", "max_21", "rev_5")
MIN_TRAIN = 2000              # picks with known outcomes before the layer sizes anything
REFIT_EVERY = 4               # periods between refits


@dataclass
class Pick:
    row: int
    col: int
    side: int
    pred_rank: float
    fwd: float


def design(picks: Sequence[Pick], feats: Dict[str, np.ndarray], market_var_ratio: np.ndarray) -> np.ndarray:
    """Per pick: how extreme it was, the stock's features, the market's regime."""
    cols = [[abs(p.pred_rank) for p in picks]]
    for name in META_FEATURES:
        f = feats.get(name)
        cols.append([float(f[p.row, p.col]) if f is not None else np.nan for p in picks])
    cols.append([float(market_var_ratio[p.row]) for p in picks])
    cols.append([float(p.side) for p in picks])
    X = np.array(cols, dtype=np.float64).T
    return np.where(np.isfinite(X), X, 0.0)


def market_variance_ratio(market_close: np.ndarray, recent: int = 21, long: int = 252) -> np.ndarray:
    """Per row: long-run over recent variance of daily market returns, from data up to that row."""
    r = np.diff(np.log(market_close), prepend=np.nan)
    out = np.full(len(r), 1.0)
    for i in range(long, len(r)):
        rv, lv = np.nanvar(r[i - recent + 1:i + 1]), np.nanvar(r[i - long + 1:i + 1])
        if rv > 0 and np.isfinite(lv):
            out[i] = lv / rv
    return out


def size_from_probability(p: np.ndarray) -> np.ndarray:
    """Bet size from the probability a pick pays: nothing at or below a coin flip."""
    return np.clip(2.0 * p - 1.0, 0.0, 1.0)


def entry_costs(picks: Sequence[Pick], cost: float) -> np.ndarray:
    """Round-trip cost charged when a position is opened, nothing while it is
    held: the book keeps most names for many periods (turnover ~10%)."""
    by_row: Dict[int, List[int]] = {}
    for k, p in enumerate(picks):
        by_row.setdefault(p.row, []).append(k)
    out = np.zeros(len(picks))
    held: Dict[int, set] = {1: set(), -1: set()}
    for row in sorted(by_row):
        now: Dict[int, set] = {1: set(), -1: set()}
        for k in by_row[row]:
            p = picks[k]
            now[p.side].add(p.col)
            if p.col not in held[p.side]:
                out[k] = cost
        held = now
    return out


def walk_forward(picks: Sequence[Pick], X: np.ndarray, cost: float, horizon: int, embargo: int
                 ) -> Dict[str, object]:
    """Equal-weight vs. meta-sized book, period by period, out of sample.
    `cost` is the round trip, charged on entry (entry_costs)."""
    from sklearn.linear_model import LogisticRegression

    c = entry_costs(picks, cost)
    gross = np.array([p.side * p.fwd for p in picks])
    y = (gross - cost > 0).astype(int)        # would the pick have paid a full round trip
    rows = np.array([p.row for p in picks])
    signed = gross - c
    periods = sorted(set(rows.tolist()))
    base: List[float] = []
    meta: List[float] = []
    traded: List[float] = []
    model: Optional[LogisticRegression] = None
    for k, d in enumerate(periods):
        here = rows == d
        known = rows + horizon + embargo <= d
        if known.sum() < MIN_TRAIN or len(set(y[known].tolist())) < 2:
            continue
        if model is None or k % REFIT_EVERY == 0:
            model = LogisticRegression(C=0.1, max_iter=500)
            model.fit(X[known], y[known])
        size = size_from_probability(model.predict_proba(X[here])[:, 1])
        base.append(float(np.mean(signed[here])))
        traded.append(float(size.mean()))
        meta.append(float(np.sum(size * signed[here]) / here.sum()))
    return {"periods": len(base), "base": _stats(base, horizon), "meta": _stats(meta, horizon),
            "mean_size": round(float(np.mean(traded)), 3) if traded else None}


CONFIRM_RUNS = 2                 # consecutive lab runs that must agree before sizing goes live


def fit_final(picks: Sequence[Pick], X: np.ndarray, cost: float) -> Optional[Dict[str, object]]:
    """The sizing classifier fitted on every pick in the history, for live use:
    its coefficients in `design`'s column order."""
    from sklearn.linear_model import LogisticRegression

    gross = np.array([p.side * p.fwd for p in picks])
    y = (gross - cost > 0).astype(int)
    if len(y) < MIN_TRAIN or len(set(y.tolist())) < 2:
        return None
    m = LogisticRegression(C=0.1, max_iter=500).fit(X, y)
    return {"coef": [round(float(c), 6) for c in m.coef_[0]], "intercept": round(float(m.intercept_[0]), 6),
            "columns": ["abs_pred_rank", *META_FEATURES, "market_variance_ratio", "side"], "picks": int(len(y))}


def probability(model: Optional[Dict[str, object]], x: Sequence[float]) -> Optional[float]:
    """The chance a pick pays its round trip, from fit_final's model."""
    if not model or len(x) != len(model["coef"]):
        return None
    z = float(model["intercept"]) + sum(float(c) * float(v) for c, v in zip(model["coef"], x))
    return 1.0 / (1.0 + math.exp(-max(-50.0, min(50.0, z))))


def confirmed(history: Sequence[Dict[str, object]], runs: int = CONFIRM_RUNS) -> bool:
    """Whether the last `runs` lab runs each found sizing raised the Sharpe."""
    last = list(history or [])[-runs:]
    return len(last) == runs and all(
        h.get("meta") is not None and h.get("base") is not None and float(h["meta"]) > float(h["base"]) for h in last)


def live_features(percentile: float, side: int, closes: Sequence[float], volumes: Sequence[float],
                  spy_closes: Sequence[float]) -> Optional[List[float]]:
    """`design`'s columns for one pick today, from its daily bars, as
    factor_model defines them: rank distance from the middle, 63-day volatility,
    log 21-day dollar volume, 126-day beta to SPY, the 21-day largest daily
    return, the 5-day return, SPY's long-over-recent variance, and the side."""
    c = [float(x) for x in closes]
    s = [float(x) for x in spy_closes]
    if len(c) < 127 or len(s) < 253 or len(volumes) < 21:
        return None
    r = [b / a - 1.0 for a, b in zip(c, c[1:])]
    sr = [b / a - 1.0 for a, b in zip(s, s[1:])]
    n = min(126, len(r), len(sr))
    ri, mi = r[-n:], sr[-n:]
    mm = sum(mi) / n
    var = sum((x - mm) ** 2 for x in mi) / n
    rm = sum(ri) / n
    beta = sum((a - rm) * (b - mm) for a, b in zip(ri, mi)) / n / var if var > 0 else 1.0
    r63 = r[-63:]
    m63 = sum(r63) / len(r63)
    vol63 = math.sqrt(sum((x - m63) ** 2 for x in r63) / len(r63))
    dv = sum(a * float(b) for a, b in zip(c[-21:], volumes[-21:])) / 21
    ls = [math.log(b / a) for a, b in zip(s, s[1:])]

    def nvar(xs: Sequence[float]) -> float:
        m = sum(xs) / len(xs)
        return sum((x - m) ** 2 for x in xs) / len(xs)
    recent, longrun = nvar(ls[-21:]), nvar(ls[-252:])
    return [abs(float(percentile) - 0.5), vol63, math.log(dv) if dv > 0 else 0.0, beta, max(r[-21:]),
            c[-1] / c[-6] - 1.0, longrun / recent if recent > 0 else 1.0, float(side)]


def _stats(xs: Sequence[float], horizon: int) -> Dict[str, Optional[float]]:
    if len(xs) < 2:
        return {"mean_bps": None, "sharpe": None}
    m = sum(xs) / len(xs)
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))
    return {"mean_bps": round(m * 1e4, 2),
            "sharpe": round(m / sd * math.sqrt(252 / horizon), 3) if sd > 0 else None}


def as_picks(rows: Sequence[Tuple[int, int, int, float, float]]) -> List[Pick]:
    return [Pick(*r) for r in rows if all(map(lambda v: v == v, r))]


__all__ = ["CONFIRM_RUNS", "confirmed", "fit_final", "live_features", "probability", "META_FEATURES", "Pick",
           "as_picks", "design", "entry_costs", "market_variance_ratio", "size_from_probability",
           "walk_forward"]

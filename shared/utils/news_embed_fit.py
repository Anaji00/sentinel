"""
shared/utils/news_embed_fit.py

The news read's monthly refit, on sufficient statistics: for each month of
sessions, X'X, X'y, y'y, the sum of X and the count, so a walk-forward over
three years of stock-days never holds more than one month in memory. Pure;
services/agents/news_model_refit.py streams the rows.

Folds as trial M ran them: each test month is predicted by a ridge trained on
every month before it except the one immediately before (the target spans five
sessions, so the adjacent month is embargoed). The penalty is chosen on the
last three training months by out-of-sample squared error, which the
statistics give exactly, from a grid floored at 100. Compared on 18 Oct 2026
(out-of-sample, 635 sessions): squared error with the floor at 1,000 gave rank
IC 0.0286 with a score scale of 1.62; floored at 100, IC 0.0281 (t 4.73) and a
scale of 1.05; floored at 10, IC 0.0277; choosing by pooled correlation
instead, IC 0.0243. The floor at 100 keeps the score's scale honest at almost
no cost in ranking.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional, Sequence, Tuple

import numpy as np

DIM = 768
LAMBDAS: Tuple[float, ...] = (1e2, 3e2, 1e3, 3e3, 1e4, 3e4, 1e5, 3e5, 1e6)
MIN_TRAIN_MONTHS = 6
VAL_MONTHS = 3
MIN_TRAIN_ROWS = 20_000


def unpack(b: bytes) -> np.ndarray:
    """A stored embedding sum (768 little-endian float16) as float32."""
    return np.frombuffer(b, dtype="<f2").astype(np.float32)


def pack(v: Sequence[float]) -> bytes:
    return np.asarray(v, dtype="<f2").tobytes()


@dataclass
class Stats:
    """One month's sufficient statistics for a ridge."""
    xtx: np.ndarray = field(default_factory=lambda: np.zeros((DIM, DIM)))
    xty: np.ndarray = field(default_factory=lambda: np.zeros(DIM))
    sx: np.ndarray = field(default_factory=lambda: np.zeros(DIM))
    sy: float = 0.0
    yty: float = 0.0
    n: int = 0

    def add(self, X: np.ndarray, y: np.ndarray) -> None:
        X64 = X.astype(np.float64)
        self.xtx += X64.T @ X64
        self.xty += X64.T @ y
        self.sx += X64.sum(axis=0)
        self.sy += float(y.sum())
        self.yty += float(y @ y)
        self.n += len(y)


def total(stats: Iterable[Stats]) -> Stats:
    out = Stats()
    for s in stats:
        out.xtx += s.xtx
        out.xty += s.xty
        out.sx += s.sx
        out.sy += s.sy
        out.yty += s.yty
        out.n += s.n
    return out


def solve(s: Stats, lam: float) -> np.ndarray:
    return np.linalg.solve(s.xtx + lam * np.eye(s.xtx.shape[0]), s.xty)


def sse(s: Stats, beta: np.ndarray) -> float:
    """Squared error of `beta` on the rows behind `s`."""
    return float(s.yty - 2.0 * beta @ s.xty + beta @ s.xtx @ beta)


def corr(s: Stats, beta: np.ndarray) -> float:
    """Pearson correlation of X beta with y over the rows behind `s`."""
    if s.n < 3:
        return float("nan")
    sp = float(beta @ s.sx)
    cov = float(beta @ s.xty) - sp * s.sy / s.n
    vp = float(beta @ s.xtx @ beta) - sp * sp / s.n
    vy = s.yty - s.sy * s.sy / s.n
    return cov / np.sqrt(vp * vy) if vp > 0 and vy > 0 else float("nan")


def choose_lambda(fit: Stats, val: Stats, lambdas: Sequence[float] = LAMBDAS) -> float:
    return min(lambdas, key=lambda lam: sse(val, solve(fit, lam)))


def folds(months: Sequence[str], by_month: Dict[str, Stats],
          lambdas: Sequence[float] = LAMBDAS) -> Dict[str, Tuple[np.ndarray, float]]:
    """Test month -> (beta, penalty), trained on months before it but the
    adjacent one, the penalty chosen on the last VAL_MONTHS of those."""
    out: Dict[str, Tuple[np.ndarray, float]] = {}
    for k in range(MIN_TRAIN_MONTHS + 1, len(months)):
        train = list(months[:k - 1])
        if sum(by_month[m].n for m in train) < MIN_TRAIN_ROWS or len(train) <= VAL_MONTHS:
            continue
        fit = total(by_month[m] for m in train[:-VAL_MONTHS])
        val = total(by_month[m] for m in train[-VAL_MONTHS:])
        lam = choose_lambda(fit, val, lambdas)
        out[months[k]] = (solve(total(by_month[m] for m in train), lam), lam)
    return out


def rank_ic(p: np.ndarray, y: np.ndarray) -> Optional[float]:
    if len(p) < 20:
        return None
    rp = np.argsort(np.argsort(p)).astype(float)
    ry = np.argsort(np.argsort(y)).astype(float)
    return float(np.corrcoef(rp, ry)[0, 1])


def summary(ics: Sequence[float], spreads: Sequence[float], horizon: int) -> Dict[str, float]:
    """Mean daily rank IC, its t on non-overlapping sessions, and the mean
    top-minus-bottom decile."""
    a = np.asarray(ics, dtype=float)
    step = a[::horizon] if horizon > 1 else a
    t = float(step.mean() / (step.std(ddof=1) / np.sqrt(len(step)))) if len(step) > 2 else float("nan")
    return {"sessions": int(len(a)), "mean_ic": round(float(a.mean()), 4) if len(a) else float("nan"),
            "ic_t": round(t, 2), "decile_spread_bps": round(float(np.mean(spreads)) * 1e4, 1) if spreads else 0.0}


def final(by_month: Dict[str, Stats], lams: Sequence[float]) -> Tuple[np.ndarray, float, float]:
    """The deployed fit on every month, at the penalty the folds chose most,
    and the standard deviation of its in-sample score."""
    lam = Counter(lams).most_common(1)[0][0] if lams else LAMBDAS[len(LAMBDAS) // 2]
    s = total(by_month.values())
    beta = solve(s, lam)
    mean = float(beta @ s.sx) / max(s.n, 1)
    var = float(beta @ s.xtx @ beta) / max(s.n, 1) - mean * mean
    return beta, lam, float(np.sqrt(max(var, 0.0)))


def passes(summ: Dict[str, float], min_ic: float = 0.02, min_t: float = 2.0) -> bool:
    """Trial M's bar, held on every refit."""
    return bool(summ.get("mean_ic", 0) >= min_ic and summ.get("ic_t", 0) >= min_t)


__all__ = ["corr", "DIM", "LAMBDAS", "Stats", "choose_lambda", "final", "folds", "pack", "passes", "rank_ic", "solve", "sse",
           "summary", "total", "unpack"]

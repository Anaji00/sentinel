"""
shared/utils/news_model.py

News into returns through embeddings, not generation. Pure; the correlation
service gathers the samples and scores live headlines.

Each headline is already embedded for the soft correlator (all-mpnet-base-v2,
768 dimensions). A ridge regression from that vector to the next-session
return of each instrument the headline names or maps to, net of SPY, measured
from the first close after the headline was known to the close after it, is
the cheapest version of Chen, Kelly & Xiu's finding that LLM embeddings of news
predict returns better than past-return signals. On this CPU host a ridge on
existing vectors costs milliseconds, where the 3B model's generated calls were
right 45% of the time.

The walk-forward trains on every day before a test day (with a one-day
embargo) and scores the test day's headlines; the gate asks for a rank
correlation that holds across enough independent days before any live score
is published.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence

import numpy as np

NEWS_MODEL_KEY = "sentinel:news_model:report"      # the walk-forward report and, once validated, the weights
NEWS_SCORES_KEY = "sentinel:news_model:scores"      # hash: instrument -> today's live score
ALPHA = 50.0
MIN_TRAIN_DAYS = 20
EMBARGO_DAYS = 1
MIN_DAY_SAMPLES = 8
GATE_MIN_DAYS = 40
GATE_MIN_T = 2.0


def ridge(X: np.ndarray, y: np.ndarray, alpha: float = ALPHA) -> np.ndarray:
    """Weights with the intercept last, X centred and y centred inside."""
    mx, my = X.mean(axis=0), float(y.mean())
    Xc = X - mx
    w = np.linalg.solve(Xc.T @ Xc + alpha * np.eye(X.shape[1], dtype=X.dtype), Xc.T @ (y - my))
    return np.append(w, my - float(mx @ w))


def predict(weights: np.ndarray, X: np.ndarray) -> np.ndarray:
    return X @ weights[:-1] + weights[-1]


def _rank_corr(a: np.ndarray, b: np.ndarray) -> Optional[float]:
    if len(a) < 3:
        return None
    ra, rb = np.argsort(np.argsort(a)).astype(float), np.argsort(np.argsort(b)).astype(float)
    sa, sb = ra.std(), rb.std()
    return float(((ra - ra.mean()) * (rb - rb.mean())).mean() / (sa * sb)) if sa > 0 and sb > 0 else None


def walk_forward(day: Sequence[int], X: np.ndarray, y: np.ndarray, alpha: float = ALPHA) -> Dict[str, Any]:
    """Expanding-window walk-forward by day: every test day is predicted by a
    model trained only on days at least EMBARGO_DAYS before it."""
    d = np.asarray(day)
    days = sorted(set(d.tolist()))
    ics: List[float] = []
    signed: List[float] = []
    for k, td in enumerate(days):
        if k < MIN_TRAIN_DAYS:
            continue
        # The day before's labels run into the test day's session: left out.
        train = d < td - EMBARGO_DAYS
        test = d == td
        if train.sum() < 200 or test.sum() < MIN_DAY_SAMPLES:
            continue
        w = ridge(X[train], y[train], alpha)
        p = predict(w, X[test])
        ic = _rank_corr(p, y[test])
        if ic is not None:
            ics.append(ic)
            signed.append(float(np.mean(np.sign(p) * y[test])))
    n = len(ics)
    if n < 3:
        return {"days": n, "mean_ic": None, "t": None, "signed_bps": None, "validated": False}
    a = np.array(ics)
    sd = float(a.std(ddof=1))
    t = float(a.mean() / (sd / math.sqrt(n))) if sd > 0 else None
    return {"days": n, "mean_ic": round(float(a.mean()), 4), "t": round(t, 2) if t is not None else None,
            "share_positive": round(float((a > 0).mean()), 3), "signed_bps": round(float(np.mean(signed)) * 1e4, 1),
            "validated": bool(n >= GATE_MIN_DAYS and t is not None and t >= GATE_MIN_T)}


__all__ = ["ALPHA", "GATE_MIN_DAYS", "GATE_MIN_T", "NEWS_MODEL_KEY", "NEWS_SCORES_KEY", "predict", "ridge",
           "walk_forward"]

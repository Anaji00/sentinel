"""
shared/utils/alert_move_model.py

How big a move an alert precedes, learned rather than declared.

Alert tiers are set by hand from a rule's declared confidence, and graded on
prices (shared/utils/producer_grades.py) they say little: within the same
stock, five families precede bigger moves and the rest do not. This is the
meta-labelling idea (López de Prado): keep each rule as the primary signal and
learn, from what was true when it fired, how large the next session's move
was against the stock's usual one. Size only -- a risk input, not a direction.

  features  rule family, confidence, domain count, hour and weekday fired, the
            stock's usual move (log), its alerts in the prior 7 days
  target    log of the 1-session move ratio (producer_grades.move_ratio)
  test      walk-forward by calendar week, training only on alerts at least
            PURGE_DAYS older than the test week; per-week Spearman of
            prediction against outcome, against a baseline that predicts each
            family's training mean
  verdict   validated when the model's mean weekly IC beats the baseline's and
            its t is at least MIN_T over at least MIN_WEEKS weeks

Pure: rows in, matrix and report out. LightGBM is imported inside fit, so the
module loads where it is not installed.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

PURGE_DAYS = 7
MIN_TRAIN = 500
MIN_WEEKS = 6
MIN_T = 2.0
FEATURES = ["family", "confidence", "domains", "hour", "weekday", "log_usual", "recent_alerts"]


@dataclass(frozen=True)
class Alert:
    at: datetime
    ticker: str
    family: str
    confidence: float
    domains: int
    log_usual: float
    recent_alerts: int
    target: float            # log(move ratio)


def matrix(alerts: Sequence[Alert], families: Sequence[str]) -> np.ndarray:
    """Features in FEATURES order; family as its index in `families` (-1 unseen)."""
    idx = {f: k for k, f in enumerate(families)}
    return np.array([[idx.get(a.family, -1), a.confidence, a.domains, a.at.hour, a.at.weekday(),
                      a.log_usual, a.recent_alerts] for a in alerts], dtype=float)


def _spearman(x: np.ndarray, y: np.ndarray) -> Optional[float]:
    if len(x) < 10 or np.std(x) == 0 or np.std(y) == 0:
        return None
    rx, ry = np.argsort(np.argsort(x)), np.argsort(np.argsort(y))
    return float(np.corrcoef(rx, ry)[0, 1])


def fit(alerts: Sequence[Alert], families: Sequence[str]) -> Any:
    import lightgbm as lgb

    model = lgb.LGBMRegressor(n_estimators=200, learning_rate=0.05, num_leaves=15, min_child_samples=40,
                              subsample=0.8, subsample_freq=1, colsample_bytree=0.9, verbose=-1)
    model.fit(matrix(alerts, families), np.array([a.target for a in alerts]), categorical_feature=[0])
    return model


def walk_forward(alerts: Sequence[Alert]) -> Dict[str, Any]:
    """Weekly walk-forward of the model against the family-mean baseline."""
    alerts = sorted(alerts, key=lambda a: a.at)
    if len(alerts) < MIN_TRAIN:
        return {"alerts": len(alerts), "weeks": 0, "validated": False, "reason": "too few alerts"}
    start = alerts[0].at.date()
    weeks: Dict[int, List[Alert]] = {}
    for a in alerts:
        weeks.setdefault((a.at.date() - start).days // 7, []).append(a)
    model_ic: List[float] = []
    base_ic: List[float] = []
    for w in sorted(weeks):
        test = weeks[w]
        cut = start + timedelta(days=7 * w - PURGE_DAYS)
        train = [a for a in alerts if a.at.date() < cut]
        if len(train) < MIN_TRAIN or len(test) < 20:
            continue
        families = sorted({a.family for a in train})
        m = fit(train, families)
        pred = m.predict(matrix(test, families))
        means: Dict[str, List[float]] = {}
        for a in train:
            means.setdefault(a.family, []).append(a.target)
        overall = float(np.mean([a.target for a in train]))
        base = np.array([float(np.mean(means[a.family])) if a.family in means else overall for a in test])
        y = np.array([a.target for a in test])
        ic, bic = _spearman(pred, y), _spearman(base, y)
        if ic is not None:
            model_ic.append(ic)
            base_ic.append(bic if bic is not None else 0.0)
    return report(model_ic, base_ic, len(alerts))


def report(model_ic: Sequence[float], base_ic: Sequence[float], n_alerts: int) -> Dict[str, Any]:
    n = len(model_ic)
    if n < 2:
        return {"alerts": n_alerts, "weeks": n, "validated": False, "reason": "too few test weeks"}
    m, b = float(np.mean(model_ic)), float(np.mean(base_ic))
    sd = float(np.std(model_ic, ddof=1))
    t = m / (sd / math.sqrt(n)) if sd > 0 else 0.0
    ok = n >= MIN_WEEKS and m > b and t >= MIN_T
    return {"alerts": n_alerts, "weeks": n, "mean_ic": round(m, 4), "t": round(t, 2),
            "baseline_ic": round(b, 4), "share_positive": round(float(np.mean(np.array(model_ic) > 0)), 3),
            "validated": bool(ok)}


def usual_move(days: Sequence[date], opens: Sequence[float], closes: Sequence[float],
               spy: Dict[date, Tuple[float, float]], i: int, lookback: int = 60) -> Optional[float]:
    """The stock's mean |open-to-close move net of SPY| over the `lookback`
    sessions before index i, as producer_grades.move_ratio measures it."""
    xs = []
    for t in range(max(0, i - lookback), i):
        s = spy.get(days[t])
        if s and opens[t] and closes[t] and s[0] and s[1]:
            xs.append(abs(math.log(closes[t] / opens[t]) - math.log(s[1] / s[0])))
    return sum(xs) / len(xs) if len(xs) >= lookback // 2 else None


__all__ = ["Alert", "FEATURES", "MIN_T", "MIN_TRAIN", "MIN_WEEKS", "PURGE_DAYS", "fit", "matrix", "report",
           "usual_move", "walk_forward"]

"""Anomaly-score calibration (registered 21 Nov 2026, reports/Upgrade pre-registrations - 12 Nov.md, R1 to R4).

Nothing here is live unless SCORE_CALIBRATION_V2=1 (default 0). The functions are pure so the replay
(scripts/replay_score_rescale.py) and any producer use the same arithmetic.
"""
from __future__ import annotations

import bisect
import math
import os
from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple

FLAG = "SCORE_CALIBRATION_V2"
KALSHI_QUIET_CAP = 0.30
KALSHI_QUIET_NOTIONAL = 5_000.0


def v2_enabled() -> bool:
    return os.getenv(FLAG, "0").strip() == "1"


def _clip01(x: float) -> float:
    return min(1.0, max(0.0, x))


def _finite(x: Optional[float]) -> bool:
    return x is not None and math.isfinite(x)


# ---- R1: earnings_report ----------------------------------------------------------------------------------------------------------

def jumpiness(returns: Sequence[float], top: int = 4, min_obs: int = 120) -> Optional[float]:
    """Mean of the `top` largest absolute daily returns over the sd of all of them; None with fewer than `min_obs` returns or no spread."""
    if len(returns) < min_obs:
        return None
    mu = sum(returns) / len(returns)
    sd = math.sqrt(sum((r - mu) ** 2 for r in returns) / len(returns))
    if sd <= 0:
        return None
    big = sorted((abs(r) for r in returns), reverse=True)[:top]
    return (sum(big) / len(big)) / sd


def earnings_pre_score(old: float, jump: Optional[float]) -> float:
    """old - 0.20 x (1 - J), J = clip((r - 2.5) / 4, 0, 1). Never above old; the old score when r is unknown."""
    if not _finite(jump):
        return old
    j = _clip01((jump - 2.5) / 4.0)
    return round(old - 0.20 * (1.0 - j), 4)


# ---- R2: Kalshi quiet cap ----------------------------------------------------------------------------------------------------------

def kalshi_quiet_score(old: float, notional_usd: Optional[float], yes_prob: Optional[float]) -> float:
    """0.30 + 0.09 x s for an event sitting on the quiet cap with under $5,000 notional, s = size x 4p(1-p); other events keep their score."""
    if abs(old - KALSHI_QUIET_CAP) > 1e-9 or not _finite(notional_usd) or notional_usd >= KALSHI_QUIET_NOTIONAL:
        return old
    p = yes_prob if _finite(yes_prob) and 0.0 <= yes_prob <= 1.0 else 0.5
    size = _clip01(math.log10(max(notional_usd, 1.0)) / math.log10(KALSHI_QUIET_NOTIONAL))
    return round(KALSHI_QUIET_CAP + 0.09 * size * 4.0 * p * (1.0 - p), 3)


# ---- R3: filings and blocks ---------------------------------------------------------------------------------------------------------

def adv_usd(closes: Sequence[float], volumes: Sequence[float], sessions: int = 20) -> Optional[float]:
    """Mean of close x volume over the last `sessions` entries (the caller passes only sessions before the event day)."""
    pairs = [(c, v) for c, v in zip(closes, volumes) if c and v and c > 0 and v > 0][-sessions:]
    if len(pairs) < max(5, sessions // 2):
        return None
    return sum(c * v for c, v in pairs) / len(pairs)


def filing_score(old: float, adv: Optional[float]) -> float:
    """old x (1 - 0.25 s), s = clip(log10(ADV / 1e7) / 3, 0, 1); the old score when ADV is unknown."""
    if not _finite(adv) or adv <= 0:
        return old
    s = _clip01(math.log10(adv / 1e7) / 3.0)
    return round(old * (1.0 - 0.25 * s), 3)


def block_score(old: float, notional_usd: Optional[float], adv: Optional[float]) -> float:
    """min(old, 0.35 + 0.60 x clip(log10((notional / ADV) / 1e-4) / 2, 0, 1)); the old score when either is unknown."""
    if not _finite(notional_usd) or not _finite(adv) or notional_usd <= 0 or adv <= 0:
        return old
    part = _clip01(math.log10((notional_usd / adv) / 1e-4) / 2.0) if notional_usd / adv > 0 else 0.0
    return min(old, round(0.35 + 0.60 * part, 3))


# ---- R4: slot-quantile mapping ------------------------------------------------------------------------------------------------------

def slot_quantile_score(old: float, slot_history: Sequence[float], pooled_history: Sequence[float], min_slot: int = 50) -> float:
    """Q_pooled(F_slot(old)): both histories are ascending-sorted scores from before the event. Mid-rank for ties. The old score when
    either history is too short."""
    if len(slot_history) < min_slot or len(pooled_history) < min_slot:
        return old
    lo, hi = bisect.bisect_left(slot_history, old), bisect.bisect_right(slot_history, old)
    q = ((lo + hi) / 2.0) / len(slot_history)
    idx = min(len(pooled_history) - 1, max(0, int(q * len(pooled_history))))
    return pooled_history[idx]


def slot_key(trade_type: str, hour_utc: int, weekend: bool) -> Tuple[str, int, bool]:
    return (trade_type, hour_utc, weekend)


# ---- outcomes and verdicts ----------------------------------------------------------------------------------------------------------

def normalized_abs_move(days: Sequence[date], closes: Sequence[float], d: date, sessions: int = 5, lookback: int = 60,
                        min_returns: int = 30) -> Optional[float]:
    """|close `sessions` after the last close on or before d / that close - 1| over (sd of the `lookback` daily log returns before it x
    sqrt(sessions)). None for a missing bar, a stale start (over 5 days), or a flat history. `days` is ascending."""
    i = bisect.bisect_right(days, d) - 1
    if i < 0 or i + sessions >= len(days) or (d - days[i]).days > 5 or closes[i] <= 0:
        return None
    window = closes[max(0, i - lookback):i + 1]
    rets = [math.log(b / a) for a, b in zip(window, window[1:]) if a > 0 and b > 0]
    if len(rets) < min_returns:
        return None
    mu = sum(rets) / len(rets)
    sd = math.sqrt(sum((r - mu) ** 2 for r in rets) / len(rets))
    if sd <= 0:
        return None
    return abs(closes[i + sessions] / closes[i] - 1.0) / (sd * math.sqrt(sessions))


def date_cluster_diff(rows: Sequence[Tuple[object, float, float, float]], draws: int = 2000, seed: int = 20261121
                      ) -> Optional[Dict[str, float]]:
    """rows = (date, old_score, new_score, outcome). Spearman(new, outcome) - Spearman(old, outcome), with a 95% interval from `draws`
    resamples of whole dates. A constant old score counts as correlation 0. None when fewer than 3 rows."""
    if len(rows) < 3:
        return None
    import numpy as np
    from scipy.stats import rankdata

    old = np.array([r[1] for r in rows], dtype=float)
    new = np.array([r[2] for r in rows], dtype=float)
    out = np.array([r[3] for r in rows], dtype=float)

    def rho(a: "np.ndarray", b: "np.ndarray") -> float:
        ra, rb = rankdata(a), rankdata(b)
        if ra.std() == 0 or rb.std() == 0:
            return 0.0
        return float(np.corrcoef(ra, rb)[0, 1])

    def diff(idx: "np.ndarray") -> float:
        return rho(new[idx], out[idx]) - rho(old[idx], out[idx])

    groups: Dict[object, List[int]] = {}
    for i, r in enumerate(rows):
        groups.setdefault(r[0], []).append(i)
    members = [np.array(v) for v in groups.values()]
    rng = np.random.default_rng(seed)
    stats = []
    for _ in range(draws):
        pick = rng.integers(0, len(members), len(members))
        stats.append(diff(np.concatenate([members[k] for k in pick])))
    stats.sort()
    everything = np.arange(len(rows))
    return {"n": float(len(rows)), "dates": float(len(members)),
            "rho_old": rho(old, out), "rho_new": rho(new, out), "diff": diff(everything),
            "lo": stats[int(0.025 * draws)], "hi": stats[min(draws - 1, int(0.975 * draws))]}


def not_worse(res: Optional[Dict[str, float]], min_n: int, tol: float = 0.02) -> bool:
    """The registered outcome bar: at least `min_n` rows, point difference >= 0 and interval lower end >= -tol."""
    return res is not None and res["n"] >= min_n and res["diff"] >= 0.0 and res["lo"] >= -tol


def mode_share(scores: Sequence[float]) -> Tuple[float, float]:
    """(most common value rounded to 3 places, its share)."""
    if not scores:
        return 0.0, 0.0
    counts: Dict[float, int] = {}
    for s in scores:
        k = round(s, 3)
        counts[k] = counts.get(k, 0) + 1
    v, n = max(counts.items(), key=lambda kv: kv[1])
    return v, n / len(scores)

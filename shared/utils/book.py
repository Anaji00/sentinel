"""
shared/utils/book.py

What a set of ideas adds up to as one book: the risk a desk sees before it
trades any of them.

Inverse-volatility weights, each side scaled to half the gross; net beta and
the SPY notional that hedges it; ex-ante volatility from the names' own daily
returns, covariance shrunk toward constant correlation (Ledoit & Wolf 2004);
concentration by industry; and for a book of a given size, each position as a
share of its daily volume, the days it takes at a participation cap, and the
square-root cost of putting it on.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence

import numpy as np

PARTICIPATION_CAP = 0.10        # at most 10% of a day's volume
SHRINK = 0.5                    # weight on the constant-correlation target


def _shrunk_cov(rets: np.ndarray) -> np.ndarray:
    """Sample covariance pulled halfway to constant correlation."""
    s = np.cov(rets, rowvar=False)
    sd = np.sqrt(np.clip(np.diag(s), 1e-12, None))
    corr = s / np.outer(sd, sd)
    n = corr.shape[0]
    rbar = (corr.sum() - n) / (n * (n - 1)) if n > 1 else 0.0
    target = rbar * np.outer(sd, sd)
    np.fill_diagonal(target, sd ** 2)
    return SHRINK * target + (1 - SHRINK) * s


def build_book(ideas: Sequence[Dict[str, Any]], returns: Dict[str, Sequence[float]],
               book_usd: float = 10_000_000.0) -> Optional[Dict[str, Any]]:
    """ideas: dicts with ticker, direction, rv (annual), beta, adv_usd, industry.
    returns: ticker -> aligned daily returns (same length for all)."""
    rows = [i for i in ideas if i.get("rv") and i["rv"] > 0 and i.get("direction")]
    if len(rows) < 2:
        return None
    raw_w = np.array([i["direction"] / i["rv"] for i in rows], dtype=float)
    longs, shorts = raw_w > 0, raw_w < 0
    w = raw_w.copy()
    if longs.any():
        w[longs] = 0.5 * raw_w[longs] / raw_w[longs].sum()
    if shorts.any():
        w[shorts] = -0.5 * raw_w[shorts] / raw_w[shorts].sum()
    betas = np.array([float(i.get("beta") or 1.0) for i in rows])
    net_beta = float((w * betas).sum())

    vol = None
    tick = [i["ticker"] for i in rows]
    if all(t in returns for t in tick):
        lengths = {len(returns[t]) for t in tick}
        if len(lengths) == 1 and min(lengths) >= 20:
            r = np.column_stack([np.asarray(returns[t], dtype=float) for t in tick])
            cov = _shrunk_cov(r) * 252
            vol = float(math.sqrt(max(w @ cov @ w, 0.0)))

    industries: Dict[str, float] = {}
    for wi, i in zip(w, rows):
        k = str(i.get("industry") or "unknown")
        industries[k] = industries.get(k, 0.0) + abs(wi)
    top_industry = max(industries.items(), key=lambda kv: kv[1]) if industries else ("", 0.0)

    positions: List[Dict[str, Any]] = []
    total_cost = 0.0
    worst_days = 0.0
    for wi, i in zip(w, rows):
        notional = abs(wi) * book_usd
        adv = float(i.get("adv_usd") or 0.0)
        share = notional / adv if adv else None
        days = share / PARTICIPATION_CAP if share is not None else None
        sigma = i["rv"] / math.sqrt(252)
        half_spread = 1.0 if adv > 5e8 else 2.5 if adv > 5e7 else 5.0 if adv > 1e7 else 12.5
        cost_bps = half_spread + (1e4 * sigma * math.sqrt(notional / adv) if adv else 0.0)
        total_cost += cost_bps * notional / 1e4
        worst_days = max(worst_days, days or 0.0)
        positions.append({"ticker": i["ticker"], "weight": round(float(wi), 4), "notional": round(notional),
                          "share_of_volume": round(share, 4) if share is not None else None,
                          "days_at_cap": round(days, 2) if days is not None else None,
                          "cost_bps": round(cost_bps, 1)})
    return {
        "book_usd": book_usd,
        "gross": round(float(np.abs(w).sum()), 3),
        "net": round(float(w.sum()), 3),
        "net_beta": round(net_beta, 3),
        "spy_hedge_usd": round(-net_beta * book_usd),
        "ex_ante_vol": round(vol, 4) if vol is not None else None,
        "top_industry": top_industry[0],
        "top_industry_share": round(top_industry[1] / float(np.abs(w).sum()), 3),
        "cost_to_enter_usd": round(total_cost),
        "cost_to_enter_bps": round(1e4 * total_cost / book_usd, 1),
        "max_days_to_trade": round(worst_days, 2),
        "participation_cap": PARTICIPATION_CAP,
        "positions": positions,
    }


__all__ = ["PARTICIPATION_CAP", "build_book"]

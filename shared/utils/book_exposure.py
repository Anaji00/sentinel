"""Read-only exposure of the stored B1 book (quant report T7, "do now" part): ex-ante beta by leg and net, leg volatilities and the heaviest sectors. No order, size or
limit reads this; it is a measurement for the dashboard and the daily review. Equal weight per leg, as the book is built. Pure: bars and the book in, a dict out."""
from __future__ import annotations

import math
import statistics
from collections import Counter
from datetime import date
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

MIN_RETURNS = 60            # common daily returns with SPY needed for a beta
VOL_WINDOW = 63             # sessions in the volatility window
Closes = Mapping[date, float]


def daily_returns(closes: Closes) -> Dict[date, float]:
    ds = sorted(closes)
    return {d: closes[d] / closes[p] - 1.0 for p, d in zip(ds, ds[1:]) if closes[p] > 0 and closes[d] > 0}


def beta_and_vol(rets: Mapping[date, float], spy: Mapping[date, float]) -> Tuple[Optional[float], Optional[float]]:
    """(beta to SPY, annualised volatility over the last 63 sessions); None when under MIN_RETURNS common days or SPY did not vary."""
    common = sorted(d for d in rets if d in spy)
    if len(common) < MIN_RETURNS:
        return None, None
    a, m = [rets[d] for d in common], [spy[d] for d in common]
    mm = sum(m) / len(m)
    var = sum((x - mm) ** 2 for x in m)
    if var <= 0:
        return None, None
    ma = sum(a) / len(a)
    beta = sum((x - ma) * (y - mm) for x, y in zip(a, m)) / var
    return beta, statistics.pstdev(a[-VOL_WINDOW:]) * math.sqrt(252)


def _leg(names: Sequence[str], bars: Mapping[str, Closes], spy: Mapping[date, float]) -> Dict[str, Any]:
    betas: List[float] = []
    vols: List[float] = []
    for n in names:
        b, v = beta_and_vol(daily_returns(bars.get(n, {})), spy)
        if b is not None and v is not None:
            betas.append(b)
            vols.append(v)
    return {"names": len(names), "measured": len(betas), "mean_beta": round(sum(betas) / len(betas), 3) if betas else None,
            "median_vol": round(statistics.median(vols), 3) if vols else None}


def exposure(book: Mapping[str, Any], bars: Mapping[str, Closes], sectors: Mapping[str, str], top: int = 5) -> Dict[str, Any]:
    """{long, short, net_beta, top_sectors, rebalanced_on}. net_beta = half of (long mean beta minus short mean beta): each leg is half of gross exposure, so this is beta per dollar of gross (None if either leg is
    unmeasured). top_sectors counts names per sector across both legs, by leg."""
    longs, shorts = list(book.get("long") or []), list(book.get("short") or [])
    spy = daily_returns(bars.get("SPY", {}))
    lg, sh = _leg(longs, bars, spy), _leg(shorts, bars, spy)
    net = round(0.5 * (lg["mean_beta"] - sh["mean_beta"]), 3) if lg["mean_beta"] is not None and sh["mean_beta"] is not None else None
    def count(names: Sequence[str]) -> List[Tuple[str, int]]:
        return Counter(sectors.get(n, "Other") for n in names).most_common(top)
    return {"rebalanced_on": book.get("rebalanced_on"), "scores_as_of": book.get("scores_as_of"), "long": lg, "short": sh, "net_beta": net,
            "top_sectors": {"long": count(longs), "short": count(shorts)}}


__all__ = ["beta_and_vol", "daily_returns", "exposure"]
